#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Linked data prep. Get URIs and (optionally) insert $0 into MARCXML records.
Works with lcnaf and lcsaf in 4store.

sudo ./sh/start-4store.sh

python uris.py -vsnrk

python uris.py -h 

from 20151023
pmg
"""
import argparse
import ConfigParser
import csv
import httplib
import logging
import libxml2
import os
import pymarc
import re
import requests
import shutil
import sqlite3 as lite
import string
import subprocess
import sys
import time
import unicodedata
import urllib
from datetime import date, datetime, timedelta
from lxml import etree

# TODO:
# Add option for checking Voyager auth tables

# config
config = ConfigParser.RawConfigParser()
config.read('./config/uris.cfg')
OUTDIR = config.get('env', 'outdir')
INDIR = config.get('env', 'indir')
TMPDIR = config.get('env', 'tmpdir')
REPORTDIR = config.get('env', 'reports')
LOG = config.get('env', 'logdir')
TOLOAD = config.get('env', 'load')
CMARCEDIT = config.get('env', 'cmarcedit')
DB = config.get('db','heading_cache')
VOYAGER_HELPER = config.get('env','voyager_helper')
ID_HEADING_RESOLVER = "http://id.loc.gov/authorities/label/"

today = time.strftime('%Y%m%d') # name log files
todaydb = time.strftime('%Y-%m-%d') # date to check against db

REPORTS = REPORTDIR + today # reports go into subdirectories in w3cdtf

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=LOG+today+'.log',level=logging.INFO)
# the following two lines disable the default logging of requests.get()
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
	'''
	Main
	'''
	try:
		s = requests.get('http://localhost:8000/status')
		n = requests.get('http://localhost:8001/status')
		i = requests.head('http://id.loc.gov')
		if s.status_code == 200:
			msg = 'lcsaf connection ok\n'
		if n.status_code == 200:
			msg += 'lcnaf connection ok\n'
		if i.status_code == 200:
			msg += 'internet connection ok'
		if verbose:
			print(msg + '\nHere we go...\n')
			print('.' * 50)
	except:
		sys.exit('Run `sudo ./sh/start_4s.sh` and check internet connection. Thank you, and have a nice day.')

	logging.info('main')
	
	mrcrecs = os.walk(INDIR).next()[2]
	mrcrecs.sort(key=alphanum_key)
	if not mrcrecs:
		sys.exit('-'*75+'\nThere are no MARC records in the IN directory (or IN doens\'t exist). Add some and try again.\n'+'-'*75)
	else:
		for mrcrec in mrcrecs:
			if verbose:
				print('current file is %s' % mrcrec)
			read_mrx(mrcrec,names,subjects)


def try_int(s):
	'''
	Helper toward sorting filenames in main(). Used in alphanum_key. Nicked from GH.
	'''
	try:
		return int(s)
	except ValueError:
		return s


def alphanum_key(s):
	'''
	For sorting filenames in main()
	'''
	return [try_int(c) for c in re.split('([0-9]+)', s)]


def setup():
	'''
	Create tmp, in, out, reports, load, log dirs, and csv files
	'''
	logging.info('seting up')

	if (not os.path.exists(TMPDIR)): #and (justfetch is None):
		os.makedirs(TMPDIR)
		
	if not os.path.exists(INDIR):
		os.makedirs(INDIR)
	
	if not os.path.exists(OUTDIR):
		os.makedirs(OUTDIR)
		
	if not os.path.exists(TMPDIR):
		os.makedirs(TMPDIR)
	
	if not os.path.exists(LOG):
		os.makedirs(LOG)

	if not os.path.exists(REPORTS):
		os.makedirs(REPORTS)

	if not os.path.exists(TOLOAD):
		os.makedirs(TOLOAD)
	
	schemelist = []
	if (csvout or nomarc) and (subjects or names):
		if subjects:
			schemelist.append('sub')
		if names:
			schemelist.append('nam')
		for scheme in schemelist:
			rpt = REPORTS+'/_'+scheme+'_'+today+'.csv'
			try:
				os.rename(rpt, rpt + '.bak') # back up output from previous runs on same day
			except OSError:
				pass
				
			with open(rpt,'wb+') as outfile:
				heading = ['bib','heading','lc_uri','lc_tag','lc_source']
				writer = csv.writer(outfile)
				writer.writerow(heading)


def cleanup():
	'''
	Clean up IN and TMP dirs
	'''
	msg = ''
	indir_count = str(len([name for name in os.listdir(INDIR) if os.path.isfile(os.path.join(INDIR, name))]))
	outdir_count = str(len([name for name in os.listdir(OUTDIR) if os.path.isfile(os.path.join(OUTDIR, name))]))
	
	if keep == False: #justfetch is None:
		tempdirs = [TMPDIR, INDIR]
		for d in tempdirs:
			if os.path.isdir(d):
				shutil.rmtree(d)
			else:
				print(d + ' didn\'t exist.')
	
	if csvout:
		msg = 'reports are in reports dir.'
		logging.info(msg)
	print('See ya.' + '\n' + '.'*50)
	
	logging.info('cleaned up')


def query_4s(label, scheme, thesaurus):
	'''
	SPARQL query
	'''
	# SPARQL endpoint(s), one for each scheme (names, subjects)
	if scheme == 'nam':
		host = "http://localhost:8001/"
	elif scheme == 'sub':
		host = "http://localhost:8000/"

	label = label.replace('"',"%22") # e.g. bib 568 "Problemna..." (heading with double quotes).
	# replace combined characters (id does this automatically)
	label = unicodedata.normalize('NFC', label.decode('utf8'))
	label = re.sub('\s+',' ',label)
	# query for notes as well, to eliminate headings that are to be used as subdivisions (see e.g. 'Marriage')
	query = 'SELECT ?s ?note WHERE { ?s ?p "%s"@en . OPTIONAL {?s <http://www.w3.org/2004/02/skos/core#note> ?note .FILTER(CONTAINS(?note,"subdivision")) .}}' % label

	# query for variants
	variant_query = 'SELECT distinct ?s ?o WHERE { {?s ?p ?bn  . ?bn <http://www.loc.gov/mads/rdf/v1#variantLabel> "%s"@en . }}' % label
	
	data = { 'query': query}
	headers={ 'content-type':'application/x-www-form-urlencoded'}
	
	r = requests.post(host + "sparql/", data=data, headers=headers )
	if r.status_code != requests.codes.ok:   # <= would mean something went wrong with 4store
		msg = '%s, %s' % (label, r.text)
		sys.exit(msg)
	try:
		doc = etree.fromstring(r.text)
	except:
		return None

	xpth = "//sparql:binding[@name='s'][not(following-sibling::sparql:binding[@name='note'])]/sparql:uri"
	
	if thesaurus == 1:
		xpth += "[contains(.,'childrensSubjects')]"
	else: 
		xpth += "[not(contains(.,'childrensSubjects'))]"
	
	if len(doc.xpath(xpth,namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'})) > 0:
		for triple in doc.xpath(xpth,namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'}):
			return triple.text
	else:
		data = { 'query': variant_query}
	
		r = requests.post(host + "sparql/", data=data, headers=headers )
		if r.status_code != requests.codes.ok:   # <= would mean something went wrong with 4store
			msg = '%s, %s' % (label, r.text)
			sys.exit(msg)
		try:
			doc = etree.fromstring(r.text)
			for triple in doc.xpath(xpth,namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'}):
				return triple.text
		except:
			return None


def read_mrx(mrcrec,names,subjects):
	'''
	Read through a given MARCXML file and optionally copy it, inserting $0 as appropriate
	'''
	enhanced = [] # will be True or False, from check_heading()
	recs = [] # will be a pymarc object or None, from check_heading()
	mrxheader = """<?xml version="1.0" encoding="UTF-8" ?>
	<collection xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">"""
	try:
		reader = pymarc.marcxml.parse_xml_to_array(INDIR+mrcrec)
		
		for rec in reader:
			f001 = rec.get_fields('001')
			f035 = rec.get_fields('035')
			try:
				ocn = rec['035']['a']
			except:
				ocn = None
			for b in f001:
				bbid = b.value()
			if names:
				en,r = check_heading(bbid,rec,'nam')
				enhanced.append(en)
				if en == True:
					recs.append(r)
			if subjects: # if just searching subjects, or if a rec only has subjects, no names
				en,r = check_heading(bbid,rec,'sub')
				enhanced.append(en) 	
				if en == True and r not in recs:
					recs.append(r)
		if nomarc == False:
			outfile = str.replace(mrcrec,'.xml','') 
			fh = open(TMPDIR+outfile+'_tmp.xml', 'wb+') 
			fh.write(mrxheader)
			for record in recs:
				if record is not None:
					try:
						out = "%s" % (pymarc.record_to_xml(record))
						fh.write(out)
					except Exception as e:
						raise						
		if nomarc == False and ((enhanced_only == True and (True in enhanced)) or (enhanced_only == False)):
			fh.write("</collection>")
			fh.close()
			
	except AttributeError as e:
		return
	except:	
		if names:
			scheme = 'nam'
		elif subjects:
			scheme = 'sub'
		etype,evalue,etraceback = sys.exc_info()
		flag = "read_mrx problem: %s %s %s line %s" % (etype,evalue,etraceback,etraceback.tb_lineno)
		if csvout or nomarc: # idea here is to report something out even when mrx has issues
			write_csv(bbid,flag,'',scheme,'','')

	if not nomarc and ((enhanced_only == True and (True in enhanced)) or (enhanced_only == False)):
		try:
			subprocess.Popen(['xmllint','--format','-o', OUTDIR+outfile, TMPDIR+outfile+'_tmp.xml']).wait()
			mrx2mrc(OUTDIR+outfile)
		except:
			etype,evalue,etraceback = sys.exc_info()
			print("xmllint problem: %s" % evalue)

	if (keep == False):
		os.remove(INDIR+mrcrec)
		

def query_lc(heading, scheme):
	'''
	Query id.loc.gov (but only after checking the local file)
	'''
	src = 'id.loc.gov'
	if ignore_cache == False: # First, check the cache
		
		cached,datediff,uri = check_cache(heading,scheme)
	
		if (cached == True and datediff <= maxage):
			src += ' (cache)'
			return uri,src
			
	if (((cached == True and datediff > maxage) or cached == False) or (ignore_cache == True and uri == 'None (404)')):
		# ping id.loc only if not found in cache, or if checked long, long ago
		heading = heading.replace('&','%26')
		heading = heading.decode('utf8')
		to_get = ID_HEADING_RESOLVER + heading
		headers = {"Accept":"application/xml"}
		time.sleep(1) # http://id.loc.gov/robots.txt has 3 secs but kh says not necessary w head requests
		resp = requests.head(to_get, headers=headers, allow_redirects=True)
		if resp.status_code == 200:
			uri = resp.headers["x-uri"]
			if (scheme == 'nam' and 'authorities/names' in uri) or (scheme == 'sub' and 'authorities/subjects' in uri):
				try:
					resp.headers["x-preflabel"]
				except KeyError as e: # x-preflabel is not returned for deprecated headings, so just check for it as a test
					uri = "None (deprecated)"
					try:
						tree = etree.html.fromstring(resp.text)
						see = tree.xpath("//h3/text()= 'Use Instead'") # this info isn't in the header, so grabbing from html
						seeother = ''
						if not see:
							other = tree.xpath("//h3[text() = 'Use Instead']/following-sibling::ul//div/a")[0]
							uri = (other.attrib['href'], other.text)
					except:
						pass

				cache_it(uri,cached,heading,scheme)

				return uri,src # ==>
				
			else:
				if (scheme == 'nam' and 'authorities/subjects' in uri):
					msg = 'None (wrong schema: %s)' % uri[18:]
				elif (scheme == 'sub' and 'authorities/names' in uri):
					msg = 'None (wrong schema: %s)' % uri[18:]
				return msg,src # ==>
				
		elif resp.status_code == 404:
			msg = "None (404)"
			cache_it(msg,cached,heading,scheme)
			return msg,src # ==>
			
		else: # if resp.status_code != 404 and status != 200:
			msg = "None (" + resp.status_code + ")"
			cache_it(msg,cached,heading,scheme)
			return msg,src # ==>

	
def record_bib(bib):
	'''
	Add bib of retrieved record into bibs.db
	'''
	con = lite.connect(DB)
	with con:
		con.row_factory = lite.Row
		cur = con.cursor()
		cur.execute("SELECT * FROM bibs WHERE bbid=?",(bib,))
		rows = cur.fetchall()
		if len(rows) == 0:
			cached = False
		else:
			cached = True
				
		todaydb = time.strftime('%Y-%m-%d %H:%M:%S')
		cur = con.cursor() 
		if cached == False:
			newbib = (bib,todaydb)
			cur.executemany("INSERT INTO bibs VALUES(?, ?)", (newbib,))
		else:
			updatedbib = (todaydb,bib)
			cur.executemany("UPDATE bibs SET date=? WHERE bbid=?", (updatedbib,))
	if con:
		print('closing sqlite3 connection')
		con.close()


def cache_it(uri,cached,heading, scheme):
	'''
	Put uris from id.loc.gov into cache (or update the date checked)
	'''
	con = lite.connect(DB)
	heading = heading.replace('%26','&')
	if verbose:
		print('==> caching %s | already cached: %s | %s | %s' % (uri,cached,heading, scheme))
	try:
		with con:
			cur = con.cursor() 
			if cached == True:
				updateurl = (today, heading, uri)
				cur.executemany("UPDATE headings SET date=? WHERE heading=? and uri=?", (updateurl,))
			else:
				newuri = (heading, scheme, uri, today)
				cur.executemany("INSERT INTO headings VALUES(?, ?, ?, ?)", (newuri,))
	except Exception as e:
		print('cache_it error %s' % e.value)
		pass
		
	if con:
		con.close()


def check_heading(bbid,rec,scheme):
	'''
	Check a given heading against 4store and, if that fails, id.loc.gov
	'''
	enhanced = False
	heading = ''
	if scheme == 'sub':
		# get subjects data from these subfields (all but 0,2,3,6,8)
		fields = ['600','610','611','630','650','651']
		subfields = ['a', 'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 
	'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'x', 'y', 'z', '4']
	elif scheme == 'nam':
		# get names data from these subfields
		fields = ['100','110','130','700','710','730']
		subfields = ['a','b','c','d','q']
	if not rec.get_fields(*fields):
		write_csv(bbid,'','',scheme,'','') # report out records with no headings
	try:
		for f in rec.get_fields(*fields):
			mrx_subs = []
			h1 = ''
			h2 = ''
			src = ''
			thesaurus = 0
			if scheme == 'sub':
				if f.indicators[1] != ' ':
					thesaurus = int(f.indicators[1]) # 6xx ind2: '1' is LC subject headings for children's literature
			for s in f.get_subfields(*subfields):
				if scheme == 'nam' and re.match('\d00',f.tag) and not f.get_subfields('c','q','d'):
					continue # continue with next iteration of loop (skip plain names)
				s = s.encode('utf8').strip()
				# TODO: a fuller list could be put into a sqlite db to be checked here
				# Account for abbreviations (using negative lookbehind)
				re.sub('(?<!(\sco))\.','',s,flags=re.IGNORECASE) # preserve '.' at end of Co. (add other 2-letter abbrev.)
				re.sub('(?<!(\sinc))\.','',s,flags=re.IGNORECASE) # preserve '.' at end of Inc. (add other 3-letter abbrev.)
				re.sub('(?<!(\sdept))\.','',s,flags=re.IGNORECASE) # preserve '.' at end of Inc. (add other 4-letter abbrev.)
				mrx_subs.append(s)
			if not mrx_subs:
				continue # continue with next iteration of loop (e.g. prevent empty heading when 730 only has $t)
			h = "--".join(mrx_subs)
			h = h.replace(',--',', ')
			h = h.replace('.--','. ')
			h = h.replace('--(',' (') # $q
			
			uri = query_4s(h, scheme, thesaurus)
			src = '4store'
			if uri is None: # if nothing found, modify it and try again
				h1 = h.rstrip('.').rstrip(',')
				h1 = re.sub('(^\[|\]$)','',h1) # remove surrounding brackets
				uri = query_4s(h1, scheme, thesaurus)
				src = '4store'
				if (noidloc == False and uri == None):
					try:
						uri,src = query_lc(h,scheme) # <= if still not found in 4store, check cache and ping id.loc.gov 
					except:
						pass # as when uri has 'classification'
					if uri is None or not uri.startswith('http'): # <= if still not found, try without trailing punct.
						h2 = h.rstrip('.').rstrip(',')
						h2 = re.sub('(^\[|\]$)','',h2)
						try:
							uri,src = query_lc(h2,scheme)
						except:
							pass # as when uri has 'classification'
			if nomarc == False and ((uri is not None and uri.startswith('http'))):
				# check for existing id.loc $0 and compare if present
				existing_sub0s = f.get_subfields('0')
	
				if existing_sub0s:
					for existing_sub0 in existing_sub0s:
						existing_sub0 = existing_sub0.encode('utf8').strip().replace('(uri)','')
						if existing_sub0 == uri:
							src = 'already has %s' % existing_sub0 # printing out full uri for double-checking
							enhanced = True
						elif 'id.loc.gov/authorities/' in existing_sub0: # <= if the url id.loc.gov but is different...
							pymarc.Field.delete_subfield(f,"0") # <=  ...assume it was wrong and delete it...
							prefixuri = '(uri)' + uri
							pymarc.Field.add_subfield(f,"0",uri) # <= ...then insert the new one.
							src = 'REPLACED %s with %s' % (existing_sub0,prefixuri)
							enhanced = True
						else:
							prefixuri = '(uri)' + uri
							pymarc.Field.add_subfield(f,"0",prefixuri)
							enhanced = True
				else:
					prefixuri = '(uri)' + uri
					pymarc.Field.add_subfield(f,"0",prefixuri)
					enhanced = True

			if uri is None:
				if h2 is not None:
					heading = h2
				elif h2 is not None:
					heading = h1
				else:
					heading = h
			elif uri is not None:
				if (h2 != '' and uri.startswith('http') == True):
						heading = h2
				elif (h1 != '' and uri.startswith('http') == True):
						heading = h1
				elif (uri.startswith('http') == False and re.search('[\.,]+$',h)):
					punct = len(re.search('[\.,]+$',h).group(0))
					heading = h[:-punct] + '['+h[-punct:]+']'
				else:
					heading = h
				
			if verbose:
				print('%s | %s | %s | %s | %s' % (bbid, heading.decode('utf8'), uri, f.tag, src))
			if csvout or nomarc:
				write_csv(bbid, heading, uri, scheme,f.tag,src)
	
		if enhanced == False and enhanced_only == True:
			return enhanced, None # ==> add bib to report, but no marcxml record in the out dir (see read_mrx)
		else:
			return enhanced, rec # ==>
	except:
		etype,evalue,etraceback = sys.exc_info()
		print("check_heading problem %s %s %s line: %s" % (etype,evalue,etraceback,etraceback.tb_lineno))
		

def write_csv(bbid, heading, uri, scheme, tag, src):
	'''
	Write out csv reports, one each for names ('_nam_') and subjects ('_sub_')
	'''
	with open(REPORTS+'/_'+scheme+'_'+today+'.csv','ab+') as outfile:
		writer = csv.writer(outfile)
		row = (bbid, heading, uri, tag, src)
		writer.writerow(row)


def check_cache(heading,scheme):
	'''
	Check the cache
	'''
	cached = False
	datediff = 0
	con = lite.connect(DB) # sqlite3 table with fields heading, scheme, uri, date
	uri = ''
	try:
		with con:
			con.row_factory = lite.Row
			cur = con.cursor()
			cur.execute("SELECT * FROM headings WHERE heading=? and scheme=?",(heading.decode('utf8'),scheme,))
			rows = cur.fetchall()
			if len(rows) != 0:
				cached = True
				for row in rows:
					uri = row['uri']
					dbscheme = row['scheme']
					date = row['date']
			if cached == True and date is not None:
				date2 = datetime.strptime(todaydb,'%Y-%m-%d')
				date1 = datetime.strptime(str(date),'%Y%m%d')
				datediff = abs((date2 - date1).days)		
			return cached,datediff,uri
	except:
		etype,evalue,etraceback = sys.exc_info()
		print("check_cache problem %s %s %s line: %s" % (etype,evalue,etraceback,etraceback.tb_lineno))


def mrx2mrc(mrx):
	'''
	Convert marcxml file to MARC21 for loading using Strawn RecordReloader
	'''
	basename = os.path.basename(mrx)
	mrc = str.replace(basename,'.mrx','.mrc')
	try:
		conv = subprocess.Popen(['mono',CMARCEDIT,'-s',mrx,'-d',TOLOAD+mrc,'-xmlmarc'])
		conv.communicate()
		msg = 'converted mrx to mrc'
	except:
		etype,evalue,etraceback = sys.exc_info()
		msg = "problem converting mrx to mrc. %s" % evalue
	logging.info(msg)

			
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Generate hold reports.')
	parser.add_argument("-v", "--verbose", required=False, default=False, dest="verbose", action="store_true", help="Runtime feedback.")
	parser.add_argument("-n", "--names", required=False, default=False, dest="names", action="store_true", help="Get URIs for names.")
	parser.add_argument("-s", "--subjects", required=False, default=False, dest="subjects", action="store_true", help="Get URIs for subjects.")
	parser.add_argument("-r", "--report", required=False, default=True, dest="csvout", action="store_true", help="Output csv reports as well as MARCXML records.")
	parser.add_argument("-R", "--Report", required=False, default=False, dest="nomarc", action="store_true", help="Output csv reports but do NOT output MARCXML records. Overrides -F.")
	parser.add_argument("-k", "--keep", required=False, default=False, dest="keep", action="store_true", help="Keep IN and TMP dirs.")
	parser.add_argument("-a",'--age', required=False, dest="maxage",help="Max days after which to re-check id.loc.gov", default=7)
	parser.add_argument("-i",'--ignore', required=False, dest="noidloc", default=False, action="store_true", help="Ignore id.loc.gov")
	parser.add_argument("-C", "--ignore-cache",required=False, default=False, dest="ignore_cache", action="store_true", help="Optionally ignore 404 errors in cache (so check these headings against id.loc.gov again).")
	parser.add_argument("-e","--enhanced",required=False, default=False, dest="enhanced", action="store_true", help="Just output enhanced records for loading.")

	args = vars(parser.parse_args())
	csvout = args['csvout']
	enhanced_only = args['enhanced']
	ignore_cache = args['ignore_cache']
	keep = args['keep']
	maxage = int(args['maxage'])
	names = args['names']
	noidloc = args['noidloc']
	nomarc = args['nomarc']
	subjects = args['subjects']
	verbose = args['verbose']
	
	logging.info('='*50)
	setup()
	main()
	cleanup()
	logging.info('='*50)
