#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Linked data prep. Get URIs and (optionally) insert $0 into MARCXML records.
Works with lcnaf and lcsaf in 4store.

sudo ./start-4store.sh
python uris.py -vnRk

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
import urllib
from datetime import date, datetime, timedelta
from lxml import etree

# config
config = ConfigParser.RawConfigParser()
config.read('./config/uris.cfg')
OUTDIR = config.get('env', 'outdir')
INDIR = config.get('env', 'indir')
TMPDIR = config.get('env', 'tmpdir')
REPORTDIR = config.get('env', 'reports')
LOG = config.get('env', 'logdir')
DB = config.get('db','sqlite')
ID_SUBJECT_RESOLVER = "http://id.loc.gov/authorities/label/"

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
			msg = 'lcsaf connection ok'
		if n.status_code == 200:
			msg += '\nlcnaf connection ok'
		if i.status_code == 200:
			msg += '\ninternet connection ok'
		if verbose:
			print(msg + '\nHere we go...')
	except:
		sys.exit('Run `sudo ./sh/start_4s.sh` and check internet connection. Thank you and have a nice day.')

	logging.info('main')
	
	if fetchngo is not None: # if fetching from bibdata.princeton.edu...
		get_bibdata()
	else: # if parsing files already in the IN dir
		mrcrecs = os.walk(INDIR).next()[2]
		mrcrecs.sort(key=alphanum_key)
		if not mrcrecs:
			sys.exit('-'*75+'\nThere are no MARC records in the IN directory (or IN doens\'t exist). Add some and try again.\n'+'-'*75)
		else:
			for mrcrec in mrcrecs:
				if verbose:
					print(mrcrec)
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
	Create tmp, in, out, reports, log dirs and csv files
	'''
	logging.info('setup')

	if (not os.path.exists(TMPDIR)) and (justfetch is None):
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
	
	schemelist = []
	if (csvout or nomarc) and (subjects or names): #or owis):
		if subjects:
			schemelist.append('sub')
		if names:
			schemelist.append('nam')
		for scheme in schemelist:
			rpt = REPORTS+'/'+fname+'_'+scheme+'_'+today+'.csv'
			try:
				os.rename(rpt, rpt + '.bak') # back up output from previous runs on same day
			except OSError:
				pass
				
			with open(rpt,'wb+') as outfile:
				heading = ['bib','heading','uri','tag','source']
				writer = csv.writer(outfile)
				writer.writerow(heading)


def cleanup():
	'''
	Clean up IN and TMP dirs
	'''
	msg = ''
	indir_count = str(len([name for name in os.listdir(INDIR) if os.path.isfile(os.path.join(INDIR, name))]))
	outdir_count = str(len([name for name in os.listdir(OUTDIR) if os.path.isfile(os.path.join(OUTDIR, name))]))
	
	if justfetch is None and keep == False:
		tempdirs = [TMPDIR]
		for d in tempdirs:
			if os.path.isdir(d):
				shutil.rmtree(d)
			else:
				print(d + ' didn\'t exist.')
	
	if justfetch or fetchngo:
		msg = indir_count + ' mrx files are in the IN dir.'
		logging.info(msg)
	if fetchngo:
		msg = outdir_count + ' enhanced records are in OUT dir.'
		logging.info(msg)
	if csvout:
		msg = 'Reports are in reports dir.'
		logging.info(msg)
	print('See ya.')
	
	logging.info('cleanup')


def get_bibdata():
	'''
	Query the PUL bibdata service
	'''
	logging.info('get_bibdata')
	
	conn = httplib.HTTPSConnection("bibdata.princeton.edu")
	flag = ""
	NS = "{http://www.loc.gov/MARC21/slim}"
		
	if fetchngo is not None:
		picklist = fetchngo
	else:
		picklist = justfetch
	
	with open(picklist,'rb') as csvfile:
			reader = csv.reader(csvfile,delimiter=',', quotechar='"')
			firstline = reader.next() # skip header row
			
			count = 0
			
			for row in reader:
				
				count += 1 # just for verbose mode
				
				bibid = row[0]
	
				conn.request("GET", "/bibliographic/"+bibid)
				got = conn.getresponse()
				data = got.read()		
				conn.close() 
				
				f = open(INDIR+bibid+'.mrx', 'w')
				f2 = open(LOG+'out.txt', 'a') # simple log
							
				try:
					doc = etree.fromstring(data)
					data = etree.tostring(doc,pretty_print=False,encoding='utf-8')
					f001 = doc.find("marc:record/marc:controlfield[@tag=\'001\']",namespaces={'marc':'http://www.loc.gov/MARC21/slim'})
					f.writelines(data)
					f.close()
					flag = "ok"
					f2.write("%s %s\n" % (bibid, flag))
					#time.sleep(1)
					if justfetch is None:
						read_mrx(bibid+'.mrx',names,subjects)
				except: # As when record has been suppressed after initial report was run, in which case, no xml
					etype,evalue,etraceback = sys.exc_info()
					flag = "problem: %s %s %s" % (etype,evalue,etraceback)
					f2.write("%s %s\n" % (bibid, flag))

				f2.close()
					
				if verbose:
					print("(%s) Got %s %s" % (count, bibid, flag))


def query_4s(label, scheme, childrens):
	'''
	SPARQL query
	'''
	# SPARQL endpoint(s), one for each scheme (names, subjects)
	if scheme == 'nam':
		host = "http://localhost:8001/"
	elif scheme == 'sub':
		host = "http://localhost:8000/"
	##label = urllib.quote_plus(label) # this doesn't work, keeping as reminder
	label = label.replace('"',"%22") # see bib 568 "Problemna..." (heading with double quotes).
	# query for notes as well, to eliminate headings that are to be used as subdivisions (see e.g. 'Marriage')
	query = 'SELECT ?s ?note WHERE { ?s ?p "%s"@en . OPTIONAL {?s <http://www.w3.org/2004/02/skos/core#note> ?note .FILTER(CONTAINS(?note,"subdivision")) .}}' % label
	
	data = { 'query': query}
	headers={ 'content-type':'application/x-www-form-urlencoded'}
	
	r = requests.post(host + "sparql/", data=data, headers=headers )
	if r.status_code != requests.codes.ok:   # <= something went wrong with 4store
		msg = '%s, %s' % (label, r.text)
		sys.exit(msg)
	try:
		doc = etree.fromstring(r.text)
	except:
		return None

	xpth = "//sparql:binding[@name='s'][not(following-sibling::sparql:binding[@name='note'])]/sparql:uri"
	
	if childrens == 1:
		xpth += "[contains(.,'childrensSubjects')]"
	else: 
		xpth += "[not(contains(.,'childrensSubjects'))]"

	for triple in doc.xpath(xpth,namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'}):
		return triple.text


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
				recs.append(r)
			if subjects:
				en,r = check_heading(bbid,rec,'sub')
				enhanced.append(en)
				recs.append(r)
			if nomarc == False and True in enhanced:
				for record in recs:
					if record is not None:
						outfile = str.replace(mrcrec,'.xml','')
						fh = open(TMPDIR+outfile+'_tmp.xml', 'wb+')
						fh.write(mrxheader)
						try:
							out = "%s" % (pymarc.record_to_xml(record))
							fh.write(out)
						except Exception as e:
							raise					
				
		if nomarc == False and True in enhanced:
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
		flag = "read_mrx problem: %s %s %s" % (etype,evalue,etraceback)
		if csvout or nomarc: # idea here is to report something out even when mrx has issues
			write_csv(bbid,flag,'',scheme, '','')
		print(flag)

	if not nomarc and True in enhanced:
		try:
			subprocess.Popen(['xmllint','--format','-o', OUTDIR+outfile, TMPDIR+outfile+'_tmp.xml']).wait()
		except:
			etype,evalue,etraceback = sys.exc_info()
			print("xmllint problem: %s" % evalue)

	if (justfetch is None and keep == False):
		os.remove(INDIR+mrcrec)


def query_lc(subject, scheme):
	'''
	Query id.loc.gov (but only after checking the local file)
	'''
	# First, check the cache
	src = 'id.loc.gov'
	cached = False
	datediff = 0
	con = lite.connect(DB) # sqlite3 table with fields heading, scheme, uri, date
	with con:
		con.row_factory = lite.Row
		cur = con.cursor()
		cur.execute("SELECT * FROM headings WHERE heading=? and scheme=?",(subject.decode('utf8'),scheme,))
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
		if (cached == True and datediff <= maxage): #and ignore_cache == False):
			src += ' (cache)'
			return uri,src
			
	if (cached == True and datediff > maxage) or cached == False or (ignore_cache == True and uri == 'None (404)'):
		# ping id.loc only if not found in cache, or if checked long, long ago
		subject = subject.replace('&','%26')
		subject = subject.decode('utf8')
		to_get = ID_SUBJECT_RESOLVER + subject
		headers = {"Accept":"application/xml"}
		time.sleep(1)
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
						
				cache_it(uri,cached,subject,scheme)
				
				return uri,src # ==>
				
			else:
				if (scheme == 'nam' and 'authorities/subjects' in uri):
					msg = 'None (wrong schema: %s)' % uri[18:]
				elif (scheme == 'sub' and 'authorities/names' in uri):
					msg = 'None (wrong schema: %s)' % uri[18:]
				return msg,src
		elif resp.status_code == 404:
			msg = "None (404)"
			cache_it(msg,cached,subject,scheme)
			return msg,src
		else: # resp.status_code != 404 and status != 200:
			msg = "None (" + resp.status_code + ")"
			cache_it(msg,cached,subject,scheme)
			return msg,src


def cache_it(uri,cached,subject, scheme):
	'''
	Put uris from id.loc.gov into cache (or update the date checked)
	'''
	con = lite.connect(DB)
	print('[cache_it] %s %s %s %s' % (uri,cached,subject, scheme))
	try:
		with con:
			cur = con.cursor() 
			if cached == True:
				updateurl = (today, subject, uri)
				cur.executemany("UPDATE headings SET date=? WHERE heading=? and uri=?", (updateurl,))
			else:
				newuri = (subject, scheme, uri, today)
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
	for f in rec.get_fields(*fields):
		mrx_subs = []
		h1 = ''
		h2 = ''
		src = '4store'
		childrens = 0
		if scheme == 'sub':
			childrens = int(f.indicators[1]) # second indicator '1' is LC subject headings for children's literature
		for s in f.get_subfields(*subfields):
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
		uri = query_4s(h,scheme, childrens)
		src = '4store'
		if uri is None:
			h1 = h.rstrip('.').rstrip(',')
			h1 = re.sub('(^\[|\]$)','',h1) # remove surrounding brackets
			uri = query_4s(h1,scheme, childrens)
			src = '4store'
			if uri is None and noidloc == False: # <= if still not found in 4store, with or without trailing punct., ping id.loc.gov
				try:
					uri,src = query_lc(h,scheme)
				except:
					#src = 'id.loc'
					pass # as when uri has 'classification'
				if uri is None or not uri.startswith('http'): # <= if not found, try without trailing punct.
					h2 = h.rstrip('.').rstrip(',')
					h2 = re.sub('(^\[|\]$)','',h2)
					try:
						uri,src = query_lc(h2,scheme)
					except:
						#src = 'id.loc'
						pass # as when uri has 'classification'
		if nomarc == False and uri is not None and uri.startswith('http'):
			# check for existing id.loc $0 and compare if present
			existing_sub0 = f.get_subfields('0')
			if existing_sub0:
				existing_sub0 = existing_sub0[0].encode('utf8').strip()
				if existing_sub0 == uri:
					src = 'already has %s' % existing_sub0
					enhanced = True
				elif 'id.loc.gov/authorities/' in existing_sub0: # <= if the url id.loc.gov but is different...
					pymarc.Field.delete_subfield(f,"0") # <=  ...assume it was wrong and delete it...
					uri = '(uri) ' + uri
					pymarc.Field.add_subfield(f,"0",uri) # <= ...then insert the new one.
					src = 'REPLACED %s with %s' % (existing_sub0,uri)
					enhanced = True
				else:
					uri = '(uri) ' + uri
					pymarc.Field.add_subfield(f,"0",uri)
					enhanced = True
			else:
				uri = '(uri) ' + uri
				pymarc.Field.add_subfield(f,"0",uri)
				enhanced = True
		if h2 != '' and uri is None:
			heading = h
		elif (h2 != '' and uri.startswith('http') == True):
			heading = h2
		elif (h1 != '' and uri.startswith('http') == True):
			heading = h1
		elif (uri.startswith('http') == False and re.match('.*[\.,]$',h)): 
			heading = h[:-1] + '['+h[-1:]+']' # to indicate that it's been searched with and without . or ,
		else:
			heading = h
	
		if verbose:
			print('%s, %s, %s, %s' % (bbid, heading.decode('utf8'), uri, src))
		if csvout or nomarc:
			write_csv(bbid, heading, uri, scheme,f.tag,src)

	if enhanced == False:
		return enhanced, None
	else:
		return enhanced, rec
		

def write_csv(bbid, heading, uri, scheme, tag, src):
	'''
	Write out csv reports, one each for names ('_nam_') and subjects ('_sub_')
	'''
	with open(REPORTS+'/'+fname+'_'+scheme+'_'+today+'.csv','ab+') as outfile:
		writer = csv.writer(outfile)
		row = (bbid, heading, uri, tag, src)
		writer.writerow(row)

	
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Generate hold reports.')
	parser.add_argument("-v", "--verbose",required=False, default=False, dest="verbose", action="store_true", help="Runtime feedback.")
	parser.add_argument("-n", "--names", required=False, default=False, dest="names", action="store_true", help="Get URIs for names.")
	parser.add_argument("-s", "--subjects", required=False, default=False, dest="subjects", action="store_true", help="Get URIs for subjects.")
	parser.add_argument("-r", "--report", required=False, default=False, dest="csvout", action="store_true", help="Output csv reports as well as MARCXML records.")
	parser.add_argument("-R", "--Report", required=False, default=False, dest="nomarc", action="store_true", help="Output csv reports but do NOT output MARCXML records. Overrides -F.")
	parser.add_argument("-f", "--fetch", type=str, required=False,dest="justfetch", help="Just fetch records listed in the given file. They will go into IN dir (and stay there). To enhance them, run again WITHOUT -f or -F flags.")
	parser.add_argument("-F", "--Fetch", type=str, required=False,dest="fetchngo", help="Fetch records listed in the given file and then enhance them 'on the fly'. Records are not left on disk.")
	parser.add_argument("-k", "--keep",required=False, default=False, dest="keep", action="store_true", help="Keep IN and TMP dirs.")
	parser.add_argument('-a','--age',dest="maxage",help="Max days after which to re-check id.loc.gov",required=False, default=30)
	parser.add_argument('-i','--ignore',dest="noidloc",required=False, default=False, action="store_true",help="Ignore id.loc.gov")
	parser.add_argument("-C", "--ignore-cache",required=False, default=False,dest="ignore_cache", action="store_true", help="Optionally ignore 404 errors in cache (so check these headings against id.loc.gov again).")

	args = vars(parser.parse_args())
	verbose = args['verbose']
	names = args['names']
	subjects = args['subjects']
	nomarc = args['nomarc']
	csvout = args['csvout']
	justfetch = args['justfetch']
	fetchngo = args['fetchngo']
	keep = args['keep']
	maxage = int(args['maxage'])
	noidloc = args['noidloc']
	ignore_cache = args['ignore_cache']
	fname = ''
	
	if justfetch or fetchngo:
		if justfetch:
			fname = os.path.basename(justfetch).replace('.csv','')
		elif fetchngo:
			fname = os.path.basename(fetchngo).replace('.csv','')
	
	logging.info('='*50)
	setup()
	if justfetch is not None:
		get_bibdata()
	else:
		main()
	cleanup()
	logging.info('='*50)
