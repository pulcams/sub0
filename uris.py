#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Linked data prep. Get URIs and (optionally) insert $0 into MARCXML records.
Works with lcnaf and lcsaf in local dump. Removed '(uri)' prefix 20170215.

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
# Add option for checking Voyager auth tables?

# config
config = ConfigParser.RawConfigParser()
config.read('/home/local/PRINCETON/pmgreen/sub0/config/uris.cfg')
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
	logging.info('starting main() function')

	try:
		mrcrecs = os.walk(INDIR).next()[2]
		mrcrecs.sort(key=alphanum_key)
		if not mrcrecs:
			msg = '-'*75+'\nThere are no marcxml records in the IN directory (or IN doens\'t exist). Add some and try again.\n'+'-'*75
			logging.info(msg)
			sys.exit(msg)
		else:
			for mrcrec in mrcrecs:
				msg = 'current file is %s' % mrcrec
				logging.info(msg)
				if verbose:
					print(msg)
				read_mrx(mrcrec,names,subjects)
	except Exception,e:
		logging.info(str(e))


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
	logging.info('setting up')

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

			enhanced_bibs = REPORTS+'/_enhanced_bibs_'+today+'.csv'
			with open(enhanced_bibs,'wb+') as enhanced_bib_outfile:
				enhanced_bib_heading = ['enhanced_bib']
				writer = csv.writer(enhanced_bib_outfile)
				writer.writerow(enhanced_bib_heading)


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
	if verbose:
		print('See ya.' + '\n' + '.'*50)
	
	logging.info('cleaned up')


def query_local(label, scheme, thesaurus):
	'''
	SPARQL query
	'''
	src = 'local'
	
	# SPARQL endpoint(s), one for each scheme (names, subjects)
	if scheme == 'nam':
		#host = "http://127.0.0.1:8001/"
		#host = "http://localhost:3030/lcnaf/"
		host = "http://ec2-52-201-199-177.compute-1.amazonaws.com:3030/lcnaf/"
	elif scheme == 'sub':
		#host = "http://127.0.0.1:8000/"
		#host = "http://localhost:3030/lcsaf/"
		host = "http://ec2-52-201-199-177.compute-1.amazonaws.com:3030/lcsaf/"

	try:
		label = label.strip()
		label = re.sub('\s+',' ',label)
		label = label.replace('"','\u005C\u0022') # e.g. bib 568 "Problemna..." (heading with double quotes).
		#label = label.replace('"','\u0022') # 4store
		# replace combined characters (id does this automatically)
		label = unicodedata.normalize('NFC', label.decode('utf8'))
		# Modify the Voyager heading variable. TODO: do we want to change them in record, if found, or flag as needing edi?
		## label = re.sub('(\s[A-Z]\.)([A-Z]\.)',r'\g<1> \g<2>',label) # insert space between initials
		label = re.sub('(\sb\.)([^\s])',r'\g<1> \g<2>',label) # insert space after ' b.' 
		label = re.sub('^([A-Z]\.)\s([A-Z]\.)',r'\g<1>\g<2>',label) # remove ' ' between initials at start of string
		label = re.sub('\(([A-Z]\.)\s([A-Z]\.)\)',r'\g<1>\g<2>',label) # remove ' ' between initials at start of string or in parens
		label = re.sub('(\sCo$)',r'\g<1>.',label) # insert period after " Co" 
		label = re.sub('(\s[A-Z]$)',r'\g<1>.',label) # insert period after concluding initial ' A.'
		label = re.sub('\s(\,)',r'\g<1>',label) # replace ' ,'
		label = re.sub('([a-z])(\()',r'\g<1> \g<2>',label) # replace 'a(' with 'a ('
		label = label.replace('-\L','-L') # ? replace '\L' 509574 Mulher-\Libertação
		label = re.sub('\\$','',label) # bib 584803 had ' \' at end of subject
		label = re.sub('\\\\','%5c',label) # bib 6181483 includes firm 'Aranda\Lasch'

		# query for notes as well, to eliminate headings that are to be used as subdivisions (see e.g. 'Marriage')
		# !isBlank() might be replaced by isIRI()
		query = '''SELECT ?s ?note WHERE { ?s ?p "%s"@en . FILTER (!isBlank(?s)) . OPTIONAL {?s <http://www.w3.org/2004/02/skos/core#note> ?note .FILTER(CONTAINS(?note,"subdivision")) .}}''' % label
		
		# query for variants
		variant_query = '''SELECT distinct ?s WHERE {?s ?p ?bn  . ?bn <http://www.loc.gov/mads/rdf/v1#variantLabel> "%s"@en . }''' % label
	
		data = { 'query': query}
		headers={ 'Content-Type':'application/x-www-form-urlencoded','Accept':'application/sparql-results+xml' }

		r = requests.post(host + "sparql", data=data, headers=headers)

		if r.status_code != requests.codes.ok:
			msg = '%s, %s' % (label, r.text)
			sys.exit(msg)
		try:
			doc = etree.fromstring(r.text)
		except:
			return None,src

		xpth = "//sparql:binding[@name='s'][not(following-sibling::sparql:binding[@name='note'])]/sparql:uri[. != '(null)']"
		
		if thesaurus == 1:
			xpth += "[contains(.,'childrensSubjects')]"
		else: 
			xpth += "[not(contains(.,'childrensSubjects'))]"

		if len(doc.xpath(xpth,namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'})) > 0:
			for triple in doc.xpath(xpth,namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'}):
				return triple.text, src
		else:
			return None,src
			data = { 'query': variant_query}
			
			r = requests.post(host + "sparql/", data=data, headers=headers )
			if r.status_code != requests.codes.ok: 
				msg = '%s, %s' % (label, r.text)
				sys.exit(msg)
			try:
				doc = etree.fromstring(r.text)
				if len(doc.xpath(xpth,namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'})) > 0:
					for triple in doc.xpath(xpth,namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'}):
						return triple.text, src
				else:
					None,src
			except:
				return None,src

	except:
		etype,evalue,etraceback = sys.exc_info()
		print("query_local problem %s %s %s line: %s" % (etype,evalue,etraceback,etraceback.tb_lineno))


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
				if ((enhanced_only == True and en == True) or (enhanced_only == False)):
					recs.append(r)
			if subjects: # if just searching subjects, or if a rec only has subjects, no names
				en,r = check_heading(bbid,rec,'sub')
				enhanced.append(en)
				if ((enhanced_only == True and en == True) or (enhanced_only == False)) and r not in recs:
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
		#if nomarc == False and ((enhanced_only == True and (True in enhanced)) or (enhanced_only == False)):
		#	print('#########################',nomarc,True in enhanced,enhanced_only)
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
		print(flag)

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
	
	if ignore_cache == False: # First, check the cache (if not ignoring it)
	
		cached,datediff,uri = check_cache(heading,scheme)
		
		if (cached == True and datediff <= maxage):
			src += ' (cache)'
			return uri,src

	if noidloc == False: # if checking id.loc...
		if (((cached == True and datediff > maxage) or (cached == False)) or (ignore_cache == True)):
			# ping id.loc only if not found in cache, or if checked long, long ago
			heading = heading.replace('&','%26')
			heading = heading.decode('utf8')
			to_get = ID_HEADING_RESOLVER + heading
			# user-agent became necessary July 2016 (see sm email to BF list 07/29/2016)
			headers = {"Accept":"application/xml","User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0"}
			time.sleep(1) # http://id.loc.gov/robots.txt has 3 secs but kh says not necessary w head requests
			resp = requests.head(to_get, headers=headers, allow_redirects=True)
			if resp.status_code == 200:
				uri = resp.headers["x-uri"]
				if (scheme == 'nam' and 'authorities/names' in uri) or (scheme == 'sub' and 'authorities/subjects' in uri):
				#if ('authorities/names' in uri or 'authorities/subjects' in uri):
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
					
				#else:
				#	if (scheme == 'nam' and 'authorities/subjects' in uri):
				#		msg = 'None (wrong schema: %s)' % uri[18:]
				#	elif (scheme == 'sub' and 'authorities/names' in uri):
				#		msg = 'None (wrong schema: %s)' % uri[18:]
				#	return msg,src # ==>
					
			elif resp.status_code == 404:
				print(heading, resp.status_code)
				msg = "None (404)"
				cache_it(msg,cached,heading,scheme)
				return msg,src # ==>
				
			else: # if resp.status_code != 404 and status != 200:
				msg = "None (" + resp.status_code + ")"
				cache_it(msg,cached,heading,scheme)
				return msg,src # ==>
	else:
		src += ' (ignored)'
		return None,src


def cache_it(uri,cached,heading, scheme):
	'''
	Put uris from id.loc.gov into cache (or update the date checked)
	'''
	con = lite.connect(DB)
	heading = heading.replace('%26','&')
	if verbose:
		print('==> cache_it() %s | already cached: %s | %s | %s' % (uri,cached,heading, scheme))
	try:
		with con:
			cur = con.cursor() 
			if cached == True:
				updateurl = (today, heading, uri)
				cur.executemany("UPDATE headings SET date=? WHERE heading=? and uri=?", (updateurl,))
			else:
				newuri = (heading, scheme, uri, today)
				# scheme isn't really important here, but keeping for the moment
				cur.executemany("INSERT INTO headings VALUES(?, ?, ?, ?)", (newuri,))
	except Exception as e:
		print('cache_it error %s' % e.value)
		pass
		
	if con:
		con.close()


def check_heading(bbid,rec,scheme):
	'''
	Check a given heading against local dump and, if that fails, id.loc.gov
	'''
	enhanced = False
	heading = ''
	nam_scheme = ''
	if scheme == 'sub':
		# get subjects data from these subfields (all but 0,2,3,6,8)
		fields = ['600','610','611','650','651'] # removing 630 20171218
		subfields = ['a', 'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 
	'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'x', 'y', 'z', '4']
	elif scheme == 'nam':
		# get names data from these subfields
		fields = ['100','110','700','710'] # removing 130, 730 20171218
		subfields = ['a','b','c','d','q']
	if not rec.get_fields(*fields):
		write_csv(bbid,'','',scheme,'','') # report out records with none of the selected tags/subfields
	try:
		for f in rec.get_fields(*fields):
			mrx_subs = []
			h1 = ''
			h2 = ''
			thesaurus = 0
			if scheme == 'sub':
				if f.indicators[1] in (1,0):
					thesaurus = int(f.indicators[1]) # 6xx ind2: '1' is LC subject headings for children's literature
				
			for s in f.get_subfields(*subfields):
				if scheme == 'nam' and re.match('\d00',f.tag) and not f.get_subfields('c','q','d'):
					continue # continue with next iteration of loop (skip plain names)
				s = s.encode('utf8').strip()
				# TODO: get a fuller list
				# Strip terminating punct. from the end of each subfield, but
				# account for abbreviations (using negative lookbehind)
				re.sub('(?<!(\sco))\.','',s,flags=re.IGNORECASE) # preserve '.' at end of Co.
				re.sub('(?<!(\sinc))\.','',s,flags=re.IGNORECASE) # preserve '.' at end of Inc.
				re.sub('(?<!(\sdept))\.','',s,flags=re.IGNORECASE) # preserve '.' at end of Dept.
				mrx_subs.append(s)
			if not mrx_subs:
				continue # continue with next iteration of loop (e.g. prevent empty heading when 730 only has $t)
			h = "--".join(mrx_subs)
			h = h.replace(',--',', ')
			h = h.replace('.--','. ')
			h = h.replace('--(',' (') # $q

			# local ==================================
			uri,src = query_local(h, scheme, thesaurus) # <= check local dump (with terminating punct)
			#=========================================
			
			if uri is None: # if nothing found, modify it and try again
				terminating = re.search('[\.,]+$',h)
				punct = terminating.group(0) if terminating is not None else None
				
				if punct: # if there's terminating punct, will try with and (if necessary) without it
					h1 = h.rstrip(punct)
					h1 = re.sub('(^\[|\]$)','',h1) # remove surrounding brackets also

					# local ===================================
					uri,src = query_local(h1, scheme, thesaurus) # <= check local dump (no terminating punct)
					#==========================================

					if uri == None and scheme == 'sub' and f.tag != '650': # if not 650 try lcnaf
						if f.tag != '651' or (f.tag == '651' and not f.get_subfields('e','g','4','v','x','y','z','2','3','6','8')):
							nam_scheme = 'nam' 
							
							# local ==================================	
							uri,src = query_local(h1, nam_scheme, thesaurus) # <= check local dump, other scheme, no terminating punct
							#=========================================
	
							if uri == None:
								# local =================================	
								uri,src = query_local(h, nam_scheme, thesaurus) # <= check local dump, other scheme, with terminating punct
								#========================================
							
								if uri == None: # if still not found in local dump, try id.loc (w and wo trailing punct)
									try:
										# id.loc =====================
										uri,src = query_lc(h,scheme) # <= ping id with terminating punct
										#=============================
									except:
										pass # (as when uri has 'classification')
										
									if uri is None or not uri.startswith('http'): # <= if still not found, try with no term. punct.
										if punct:
											h2 = h.rstrip(punct)
											h2 = re.sub('(^\[|\]$)','',h2) # remove surrounding brackets also
											try:
												# id.loc ======================
												uri,src = query_lc(h2,scheme) # <= ping id without trailing punct
												#==============================
											except:
												pass # as when uri has 'classification'
											
				else: # if there was no terminating punct., try local dump w other scheme, then (if needed) id.loc...
					
					if scheme == 'sub' and f.tag != '650':
						if f.tag != '651' or (f.tag == '651' and not f.get_subfields('e','g','4','v','x','y','z','2','3','6','8')):
							nam_scheme = 'nam'

							# local =====================================		
							uri,src = query_local(h, nam_scheme, thesaurus) # <= check local dump again, other scheme
							#============================================
		
							if uri is None:
								try:
									# id.loc ===================
									uri,src = query_lc(h,scheme)
									#===========================
								except:
									pass
							
			if nomarc == False and (uri is not None and uri.startswith('http')):
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
							#prefixuri = '(uri)' + uri
							pymarc.Field.add_subfield(f,"0",uri) # <= ...then insert the new one.
							src = 'REPLACED %s with %s' % (existing_sub0,uri)
							enhanced = True
						else:
							#prefixuri = '(uri)' + uri
							pymarc.Field.add_subfield(f,"0",uri)
							enhanced = True
				else:
					#prefixuri = '(uri)' + uri
					pymarc.Field.add_subfield(f,"0",uri)
					enhanced = True
			
			if uri is None:
				if h2 is not None and h2 != '':
					heading = h2
				elif h1 is not None and h1 != '':
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
				write_csv(bbid, heading, uri, scheme, f.tag, src)

		if enhanced_only == False or (enhanced_only == True and enhanced == True):
			return enhanced, rec # ==>
		elif (enhanced_only == True and enhanced == False):
			return enhanced, None # ==> add bib to report, but no marcxml record in the out dir (see read_mrx)
				
	except:
		etype,evalue,etraceback = sys.exc_info()
		print("check_heading problem %s %s %s line: %s" % (etype,evalue,etraceback,etraceback.tb_lineno))


def write_csv(bbid, heading, uri, scheme, tag, src):
	'''
	Write out csv reports, one each for names ('_nam_') and subjects ('_sub_')
	'''
	# TODO sqlite db
	with open(REPORTS+'/_'+scheme+'_'+today+'.csv','ab+') as outfile:
		writer = csv.writer(outfile)
		row = (bbid, heading, uri, tag, src)
		writer.writerow(row)

	with open(REPORTS+'/_enhanced_bibs_'+today+'.csv','ab+') as enhanced_bibs_outfile:
		enhanced_bibs_file = enhanced_bibs_outfile.read()
		enhanced_bibs_writer = csv.writer(enhanced_bibs_outfile)
		if bbid not in enhanced_bibs_file and (uri and uri.startswith('http')):
			row = (bbid,)
			enhanced_bibs_writer.writerow(row)


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
			cur.execute("SELECT * FROM headings WHERE heading=?",(heading.decode('utf8'),))
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
	parser = argparse.ArgumentParser(description='Generate uri reports.')
	parser.add_argument("-v", "--verbose", required=False, default=False, dest="verbose", action="store_true", help="Runtime feedback.")
	parser.add_argument("-n", "--names", required=False, default=False, dest="names", action="store_true", help="Get URIs for names.")
	parser.add_argument("-s", "--subjects", required=False, default=False, dest="subjects", action="store_true", help="Get URIs for subjects.")
	parser.add_argument("-r", "--report", required=False, default=True, dest="csvout", action="store_true", help="Output csv reports as well as MARCXML records.")
	parser.add_argument("-R", "--Report", required=False, default=False, dest="nomarc", action="store_true", help="Output csv reports but do NOT output MARCXML records.")
	parser.add_argument("-k", "--keep", required=False, default=False, dest="keep", action="store_true", help="Keep IN and TMP dirs.")
	parser.add_argument("-a",'--age', required=False, dest="maxage",help="Max days after which to re-check id.loc.gov", default=30)
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
