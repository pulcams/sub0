#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Linked data prep. Get URIs and (optionally) insert $0 into MARCXML records.
Works with lcnaf and lcsaf in 4store.

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
import pickle
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

# TODO
#X cache for id.loc
#X ping id.loc when not found in (old) downloaded files
# account for suppressed recs (exception in getbibdata)
# better logging--count of records checked, count of fields checked, uris vs not
#X remove OWI (defunct)
#X add delete -N and make -C for 'report plus MARC')

# config
config = ConfigParser.RawConfigParser()
config.read('./config/uris.cfg')
#config.read('./config/uris_aws.cfg') # <= aws ec2 instance
OUTDIR = config.get('env', 'outdir')
INDIR = config.get('env', 'indir')
TMPDIR = config.get('env', 'tmpdir')
REPORTS = config.get('env', 'reports')
LOG = config.get('env', 'logdir')
DB = config.get('db','sqlite')
ID_SUBJECT_RESOLVER = "http://id.loc.gov/authorities/label/"

today = time.strftime('%Y%m%d') # name log files
todaydb = time.strftime('%Y-%m-%d') # date to check against db

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=LOG+today+'.log',level=logging.INFO)
# the following two lines disable the default logging of requests.get()
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

class HeadingNotFoundException(Exception):
	def __init__(self, msg, heading, type, instead=None):
		super(HeadingNotFoundException, self).__init__(msg)
		"""
		@param msg: Message for logging
		@param heading: The heading we were searching when this was raised
		@param type: The type of heading (personal or corporate) 
		@param instead: The "Use instead" URI and string when heading is deprecated
		"""
		self.heading = heading
		self.type = type
		self.instead = instead

def main():
	'''
	Main
	'''
	logging.info('main')
	
	if fetchngo is not None: # if fetching from bibdata.princeton.edu...
		getbibdata()
	else: # if parsing files already in the IN dir
		mrcrecs = os.walk(INDIR).next()[2]
		if not mrcrecs:
			sys.exit('-'*75+'\nThere are no MARC records in the IN directory. Add some and try again.\n'+'-'*75)
		else:
			for mrcrec in mrcrecs: # TODO get count for log
				if verbose:
					print(mrcrec)
				readmrx(mrcrec,names,subjects)


def setup():
	'''
	Create tmp and in dirs, and csv file
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
	
	schemelist = []
	if (csvout or nomarc) and (subjects or names): #or owis):
		if subjects:
			schemelist.append('sub')
		if names:
			schemelist.append('nam')
		for scheme in schemelist:
			rpt = REPORTS+fname+'_'+scheme+'_'+today+'.csv'
			try:
				os.rename(rpt, rpt + '.bak') # just back up output from previous runs on same day
			except OSError:
				pass
				
			with open(rpt,'wb+') as outfile:
				heading = ['bib','heading','uri','source']
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


def getbibdata():
	'''
	Query the PUL getbibdata service
	'''
	logging.info('getbibdata')
	
	conn = httplib.HTTPConnection("bibdata.princeton.edu")
	flag = ""
	NS = "{http://www.loc.gov/MARC21/slim}"
		
	if fetchngo is not None:
		picklist = fetchngo
	else:
		picklist = justfetch
	
	with open(picklist,'rb') as csvfile: # TODO get count for log
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
					time.sleep(1)
					if justfetch is None:
						readmrx(bibid+'.mrx',names,subjects)
				except: #TODO pass? (As when record has been suppressed after initial report was run, in which case, no xml)
					etype,evalue,etraceback = sys.exc_info()
					flag = "problem: %s %s %s" % (etype,evalue,etraceback)
					f2.write("%s %s\n" % (bibid, flag))

				f2.close()
					
				if verbose:
					print("(%s) Got %s %s" % (count, bibid, flag))


def query_4s(label, scheme):
	'''
	SPARQL query
	'''
	# SPARQL endpoint(s), one for each scheme (names, subjects)
	if scheme == 'nam':
		host = "http://localhost:8001/"
	elif scheme == 'sub':
		host = "http://localhost:8000/"

	query = 'SELECT ?s WHERE { ?s ?p "%s"@en . }' % label
	data = { "query": query }
	
	r = requests.post(host + "sparql/", data=data)
	if r.status_code != requests.codes.ok:   # <= something went wrong
		print(label, r.text)

	doc = etree.fromstring(r.text)

	for triple in doc.xpath("//sparql:binding[@name='s']/sparql:uri",namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'}):
		return triple.text


def readmrx(mrcrec,names,subjects):
	'''
	Read through a given MARCXML file and copy, inserting $0 as appropriate
	'''
	mrxheader = """<?xml version="1.0" encoding="UTF-8" ?>
	<collection xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">"""
	try:
		reader = pymarc.marcxml.parse_xml_to_array(INDIR+mrcrec)
		if nomarc == False:
			outfile = str.replace(mrcrec,'.xml','')
			fh = open(TMPDIR+outfile+'_tmp.xml', 'wb+')
			fh.write(mrxheader)
		
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
				r = check(bbid,rec,'nam')
			if subjects:
				r = check(bbid,rec,'sub')	
			if nomarc == False:
				out = "%s" % (pymarc.record_to_xml(r))
				fh.write(out)
		if nomarc == False:
			fh.write("</collection>")
			fh.close()
	except:
		etype,evalue,etraceback = sys.exc_info()
		print("readmrx problem: %s %s" % (etype,evalue))

	if not nomarc:
		try:
			subprocess.Popen(['xmllint','--format','-o', OUTDIR+outfile, TMPDIR+outfile+'_tmp.xml']).wait()
		except:
			etype,evalue,etraceback = sys.exc_info()
			print("xmllint problem: %s" % evalue)
				
	# uncomment this to delete each record immediately after processing
	if justfetch is None and keep == False:
		os.remove(INDIR+mrcrec)


def query_lc(subject, scheme):
	"""
	Query id.loc.gov (but only after checking the local file)
	"""
	# First, check the cache
	src = 'id.loc.gov'
	cached = False
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
	if (cached == True and datediff <= maxage):
		src += ' (cache)'
		return uri,src
	elif (cached == True and datediff > maxage) or cached == False:
		# ping id.loc only if not found in cache, or if checked long, long ago
		to_get = ID_SUBJECT_RESOLVER + subject
		headers = {"Accept":"application/xml"}
		resp = requests.get(to_get, headers=headers, allow_redirects=True)
		if resp.status_code == 200:
			uri = resp.headers["x-uri"]
			if (scheme == 'nam' and 'authorities/names' in uri) or (scheme == 'sub' and 'authorities/subjects' in uri):
				try: 
					label = resp.headers["x-preflabel"]
				except: # x-preflabel is not returned for deprecated headings
					uri = "None (deprecated)"
					tree = html.fromstring(resp.text)
					see = tree.xpath("//h3/text()= 'Use Instead'") # this info isn't in the header, so grabbing from html
					seeother = ''
					if see:
						other = tree.xpath("//h3[text() = 'Use Instead']/following-sibling::ul//div/a")[0]
						seeother = (other.attrib['href'], other.text)
					raise HeadingNotFoundException(msg, subject, 'subject',seeother) # put the see other url and value into the db
	
				# TODO: check that uri matches scheme
				
				with con:
					cur = con.cursor() 
					if cached == True:
						updateurl = (today, subject.decode('utf8'), uri)
						cur.executemany("UPDATE headings SET date=? WHERE heading=? and uri=?", (updateurl,))
						src+=' (cache)'
					else:
						newuri = (subject.decode('utf8'), scheme, uri, today)
						cur.executemany("INSERT INTO headings VALUES(?, ?, ?, ?)", (newuri,))
				if con:
					con.close()
				return uri, src # ==>
			
		elif resp.status_code == 404:
			msg = "None (404)"
			return msg,src
		else: # resp.status_code != 404 and status != 200:
			msg = "None (" + resp.status_code + ")"
			return msg,src


def check(bbid,rec,scheme):
	if scheme == 'sub':
		# get subjects data from these subfields (all but 0,2,3,6,8)
		fields = ['600','610','611','630','650','651']
		subfields = ['a', 'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 
	'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'x', 'y', 'z', '4']
	elif scheme == 'nam':
		# get names data from these subfields
		fields = ['100','110','130','700','710','730']
		subfields = ['a','c','d','q']
	for f in rec.get_fields(*fields):
		mrx_subs = []
		h1 = ''
		h2 = ''
		src = '4store'
		for s in f.get_subfields(*subfields):
			s = s.encode('utf8').strip()
			mrx_subs.append(s)
		h = "--".join(mrx_subs)
		h = h.replace(',--',', ')
		h = h.replace('--(',' (') # $q
		uri = query_4s(h,scheme)
		src = '4store'
		if uri is None:
			h1 = h.rstrip('.').rstrip(',')
			h1 = re.sub('(^\[|\]$)','',h1) # remove surrounding brackets
			uri = query_4s(h1,scheme)
			src = '4store'
		if uri is None and not noidloc: # <= if still not found in 4store, with or without trailing punct., ping id.loc.gov
			uri,src = query_lc(h,scheme)
			if not uri.startswith('http'): # <= if not found, try without trailing punct.
				h2 = h.rstrip('.').rstrip(',')
				h2 = re.sub('(^\[|\]$)','',h2)
				uri,src = query_lc(h2,scheme)
		if nomarc == False and uri is not None and not uri.startswith('http'):
			pymarc.Field.add_subfield(f,"0",uri)
			# TODO get count for log
			
		if (h2 != '' and uri.startswith('http') == True):
			heading = h2
		elif (h1 != '' and uri.startswith('http') == True):
			heading = h1
		elif (uri.startswith('http') == False and re.match('.*[\.,]$',h)): 
			heading = h[:-1] + '['+h[-1:]+']'
		else:
			heading = h

		if verbose:
			print('%s, %s, %s, %s' % (bbid, heading.decode('utf8'), uri, src))
		if csvout or nomarc:
			write_csv(bbid, heading, uri, scheme, src)
	return rec


def write_csv(bbid, heading, uri, scheme, src):
	'''
	Write out csv reports
	'''
	with open(REPORTS+fname+'_'+scheme+'_'+today+'.csv','ab+') as outfile:
		writer = csv.writer(outfile)
		row = (bbid, heading, uri,src)
		writer.writerow(row)

	
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Generate hold reports.')
	parser.add_argument("-v", "--verbose",required=False, default=False, dest="verbose", action="store_true", help="Runtime feedback.")
	parser.add_argument("-n", "--names", required=False, default=False, dest="names", action="store_true", help="Get URIs for names.")
	parser.add_argument("-s", "--subjects", required=False, default=False, dest="subjects", action="store_true", help="Get URIs for subjects.")
	parser.add_argument("-r", "--report", required=False, default=False, dest="csvout", action="store_true", help="Output csv reports as well as MARCXML records.")
	parser.add_argument("-R", "--Report", required=False, default=False, dest="nomarc", action="store_true", help="Output csv reports but do NOT output MARCXML records.")
	parser.add_argument("-f", "--fetch", type=str, required=False,dest="justfetch", help="Just fetch records listed in the given file. They will go into IN dir (and stay there). To enhance them, run again WITHOUT -f or -F flags.")
	parser.add_argument("-F", "--Fetch", type=str, required=False,dest="fetchngo", help="Fetch records listed in the given file and then enhance them 'on the fly'. Records are not left on disk.")
	parser.add_argument("-k", "--keep",required=False, default=False, dest="keep", action="store_true", help="Keep IN and TMP dirs.")
	parser.add_argument('-a','--age',dest="maxage",help="Max days after which to re-check WorldCat",required=False, default=30)
	parser.add_argument('-i','--ignore',dest="noidloc",required=False, default=False, action="store_true",help="Ignore id.loc.gov")

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
	fname = ''
	
	if justfetch or fetchngo:
		if justfetch:
			fname = os.path.basename(justfetch).replace('.csv','')
		elif fetchngo:
			fname = os.path.basename(fetchngo).replace('.csv','')
	
	logging.info('='*50)
	setup()
	if justfetch is not None:
		getbibdata()
	else:
		main()
	cleanup()
	logging.info('='*50)
