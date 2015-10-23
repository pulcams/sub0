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
import subprocess
import sys
import time
import urllib
from lxml import etree
#from HTTP4Store import HTTP4Store

# TODO
# account for suppressed recs (exception in getbibdata)
# better logging
#X remove OWI (defunct)
#X add delete -N and make -C (for 'report plus MARC')

# config
config = ConfigParser.RawConfigParser()
config.read('./config/sub0.cfg')
#config.read('./config/sub0_aws.cfg') # <= aws ec2 instance
OUTDIR = config.get('env', 'outdir')
INDIR = config.get('env', 'indir')
TMPDIR = config.get('env', 'tmpdir')
REPORTS = config.get('env', 'reports')
LOG = config.get('env', 'logdir')
DB_FILE =  config.get('env', 'db')
AI =  config.get('env', 'ai')

workid = ''
today = time.strftime('%Y%m%d') # name log files

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=LOG+today+'.log',level=logging.INFO)
# the following two lines disable the default logging of requests.get()
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


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
			for mrcrec in mrcrecs:
				#outfile = str.replace(mrcrec,'.xml','')
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
	if csvout and (subjects or names): #or owis):
		if subjects:
			schemelist.append('sub')
		if names:
			schemelist.append('nam')
		#if owis:
		#	schemelist.append('owi')
		for scheme in schemelist:
			try:
				os.rename(REPORTS+fname+'_'+scheme+'_'+today+'.csv' + '.bak') # just back up output from previous runs on same day
			except OSError:
				pass
				
			with open(REPORTS+fname+'_'+scheme+'_'+today+'.csv','wb+') as outfile:
			#	if scheme == 'owi':
			#		heading = ['bib','oclcnum','uri']
			#	else:
				heading = ['bib','heading','uri']
				writer = csv.writer(outfile)
				writer.writerow(heading)


def cleanup():
	'''
	Clean up IN and TMP dirs
	'''
	msg = ''
	indir_count = str(len([name for name in os.listdir(INDIR) if os.path.isfile(os.path.join(INDIR, name))]))
	outdir_count = str(len([name for name in os.listdir(OUTDIR) if os.path.isfile(os.path.join(OUTDIR, name))]))
	
	if justfetch is None: # if not just fetching files, delete IN dir too
		tempdirs = [INDIR, TMPDIR]
		for d in tempdirs:
			if os.path.isdir(d):
				#choice = raw_input("Delete the temp working folder? [Y/n] ")
				#if choice in ("Y","y",""):
				shutil.rmtree(d)
				#else:
				#	print(d + ' was NOT removed.')
			else:
				print(d + ' didn\'t exist.')
	
	if justfetch or fetchngo:
		msg = indir_count + ' mrx files were put into the IN dir.'
		logging.info(msg)
	if fetchngo:
		msg = outdir_count + ' enhanced records are in OUT dir.'
		logging.info(msg)
	#if owis:
	#	logging.info('Checked OWIs')
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
					time.sleep(1)
					if justfetch is None:
						readmrx(bibid+'.mrx',names,subjects)
				except: #TODO pass? (As when record has been suppressed after initial report was run)
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
	# SPARQL endpoint(s), one for each scheme (name, subjects)
	if scheme == 'nam':
		host = "http://localhost:8001/"
	elif scheme == 'sub':
		host = "http://localhost:8000/"

	query = 'SELECT ?s WHERE { ?s ?p "%s"@en . }' % label
	data = { "query": query }
	
	r = requests.post(host + "sparql/", data=data)
	if r.status_code != requests.codes.ok:   # something went wrong
		print(label, r.text)

	doc = etree.fromstring(r.text)
	for triple in doc.xpath("//sparql:binding[@name='s']/sparql:uri",namespaces={'sparql':'http://www.w3.org/2005/sparql-results#'}):
		return triple.text


#def query_oclc(xid):
	#'''
	#See the following for parameters:
	#http://www.oclc.org/developer/develop/web-services/xid-api/xstandardNumber-resource.en.html
	#'''
	#XID_RESOLVER = "http://xisbn.worldcat.org/webservices/xid/oclcnum/%s"
	#WORK_ID = "http://worldcat.org/entity/work/id/"
	#msg = ''
	#to_get = XID_RESOLVER % xid
	#to_get += "?method=getMetadata&format=xml&fl=*" # could also try &fl=owi
	#to_get += "&ai="+AI
	## print(to_get)
	#headers = {"Accept":"application/xml"}
	#resp = requests.get(to_get, headers=headers, allow_redirects=True)
	#if resp.status_code == 200:
		#doc = libxml2.parseDoc(resp.text.encode("UTF-8", errors="ignore"))
		#ctxt = doc.xpathNewContext()
		#if ctxt.xpathEval("//@stat[.='overlimit']"):
			#print("over limit with %s" % xid)
			#sys.exit(0)
		#else: 
			#try:
				#owi = ctxt.xpathEval("//@owi")[0].content
				#cleanowi = owi.replace('owi','')
				#return WORK_ID + cleanowi
			#except:
				#print("no owi found")
				
	#elif resp.status_code == 404:
		#msg = "Not found: %s%s" % (xid, os.linesep)
	#elif resp.status_code == 500:
		#msg = "Server error (%s)" % xid
	#else: # resp.status_code isn't 200, 404 or 500:
		#msg = " Response for %s was " % xid
		#msg += "%s%s" % (resp.status_code, os.linesep)
	#print(msg)
		
	#time.sleep(1)


#def check_owi_cache(ocn):
	#'''
	#Check OWI cache
	#'''
	#con = lite.connect(DB_FILE)
	#workid = ''
	#with con:
		#con.row_factory = lite.Row
		#cur = con.cursor()
		#cur.execute("SELECT * FROM owis WHERE ocn=?",(ocn,))
		#rows = cur.fetchall()
		#if len(rows) == 0:
			#workid = None
		#else:
			#for row in rows:
				#workid = row['uri']
			#if verbose:
				#os.sys.stdout.write("[Cache] Found: "+ocn+" "+workid+"\n") 
	#if workid is None:
		#workid = query_oclc(ocn)
		#if workid is not None and workid != '':
			#try:
				#with con:
					#cur = con.cursor() 
					#newone = (ocn,workid)
					#cur.executemany("INSERT INTO owis VALUES(?,?)", (newone,))
			#except:
				#str(sys.exc_info())
		
	#return workid


def readmrx(mrcrec,names,subjects):
	'''
	Read through a given MARCXML file and copy, inserting $0 as appropriate
	'''
	mrx_subs = []
	workid = ''
	num = ''
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
				#=======================
				# NAMES
				#=======================
				# get names data from these subfields
				namesubf = ['a','c','d','q']
				names = ['100','110','130','700','710','730']
				for n in rec.get_fields(*names):
					for s in n.get_subfields(*namesubf):
						s = s.encode('utf8')
						mrx_subs.append(s)
					h = "--".join(mrx_subs)
					h.rstrip('\.')
					tag = n.tag # trash?
					uri = query_4s(h,'nam')
					if uri is not None and uri != '':
						pymarc.Field.add_subfield(n,"0",uri)
					if verbose:
						print('%s, %s, %s' % (bbid, h.decode('utf8'), uri))
					if csvout or nomarc:
						write_csv(bbid, h, uri, 'nam')
					mrx_subs = []
			if subjects:
				#=======================
				# SUBJECTS
				#=======================
				# get subjects data from these subfields (all but 0,2,3,6,8)
				subs = ['600','610','611','630','650','651']
				subsubf = ['a', 'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 
				'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'x', 'y', 'z', '4'] 
				for f in rec.get_fields(*subs):
					for s in f.get_subfields(*subsubf):
						s = s.encode('utf8')
						mrx_subs.append(s)
					h = "--".join(mrx_subs)
					h = h.rstrip('\.')
					tag = f.tag # trash? for reporting?
					uri = query_4s(h,'sub')
					if uri is not None and uri != '':
						pymarc.Field.add_subfield(f,"0",uri)
					#fast = get_fast(ocn, h)
					#if fast is not None and fast != '':
					#	pymarc.Field.add_subfield(f,"0",fast)
					if verbose:
						print('%s, %s, %s' % (bbid, h.decode('utf8'), uri))
					if csvout or nomarc:
						write_csv(bbid, h, uri, 'sub')
					mrx_subs = []
			#if owis:
				##=======================
				## OWIS
				##=======================
				#for n in rec.get_fields('035'):
					#for s in n.get_subfields('a'):
						#if 'OCoLC' in s:
							#num = s.replace('(OCoLC)','')
							#workid = check_owi_cache(str(num))
							
				#if workid is not None and workid != '':
					#field = pymarc.Field(
					#tag = '787', 
					#indicators = ['0',' '],
					#subfields = [
					#'o', str(workid)
					#])
					#rec.add_field(field)
				#else:
					#workid = str(workid)
					
				#if verbose:
					#print('%s, %s, %s' % (bbid, num, workid))
					
				#if csvout:
					#write_csv(bbid, num, workid, 'owi')
					
			if nomarc == False:
				out = "%s" % (pymarc.record_to_xml(rec))
				fh.write(out)
		if nomarc == False:
			fh.write("</collection>")
			fh.close()
	except:
		etype,evalue,etraceback = sys.exc_info()
		print("problem: %s" % evalue)

	if not nomarc:
		try:
			subprocess.Popen(['xmllint','--format','-o', OUTDIR+outfile, TMPDIR+outfile+'_tmp.xml']).wait()
		except:
			etype,evalue,etraceback = sys.exc_info()
			print("xmllint problem: %s" % evalue)
				
	if justfetch is None:
		os.remove(INDIR+mrcrec)


def write_csv(bbid, heading, uri, scheme):
	'''
	Write out csv reports
	'''
	with open(REPORTS+fname+'_'+scheme+'_'+today+'.csv','ab+') as outfile:
		writer = csv.writer(outfile)
		row = (bbid, heading, uri)
		writer.writerow(row)

	
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Generate hold reports.')
	parser.add_argument("-v", "--verbose",required=False, default=False, dest="verbose", action="store_true", help="Runtime feedback.")
	#parser.add_argument("-p", "--prettify",required=False, default=True, dest="prettify", action="store_true", help="Format resulting marcxml. Doesn't matter if using -N (no MARCXML output).")
	parser.add_argument("-n", "--names", required=False, default=False, dest="names", action="store_true", help="Get URIs for names.")
	parser.add_argument("-s", "--subjects", required=False, default=False, dest="subjects", action="store_true", help="Get URIs for subjects.")
	parser.add_argument("-r", "--report", required=False, default=False, dest="csvout", action="store_true", help="Output csv reports as well as MARCXML records.")
	parser.add_argument("-R", "--Report", required=False, default=False, dest="nomarc", action="store_true", help="Output csv reports but do NOT output MARCXML records.")
	parser.add_argument("-f", "--fetch",type=str, required=False,dest="justfetch", help="Just fetch records listed in the given file. They will go into IN dir (and stay there). To enhance them, run again WITHOUT -f or -F flags.")
	parser.add_argument("-F", "--Fetch",type=str, required=False,dest="fetchngo", help="Fetch records listed in the given file and then enhance them 'on the fly'. Records are not left on disk.")
	#parser.add_argument("-o", "--owi", required=False, default=False, dest="owis", action="store_true", help="Get OCLC Work Ids.")

	args = vars(parser.parse_args())
	verbose = args['verbose']
	#prettify = args['prettify']
	names = args['names']
	subjects = args['subjects']
	nomarc = args['nomarc']
	csvout = args['csvout']
	justfetch = args['justfetch']
	fetchngo = args['fetchngo']
	#owis = args['owis']
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
