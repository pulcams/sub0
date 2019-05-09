#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Gets list of bib ids and (optionally) the marcxml as input for uris.py.
A simple sqlite db keeps track of (unsuppressed) bib ids retrieved to date; each run begins where the last left off.
List goes into directory specified in the BIBS variable (e.g. ./csv).
The marcxml goes into INDIR as a single file.
A 'bibs_' report goes into ./reports/yyyymmdd by default.
Cache schema:
`CREATE TABLE bibs(bbid TEXT, date DATE, PRIMARY KEY (bbid));`
from 20151207
pmg
"""
import argparse
import ConfigParser
import csv
import cx_Oracle
import httplib
import logging
import os
import sqlite3 as lite
import subprocess
import sys
import time
from lxml import etree

# config
config = ConfigParser.RawConfigParser()
config.read('./config/uris.cfg')
BIBS = config.get('env','bibs')
DB = config.get('db', 'bib_cache')
HOST = config.get('vger', 'ip')
INDIR = config.get('env', 'indir')
LOG = config.get('env', 'logdir')
USER = config.get('vger', 'user')
PASS = config.get('vger', 'pw')
PORT = config.get('vger', 'port')
REPORTDIR = config.get('env', 'reports')
SID = config.get('vger', 'sid')
TODAY = time.strftime('%Y%m%d') # for csv filename
VOYAGER_HELPER = config.get('env','voyager_helper')

REPORTS = REPORTDIR + TODAY # reports go into subdirectories named yyyymmdd

# logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=LOG+'bibs_'+TODAY+'.log',level=logging.INFO)


def get_last_bib():
	'''
	Retrieve the last bib id in the cache (going in numerical order).
	'''
	last_bib = 1 # default
	con = lite.connect(DB)
	with con:
		con.row_factory = lite.Row
		cur = con.cursor()
		cur.execute("SELECT bbid FROM bibs ORDER BY cast(bbid as int) DESC LIMIT 1;")
		rows = cur.fetchall()
		for row in rows:
			last_bib = str(row[0])
	msg = 'bib_id %s, %s bibs' % (last_bib,bibstofetch)
	logging.info(msg)
	get_bib_total()
	ask_the_oracle(last_bib)


def get_bib_total():
	'''
	Get total number of unsuppressed bibs for reporting
	'''
	con = lite.connect(DB)
	
	q = """SELECT COUNT(DISTINCT(BIB_ID)) 
			FROM BIB_MASTER 
			WHERE SUPPRESS_IN_OPAC = 'N'"""

	dsn = cx_Oracle.makedsn(HOST,PORT,SID)
	oradb = cx_Oracle.connect(USER,PASS,dsn)
		
	rows = oradb.cursor()
	rows.execute(q)
	r = rows.fetchall()
	
	rows.close()
	oradb.close()

	with open(BIBS+TODAY+'.csv','wb+') as outfile:
		writer = csv.writer(outfile)
		for row in list(r):
			bib = str(row[0])
			writer.writerow(['BIB_ID',bib]) # stick the total no of unsuppressed bibs next to col header
	print('wrote bibs total to ' + BIBS + picklist + '.csv')
	
 
def ask_the_oracle(last_bib):
	'''
	Starting from the last bib retrieved, query VGER for the next n bibs (n specified in bibstofetch variable). 
	'''	
	q = """SELECT b.BIB_ID
	FROM BIB_MASTER b
	WHERE
	b.SUPPRESS_IN_OPAC <> 'Y'
	AND b.BIB_ID > %s
	AND ROWNUM <= %s
	ORDER BY BIB_ID""" % (last_bib, bibstofetch)

	dsn = cx_Oracle.makedsn(HOST,PORT,SID)
	oradb = cx_Oracle.connect(USER,PASS,dsn)
		
	rows = oradb.cursor()
	rows.execute(q)
	r = rows.fetchall()
	
	rows.close()
	oradb.close()

	with open(BIBS+TODAY+'.csv','ab') as outfile:
		writer = csv.writer(outfile)
		#header = ['BIB_ID']
		#writer.writerow(header) 
		for row in list(r):
			bib = str(row[0])
			writer.writerow([bib])
	print('wrote list of bibs to ' + BIBS + picklist + '.csv')


def get_bibdata_rb():
	'''
	Query the PUL bibdata service using voyager_helpers gem (maybe better for large batches)
	'''
	try:
		subprocess.Popen(['ruby',VOYAGER_HELPER,picklist]).wait()
		logging.info('got bibdata using get_bibdata.rb')
	except:
		etype,evalue,etraceback = sys.exc_info()
		print("get_bibdata_rb problem: %s" % evalue)


def get_bibdata():
	'''
	Query the PUL bibdata service. Takes the csv output of ask_the_oracle() as input.  
	'''
	conn = httplib.HTTPSConnection("bibdata.princeton.edu")
	flag = ""
	NS = "{http://www.loc.gov/MARC21/slim}"
			
	with open(BIBS + picklist+'.csv','rb') as csvfile:
		reader = csv.reader(csvfile,delimiter=',', quotechar='"')

		firstline = reader.next() # skip the header row
		
		count = 0

		f = open(INDIR+TODAY+'.mrx', 'w+')
		f.writelines("<collection>")
		
		for row in reader:
		
			count += 1 # just for verbose mode
			
			bibid = row[0]

			conn.request("GET", "/bibliographic/"+bibid)
			got = conn.getresponse()
			data = got.read()
			conn.close() 

			if justfetch: # for the case of named files rather than dates e.g. 'derrida'
				REPORTS = REPORTDIR + justfetch
			f = open(INDIR+TODAY+'.mrx', 'a')
			f2 = open(REPORTS+'bibs.txt', 'a') # simple log
	
			try:
				doc = etree.fromstring(data)
				data = etree.tostring(doc,pretty_print=False,encoding='utf-8')
				f001 = doc.find("marc:record/marc:controlfield[@tag=\'001\']",namespaces={'marc':'http://www.loc.gov/MARC21/slim'})
				f.writelines(data)
				flag = "ok"
				f2.write("%s\n" % bibid)
			except: # As when record has been suppressed after initial report was run, in which case, no xml
				etype,evalue,etraceback = sys.exc_info()
				flag = "problem: %s %s %s, line %s" % (etype,evalue,etraceback,etraceback.tb_lineno)
				f2.write("%s %s\n" % (bibid, flag))
		
			f2.close()

			# add to bibs.db with the date/time that record was extracted (check later to avoid overwriting)
			cache_bib(bibid) 
			
			if verbose:
				print("(%s) Got %s" % (count, bibid))
	
		f.writelines("</collection>")
		f.close()

		logging.info('got bibdata')
		

def cache_bib(bib):
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
		con.close()


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Get list of BIB_IDs from Voyager to serve as input for uris.py')
	bib_types = ['vger','oclc']
	parser.add_argument("-b", "--bibs",required=False, default=10000, dest="bibstofetch", help="Number of bibs to fetch")
	parser.add_argument("-v", "--verbose", required=False, default=False, dest="verbose", action="store_true", help="Runtime feedback.")
	parser.add_argument("-p", "--pyget", required=False, default=False, dest="pyget", action="store_true", help="Get records with Python function using httplib (rather than the default voyager_helpers.rb).")
	parser.add_argument("-F", "--justfetch", required=False, type=str, dest="justfetch", help="Fetch records listed in the given file. Skips getting new set of bib ids incrementing from the previous set. Use this if you're re-loading a set of bibs.")
	parser.add_argument("-R", "--Report", required=False, default=False, dest="nomarc", action="store_true", help="Output csv reports but do NOT output MARCXML records.")
	args = vars(parser.parse_args())
	bibstofetch = args['bibstofetch']
	justfetch = args['justfetch']
	nomarc = args['nomarc']
	pyget = args['pyget']
	verbose = args['verbose']

	if justfetch:
		picklist = os.path.basename(justfetch).replace('.csv','')
	else:
		picklist = TODAY

	logging.info('='*25)
	if not justfetch:
		get_last_bib()
	if not nomarc:
		if pyget:
			get_bibdata()
		else:
			get_bibdata_rb()
	logging.info('='*25)

