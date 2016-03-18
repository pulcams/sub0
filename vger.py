#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Gets lists of bib ids for uri enhancement.
Resulting csv lists go into directory specified in the BIBS variable (e.g. ./csv).
Resulting 'bibs_' report goes into ./reports/yyyymmdd by default.
A simple sqlite db keeps track of (unsuppressed) bib ids retrieved to date; each run begins where the last left off.
Cache schema:
`CREATE TABLE bibs(bbid TEXT, date DATE, PRIMARY KEY (bbid));`
from 20151207
pmg
"""
import argparse
import ConfigParser
import csv
import cx_Oracle
import logging
import sqlite3 as lite
import time

# config
config = ConfigParser.RawConfigParser()
config.read('./config/uris.cfg')
BIBS = config.get('env','bibs')
LOG = config.get('env', 'logdir')
USER = config.get('vger', 'user')
PASS = config.get('vger', 'pw')
PORT = config.get('vger', 'port')
SID = config.get('vger', 'sid')
HOST = config.get('vger', 'ip')
DB = config.get('db', 'bib_cache')
TODAY = time.strftime('%Y%m%d') # for csv filename

# logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=LOG+'bibs_'+TODAY+'.log',level=logging.INFO)

def get_last_bib():
	'''
	Retrieve the last bib id in the cache (going in numerical order).
	'''
	last_bib = 1 # default
	con = lite.connect(DB) # schema: CREATE TABLE bibs(bbid TEXT, date DATE, PRIMARY KEY (bbid));
	with con:
		con.row_factory = lite.Row
		cur = con.cursor()
		cur.execute("SELECT bbid FROM bibs ORDER BY cast(bbid as int) DESC LIMIT 1;")
		#cur.execute("SELECT * FROM bibs WHERE bbid = (SELECT MAX(cast(bbid as int)) FROM bibs);") # this works too
		rows = cur.fetchall()
		for row in rows:
			last_bib = str(row[0])
	msg = 'bib_id %s, %s bibs' % (last_bib,tofetch)
	logging.info(msg)
	ask_the_oracle(last_bib)

 
def ask_the_oracle(last_bib):
	'''
	Starting from the last bib retrieved, query VGER for the next n bibs (n specified in bibstofetch variable). 
	'''
	con = lite.connect(DB)
	
	q = """SELECT b.BIB_ID
	FROM BIB_TEXT b
	LEFT JOIN BIB_MASTER bmr ON b.BIB_ID = bmr.BIB_ID
	WHERE
	bmr.SUPPRESS_IN_OPAC <> 'Y'
	AND b.BIB_ID > %s
	AND ROWNUM <= %s
	ORDER BY BIB_ID""" % (last_bib, tofetch)

	dsn = cx_Oracle.makedsn(HOST,PORT,SID)
	oradb = cx_Oracle.connect(USER,PASS,dsn)
		
	rows = oradb.cursor()
	rows.execute(q)
	r = rows.fetchall()
	
	rows.close()
	oradb.close()

	#thisfile = time.strftime("%Y%m%d%I%M%S") # including h, m, s was to avoid overwriting files written on same day
	with open(BIBS+TODAY+'.csv','wb+') as outfile:
		writer = csv.writer(outfile)
		header = ['BIB_ID']
		writer.writerow(header) 
		for row in list(r):
			bib = str(row[0]) # to put in log files below
			print(bib)
			writer.writerow([bib])

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

	print('wrote to ' + BIBS + TODAY + '.csv')


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Get list of BIB_IDs from Voyager to serve as input for uris.py')
	parser.add_argument("-b", "--bibs",required=False, default=10000, dest="bibstofetch", help="Number of bibs to fetch")
	args = vars(parser.parse_args())
	tofetch = args['bibstofetch']
	
	logging.info('='*25)
	get_last_bib()
	logging.info('='*25)

