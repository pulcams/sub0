#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Cheaply re-check a list of bibs against vger before reloading to be sure they've not been changed.
This is useful when records are reloaded some time after the bibs have been extracted / processed.
From 20160414
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

#=============================================================
TODAYDB = time.strftime('%Y-%m-%d') # to check against cache
#=============================================================

def get_retrieved_date():
	retrieved = ''
	con = lite.connect(DB)
	changed_bibs = []
	changed_file = BIBS+'changed_'+TODAY+'.csv'
	with con:
		con.row_factory = lite.Row
		cur = con.cursor()
		q = "SELECT bbid, date FROM bibs where date like '%s%%' ORDER BY cast(bbid as int);" % TODAYDB
		cur.execute(q)
		rows = cur.fetchall()
		for row in rows:
			bib_id = str(row[0])
			retrieved = str(row[1])
			date = ask_the_oracle(bib_id, retrieved)
			if date:
				print('%s changed ===================' % bib_id)
				changed_bibs.append(bib_id)
				with open(changed_file,'ab+') as outfile:
					writer = csv.writer(outfile)
					writer.writerow([bib_id,retrieved,date])
			else:
				print('%s ok' % bib_id)
	if len(changed_bibs) > 0:
		print('Some bib records had been changed. Check %s before reloading.' % changed_file)
	else:
		print('Good to go.')


def ask_the_oracle(bib_id,retrieved):
	'''
	Starting from the last bib retrieved, query VGER for the next n bibs (n specified in bibstofetch variable). 
	'''
	bib = ''
	con = lite.connect(DB)
	
	q = """SELECT DISTINCT BIB_MASTER.BIB_ID,
	TO_CHAR(ACTION_DATE,'YYYY-MM-DD HH24:MI:SS') as ACTION_DATE
	FROM BIB_MASTER LEFT JOIN BIB_HISTORY ON BIB_MASTER.BIB_ID = BIB_HISTORY.BIB_ID
	LEFT JOIN BIB_TEXT ON BIB_MASTER.BIB_ID = BIB_TEXT.BIB_ID
	WHERE
	(BIB_MASTER.BIB_ID = %s
	AND
	(TO_CHAR(ACTION_DATE,'YYYY-MM-DD HH24:MI:SS') > '%s'))""" % (bib_id,retrieved)

	dsn = cx_Oracle.makedsn(HOST,PORT,SID)
	oradb = cx_Oracle.connect(USER,PASS,dsn)
		
	rows = oradb.cursor()
	rows.execute(q)
	r = rows.fetchall()
	
	rows.close()
	oradb.close()

	for row in list(r):
		date = str(row[1])
		return date # => only returns dates of changed files


if __name__ == "__main__":
	get_retrieved_date()

