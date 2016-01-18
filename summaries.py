#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Get totals for a given run (identified using w3cdtf, yyyymmdd), and write out reports:
enhanced, nonenhanced, TOTALS, subfield0.html
Example (test run):

`python summaries.py -r 20160114 -d test`

Example (production run):

`python summaries.py -r 20160114`

from 20160113
pmg
"""
import argparse
import ConfigParser
import csv
import glob
import os
import re
import sys
import time

config = ConfigParser.RawConfigParser()
config.read('./config/uris.cfg')
REPORTDIR = config.get('env', 'reports')
today = time.strftime('%Y%m%d')

def main(report):
	'''
	Loop through reports dir and identify reports for given run
	'''
	run = ''
	r = re.search('\d{8}',report) # assumes filename pattern _schema_yyyymmdd.csv
	if r:
		run = r.group(0)
	filepath = REPORTS
	all_totals = []
	for filename in os.listdir(filepath):
		if re.match('^_(nam|sub)_'+run+'.csv$',filename):
			all_totals.append(get_totals(filename))
	totes = total_enhanced(report)
	for t in all_totals:
		write_totals(t[0],t[1],t[2],t[3],t[4],t[5],t[6],t[7],db,totes)

		
def get_totals(csvreport):
	'''
	Get totals from given run's reports
	'''
	report = csvreport.replace('.csv','')

	with open(REPORTS+report+'.csv','rb') as infile, open(REPORTS+report+'_enhanced.csv','wb+') as enhanced_file, open(REPORTS+report+'_nonenhanced.csv','wb+') as nonenhanced_file:
		bibdict = dict()
		bibset = set()
		infile.readline()
		reader = csv.reader(infile) # skip the first row
		total = 0
		found = 0
		notfound = 0
		noheading = 0
		for row in reader:
			bib = row[0]
			uri = row[2]
			if total == 0:
				startbib = bib
			if uri.startswith('http'):
				enhanced = 'y'
				found += 1
			elif uri != '' and not uri.startswith('http'):
				enhanced = 'n'
				notfound += 1 # heading was tested and not found
			else:
				enhanced = 'n'
				noheading += 1 # there was no heading -- TODO: something's off here?
			if bib in bibdict:
				bibdict[bib].append(enhanced)
			else:
				bibdict[bib] = [enhanced]
			if bib != '':
				total += 1
		lastbib = bib
		enhanced = 0		
		for k,v in bibdict.iteritems():
			if 'y' in v:
				writer = csv.writer(enhanced_file)
				writer.writerow([k])
				enhanced += 1
			else:
				writer = csv.writer(nonenhanced_file)
				writer.writerow([k])

	return [report,len(bibdict),startbib,lastbib,enhanced,int(total)-int(noheading),found,notfound]


def write_totals(report,bibdict,startbib,lastbib,enhanced,headings_total,found,notfound,db,totes):
	'''
	Write new lines in TOTALS.csv
	'''
	with open(REPORTDIR+'TOTALS.csv','ab+') as totals_file:
		reader = csv.reader(totals_file)
		if (os.path.getsize(REPORTDIR+'TOTALS.csv') == 0):
			writer = csv.writer(totals_file)
			writer.writerow(['run','records','first_bib','last_bib','records_enhanced','headings_total','headings_found','headings_not_found','total_loaded','db'])
		writer = csv.writer(totals_file)
		writer.writerow([report,bibdict,startbib,lastbib,enhanced,headings_total,found,notfound,totes,db])	


def total_enhanced(report):
	'''
	Get total of enhanced files for a given run (nam + sub)
	'''
	filepath = REPORTS
	all_enhanced = []
	for filename in os.listdir(filepath):
		if re.match('.*'+report+'_enhanced.csv',filename):
			with open(os.path.join(filepath,filename),'r') as f:
				for line in f:
					all_enhanced.append(line)
	return len(set(all_enhanced))


def make_html():
	"""
	Generate a simple html page
	"""
	htmlfile = open('./html/subfield0.html','wb+')
	sch = ''
	header = """<!doctype html>
<html>
<meta charset="utf-8">
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css">
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>
<style>
	div {
	font-family:Consolas,monaco,monospace;
	}
</style>
<script src="http://www.d3plus.org/js/d3.js"></script>
<script src="http://www.d3plus.org/js/d3plus.js"></script>
<div class="container" id="top">
<h1>$0 loads</h1>
<table class="table-condensed table-bordered">
<tr><th>run</th><th>scheme</th><th>records</th><th>first_bibid</th><th>last_bibid</th><th>records_enhanced</th><th>headings_total<!--<a href="#headings">?</a>--></th><th>headings_found</th><th>headings_not_found</th><th>total_enhanced</th><th>vger_db</th></tr>"""

	footer = """</table>
<br /><hr /><div id="headings" style="color:#999;">
	<!--<a href="#top">top</a>-->
	<h4>fields checked</h4>
	<p>names ('_nam_')</p>
	<dl class="dl-horizontal">
		<dt>tags</dt>
		<dd>100,110,130,700,710,730</dd>
		<dt>subfields</dt>
		<dd>a,b,c,d,q</dd>
	</dl>
	<p>subjects ('_sub_')</p>
	<dl class="dl-horizontal">
		<dt>tags</dt>
		<dd>600,610,611,630,650,651</dd>
		<dt>subfields</dt>
		<dd>a,b,c,d,f,g,h,j,k,l,m,n,o,p,q,r,s,t,u,v,x,y,z,4</dd>
	</dl>
</div>
</div>
</html>
"""

	htmlfile.write(header)
	with open(REPORTDIR+'TOTALS.csv','ab+') as totals:
		totals.readline()
		reader = csv.reader(totals)
		for row in reader:
			r = re.search('\d{8}',row[0]) # assumes filename pattern _schema_yyyymmdd.csv
			s = re.search('^_(\D{3})_',row[0])
			if r:
				run = r.group(0)
			if s:
				sch = s.group(1)
			htmlfile.write('<tr><td><a href="subfield0/reports/%s" target="_BLANK">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n' % (run,run,sch,row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
	htmlfile.write(footer)
	print('wrote html')


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Generate report of totals.')
	parser.add_argument("-r", "--run", type=str, required=True,dest="report", help="Run to get totals for. Reports assumed to be in ./reports. Assumed to be one run per day, so just enter yyyymmdd, e.g. 20160114")
	parser.add_argument("-d", "--db", type=str, required=False,dest="db", help="Specify whether it was a Test run.")

	args = vars(parser.parse_args())
	report = args['report']
	db = args['db']
	REPORTS = REPORTDIR + report + '/'
	
	if not db:
		db = 'prod'
	
	if not re.search('\d{8}',report):
		msg = 'Enter date of the run in w3cdtf yyyymmdd, e.g. \'python summaries.py -r %s\'' % today
		sys.exit(msg) 
	
	try:
		main(report)
		make_html()
		print('done!')
	except:
		etype,evalue,etraceback = sys.exc_info()
		print('problem-o %s %s' % (str(etype),str(evalue)))

