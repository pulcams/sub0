#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Get totals for a given run (identified using w3cdtf, yyyymmdd), and write out reports:
enhanced, nonenhanced, TOTALS, index.html
Example (a test run):

`python summaries.py -r 20160401 -d test`

Example (a production run; 'prod' is default):

`python summaries.py -r 20160401`

from 20160113
pmg
"""
import argparse
import ConfigParser
import csv
import fileinput
import glob
import os
import re
import sys
import time
from collections import defaultdict

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

	with open(REPORTS+report+'.csv','rb') as infile, open(REPORTS+report+'_enhanced.csv','wb+') as enhanced_file, open(REPORTS+report+'_nonenhanced.csv','wb+') as nonenhanced_file, open(REPORTS+'bibs.txt') as outbibs_file:
		bibdict = dict()
		bibset = set()
		infile.readline()
		reader = csv.reader(infile) # skip the first row
		total = 0
		found = 0
		notfound = 0
		noheading = 0

		# get the total bibs downloaded (as opposed to totals per scheme)
		out_reader = csv.reader(outbibs_file)
		firstbib = next(out_reader)[0] # first bib downloaded
		bibs_downloaded = 1
		for bib in out_reader:
			bibs_downloaded += 1
			#print(bibs_downloaded)
			pass
		finalbib = bib[0] # last bib downloaded
		

		for row in reader:
			bib = row[0]
			uri = row[2]
			#if total == 0:
			#	startbib = bib # first bib per scheme report
			if uri.startswith('http'):
				enhanced = 'y'
				found += 1
			elif uri != '' and not uri.startswith('http'):
				enhanced = 'n'
				notfound += 1 # heading was tested and not found
			else:
				enhanced = 'n'
				noheading += 1 # there was no heading -- TODO: what's up here?
			if bib in bibdict:
				bibdict[bib].append(enhanced)
			else:
				bibdict[bib] = [enhanced]
			if bib != '':
				total += 1
		#lastbib = bib # the last bib per scheme report (e.g. last bib in nam report, which can differ from last bib in sub report)
		enhanced = 0		
		for k,v in bibdict.iteritems():
			if 'y' in v:
				writer = csv.writer(enhanced_file)
				writer.writerow([k])
				enhanced += 1
			else:
				writer = csv.writer(nonenhanced_file)
				writer.writerow([k])

	return [report,bibs_downloaded,firstbib,finalbib,enhanced,int(total)-int(noheading),found,notfound]


def write_totals(report,bibdict,startbib,finalbib,enhanced,headings_total,found,notfound,db,totes):
	'''
	Write new lines in TOTALS.csv
	'''
	tempfile = REPORTDIR+'TOTALS.tmp'
	totalsfile = REPORTDIR+'TOTALS.csv'
	seen = set()
	newrow = [report,str(bibdict),startbib,finalbib,str(enhanced),str(headings_total),str(found),str(notfound),str(totes),db]

	with open(totalsfile,'ab+') as totals_file:
		writer = csv.writer(totals_file)
		if (os.path.getsize(totalsfile) == 0): # create new TOTALS.csv if doesn't exist
			writer.writerow(['run','records','first_bib','last_bib','records_enhanced','headings_total','headings_found','headings_not_found','total_loaded','db'])
	
	with open(totalsfile,'rb') as existing_totals_file,open(tempfile,'wb+') as temp_totals_file:
		reader = csv.reader(existing_totals_file)
		writer = csv.writer(temp_totals_file)
		for row in reader:
			thisrun = row[0]
			seen.add(thisrun)
						
		if newrow[0] not in seen: # write unique new rows to temp file
			writer.writerow(newrow)

	# read from temp file (unique rows) and append to TOTALS
	with open(tempfile,'rb') as temp_totals_file, open(totalsfile,'ab') as new_totals_file:
		reader = csv.reader(temp_totals_file)
		writer = csv.writer(new_totals_file)
		for row in reader:
			writer.writerow(newrow)
		
	os.remove(tempfile) # remove temp file

		
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
	htmlfile = open('./html/index.html','wb+')
	sch = ''
	total_prod = 0
	total_checked = 0
	enhanced_total = 0
	headings_total = 0
	total_headings_found = 0
	total_headings_not_found = 0
	s_enhanced_total = 0
	s_headings_total = 0
	s_total_headings_found = 0
	s_total_headings_not_found = 0
	header = """<!doctype html>
<html>
<meta charset="utf-8">
<title>loads</title>
<script src="https://code.jquery.com/jquery-2.2.2.min.js"
			  integrity="sha256-36cp2Co+/62rEAAYHLmRCPIych47CvdM+uTBJwSzWjI="
			  crossorigin="anonymous"></script>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css">
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>
<style>
	div {
	font-family:Consolas,monaco,monospace;
	}
	.hi {
	background-color:yellow;
	}
	.table-condensed > tbody > tr > td, .table-condensed > tbody > tr > th, .table-condensed > tfoot > tr > td, .table-condensed > tfoot > tr > th, .table-condensed > thead > tr > td, .table-condensed > thead > tr > th {
	padding: 5px;
}
</style>
<div class="container" id="top">
<h1>$0 loads</h1>
<a href="about.html">about</a>"""

	start_main_table="""<table class="table-condensed table-bordered">
<tr  bgcolor="#F0F8FF"><th>run</th><th>records</th><th>first_bibid</th><th>last_bibid</th><th>recs_enhanced</th><th>vger_db</th><th width="400px">breakdown</th></tr>
<tr bgcolor="#F0F8FF"><td colspan="6"></td><td><table class="table-condensed" width="100%%" style="font-size:.75em;"><tr bgcolor="#F0F8FF"><td width="20%%">scheme</td><td width="20%%">enhanced</td><td width="20%%">headings</td><td width="20%%">found</td><td width="20%%">not_found</td></tr></table></td></tr>
"""

	end_main_table = """</table>
</div>
<br />
</html>
"""
	dct = defaultdict(list)
	sch_dct = defaultdict(list)
	with open(REPORTDIR+'TOTALS.csv','ab+') as totals:
		totals.readline()
		reader = csv.reader(totals)
		for row in reader:
			r = re.search('\d{8}',row[0]) # assumes filename pattern _schema_yyyymmdd.csv
			if r:
				run = r.group(0)
	
			# add to the dictionary, with run as the key
			dct[run].append(row[0:])

		htmlfile.write(header)
		prod_count = 0
		for k,v in sorted(dct.items()):
			checked = int(v[0][1])
			enhanced = int(v[0][8])
			db = v[0][9]
			# get total records loaded into prod
			if db == 'prod':
				if prod_count == 0:
					total_prod = enhanced
					total_checked = checked
				else:
					total_prod += enhanced
					total_checked += checked
				
				for scheme in sorted(v):
					s = re.search('^_(\D{3})_',scheme[0])
					if s:
						sch = s.group(1)
					
					if sch == 'nam':
						if prod_count == 0:
							enhanced_total = int(scheme[4])
							headings_total = int(scheme[5])
							total_headings_found = int(scheme[6])
							total_headings_not_found = int(scheme[7])
						else:
							enhanced_total += int(scheme[4])
							headings_total += int(scheme[5])
							total_headings_found += int(scheme[6])
							total_headings_not_found += int(scheme[7])
					elif sch == 'sub':
						if prod_count == 0:
							s_enhanced_total = int(scheme[4])
							s_headings_total = int(scheme[5])
							s_total_headings_found = int(scheme[6])
							s_total_headings_not_found = int(scheme[7])
						else:
							s_enhanced_total += int(scheme[4])
							s_headings_total += int(scheme[5])
							s_total_headings_found += int(scheme[6])
							s_total_headings_not_found += int(scheme[7])

			prod_count += 1

		htmlfile.write('<h3>totals</h3>')
		htmlfile.write('<table class="table-condensed table-bordered">')
		htmlfile.write('<tr><td>records processed</td><td>%s</td></tr>' % total_checked)
		htmlfile.write('<tr><td>records enhanced</td><td>%s</td></tr>' % total_prod)
		htmlfile.write('</table><br />')

		htmlfile.write('<table class="table-condensed table-bordered">')
		htmlfile.write('<tr><td width="20%%">scheme</td><td width="20%%">enhanced</td><td width="20%%">headings</td><td width="20%%">found</td><td width="20%%">not_found</td></tr>')
		htmlfile.write('<tr><td>names</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>' % (enhanced_total,headings_total,total_headings_found,total_headings_not_found))
		htmlfile.write('<tr><td>subjects</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>' % (s_enhanced_total,s_headings_total,s_total_headings_found,s_total_headings_not_found))
		htmlfile.write('</table>')

		htmlfile.write('<h3>details</h3>')
		htmlfile.write(start_main_table)
		
		for k,v in sorted(dct.items()):
			run = k
			bibs = v[0][1]
			first = v[0][2]
			last = v[0][3]
			enhanced = v[0][8]
			db = v[0][9]

			htmlfile.write('<tr><td><a href="reports/%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>' % (run,run,bibs,first,last, enhanced,db))
			innertablehead = '''<table class="table-condensed table-hover" width="100%%" style="font-size:.75em;">'''
			htmlfile.write(innertablehead)
			for scheme in sorted(v):
				s = re.search('^_(\D{3})_',scheme[0])
				if s:
					sch = s.group(1)
				records_enhanced = scheme[4]
				headings_total = scheme[5]
				headings_found = scheme[6]
				headings_not_found = scheme[7]

				htmlfile.write('<tr><td width="20%%">%s</td><td width="20%%">%s</td><td width="20%%">%s</td><td width="20%%">%s</td><td width="20%%">%s</td></tr>' % (sch,records_enhanced,headings_total,headings_found, headings_not_found))
				
			htmlfile.write('</table>')
			htmlfile.write('</td></tr>')

	htmlfile.write(end_main_table)
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

