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
import cx_Oracle
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
BIBS = config.get('env','bibs')
today = time.strftime('%Y%m%d')

user = config.get('vger', 'user')
pw = config.get('vger', 'pw')
sid = config.get('vger', 'sid')
ip = config.get('vger', 'ip')
port = config.get('vger', 'port')
dsn_tns = cx_Oracle.makedsn(ip,port,sid)


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
		write_totals(t[0],t[1],t[2],t[3],t[4],t[5],t[6],t[7],db,totes,t[8])

		
def get_totals(csvreport):
	'''
	Get totals from given run's reports
	'''
	report = csvreport.replace('.csv','')
	bib_report = re.sub('^_\w{3}_','',report)

	with open(REPORTS+report+'.csv','rb') as infile, open(BIBS+bib_report+'.csv','rb') as bib_list, open(REPORTS+report+'_enhanced.csv','wb+') as enhanced_file, open(REPORTS+report+'_nonenhanced.csv','wb+') as nonenhanced_file, open(REPORTS+'bibs.txt') as outbibs_file:
		bibdict = dict()
		bibset = set()
		infile.readline()
		reader = csv.reader(infile) # skip the first row
		total = 0
		found = 0
		notfound = 0
		noheading = 0

		# get total bibs in Voyager
		bibs_reader = csv.reader(bib_list)
		total_vger_bibs = next(bibs_reader)[1]

		# get the total bibs extracted (as opposed to totals per scheme)
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
				noheading += 1 # there was no heading
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

	return [report,bibs_downloaded,firstbib,finalbib,enhanced,int(total)-int(noheading),found,notfound,total_vger_bibs]


def get_overall_totals(report):
	'''
	Get totals from *all* reports for analysis
	'''
	filepath = report
	all_totals = []

	with open('./reports/all_names_found.csv','wb+') as nfound, open('./reports/all_names_not_found.csv','wb+') as nmissing, open('./reports/all_subjects_found.csv','wb+') as sfound, open('./reports/all_subjects_not_found.csv','wb+') as smissing:
		header_row = ['bib','heading','uri']
		writer1 = csv.writer(nfound)
		writer2 = csv.writer(nmissing)
		writer3 = csv.writer(sfound)
		writer4 = csv.writer(smissing)
		writer1.writerow(header_row)
		writer2.writerow(header_row)
		writer3.writerow(header_row)
		writer4.writerow(header_row)
	
	for r,d,f in os.walk(filepath):
		for thisfile in f:
			if re.match('.*/\d{8}$',r): # paths with folders named as yyyymmdd
				nmatch = re.match('_nam_\d{8}\.csv$',thisfile)
				smatch = re.match('_sub_\d{8}\.csv$',thisfile)
				if nmatch or smatch: # reports with naming convention like '_nam_yyyymmdd.csv'
					thisreport = os.path.join(r,thisfile)
					reader = csv.reader(thisreport)
					if nmatch: # name report
						with open(thisreport,'r') as csvin, open('./reports/all_names_not_found.csv','ab') as nmissing, open('./reports/all_names_found.csv','ab') as nfound:
							thiscsv = csv.reader(csvin)
							for row in thiscsv:
								bib = row[0]
								heading = row[1]
								uri = row[2]
								if uri <> '' and not uri.startswith('http') and uri != 'lc_uri':
									writer = csv.writer(nmissing)
									writer.writerow([bib, heading, uri])
								elif uri <> '' and uri != 'lc_uri':
									writer = csv.writer(nfound)
									writer.writerow([bib, heading, uri])
					else: # subject report
						with open(thisreport,'r') as csvin, open('./reports/all_subjects_not_found.csv','ab') as smissing, open('./reports/all_subjects_found.csv','ab') as sfound:
							thiscsv = csv.reader(csvin)
							for row in thiscsv:
								bib = row[0]
								heading = row[1]
								uri = row[2]
								if uri <> '' and not uri.startswith('http') and uri != 'lc_uri':
									writer = csv.writer(smissing)
									writer.writerow([bib, heading, uri])
								elif uri <> '' and uri != 'lc_uri':
									writer = csv.writer(sfound)
									writer.writerow([bib, heading, uri])

	total_sub_not_found = sum(1 for line in open('./reports/all_subjects_not_found.csv')) - 1 
	total_sub_found = sum(1 for line in open('./reports/all_subjects_found.csv')) - 1 
	total_nam_not_found = sum(1 for line in open('./reports/all_names_not_found.csv')) - 1
	total_nam_found = sum(1 for line in open('./reports/all_names_found.csv')) - 1
	total_nam_headings = total_nam_not_found + total_nam_found
	total_sub_headings = total_sub_not_found + total_sub_found

	return [total_nam_headings,total_nam_found,total_nam_not_found,total_sub_headings,total_sub_found,total_sub_not_found]


def write_totals(report,bibdict,startbib,finalbib,enhanced,headings_total,found,notfound,db,totes,vger_bibs):
	'''
	Write new lines in TOTALS.csv
	'''
	tempfile = REPORTDIR+'TOTALS.tmp'
	totalsfile = REPORTDIR+'TOTALS.csv'
	seen = set()
	newrow = [report,str(bibdict),startbib,finalbib,str(enhanced),str(headings_total),str(found),str(notfound),str(totes),db,vger_bibs]

	with open(totalsfile,'ab+') as totals_file:
		writer = csv.writer(totals_file)
		if (os.path.getsize(totalsfile) == 0): # create new TOTALS.csv if doesn't exist
			writer.writerow(['run','records','first_bib','last_bib','records_enhanced','headings_total','headings_found','headings_not_found','total_loaded','db','vger_bibs'])
	
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


def get_sub0_count():
	total_sub0 = 0
	db = cx_Oracle.connect(user,pw,dsn_tns)
	c = db.cursor()
	sql = """SELECT COUNT(DISTINCT(BIB_ID)) 
				FROM BIB_DATA 
				WHERE 
				(RECORD_SEGMENT LIKE '%0(uri)%'
				OR
				RECORD_SEGMENT LIKE '%0http://%')"""
	c.execute(sql)
	for row in c:
		total_sub0 = row[0]

	return str(total_sub0)


def make_html():
	"""
	Generate a simple html pages: totals and loads
	"""
	totalsfile = open('./html/totals.html','wb+')
	loadsfile = open('./html/loads.html','wb+')
	overall_totals = get_overall_totals(REPORTDIR)
	total_sub0 = get_sub0_count()
	n_headings = overall_totals[0]
	n_found = overall_totals[1]
	n_notfound = overall_totals[2]
	s_headings = overall_totals[3]
	s_found = overall_totals[4]
	s_notfound = overall_totals[5]
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
	header = """<!DOCTYPE html>
<html>
<meta charset="utf-8">
<title>%s</title>
<script src="https://code.jquery.com/jquery-2.2.2.min.js"
			  integrity="sha256-36cp2Co+/62rEAAYHLmRCPIych47CvdM+uTBJwSzWjI="
			  crossorigin="anonymous"></script>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css">
<link rel="stylesheet" href="css/styles.css">
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>
<body>
<div class="container" id="top">
 <!-- Static navbar -->
      <nav class="navbar navbar-default navbar-fixed-top">
        <div class="container">
          <div class="navbar-header">
            <a class="navbar-brand" href="index.html">$0</a>
          </div>
          <div id="navbar" class="navbar-collapse collapse">
            <ul class="nav navbar-nav">
				<li%s><a href="totals.html">totals</a></li>
				<li%s><a href="loads.html">loads</a></li>
				<li><a href="about.html">details</a></li>
            </ul>
          </div><!--/.nav-collapse -->
        </div><!--/.container-fluid -->
      </nav>"""

	start_main_table="""<table class="table-condensed table-bordered table-hover">
<tr  bgcolor="#F0F8FF"><th>run</th><th>records</th><th>first_bibid</th><th>last_bibid</th><th>recs_enhanced</th><th>vger_db</th><!--<th width="400px">breakdown</th>--></tr>
<!--<tr bgcolor="#F0F8FF"><td colspan="6"></td><td>
<table class="table-condensed" width="100%%" style="font-size:.75em;"><tr bgcolor="#F0F8FF"><td width="20%%">scheme</td><td width="20%%">enhanced</td><td width="20%%">headings</td><td width="20%%">found</td><td width="20%%">not_found</td></tr></table></td></tr>-->
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
		
		prod_count = 0
		for k,v in sorted(dct.items()):
			checked = int(v[0][1])
			enhanced = int(v[0][8])
			db = v[0][9]
			vger_bibs = v[0][10]
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

		#=============
		# waffle.csv
		#=============
		waffle_total = int(vger_bibs) - int(total_checked)
		with open('./html/waffle.csv','wb+') as waffle_data:
			writer = csv.writer(waffle_data)
			writer.writerow(['group','number'])
			writer.writerow(['extracted',total_checked])
			writer.writerow(['remaining',waffle_total])

		#=============
		# totals.html
		#=============
		totalsfile.write(header % ('totals',' class="active"',''))
		vizdiv = '''<script src="http://d3js.org/d3.v3.min.js"></script>
		<p><sub>progress as of %s...</sub></p>
		<div id="waffle">
		</div>
		<hr />
		''' % (today)
		percent_vger_enhanced = round((float(total_sub0)/float(vger_bibs) * 100), 2)
		totalsfile.write(vizdiv)
		totalsfile.write('<table class="table-condensed">')
		totalsfile.write('<tr><td>bibs in voyager:</td><td>%s</td></tr>' % vger_bibs)
		totalsfile.write('<tr><td>records extracted:</td><td>%s</td></tr>' % total_checked)
		totalsfile.write('<tr><td>records enhanced:</td><td>%s</td></tr>' % total_prod)
		totalsfile.write('<tr><td style="font-size:.85 em;">total records enhanced*</td><td>%s (%s%%)</td></tr>' % (total_sub0,percent_vger_enhanced))
		totalsfile.write('<tr><td><span style="font-size:.75em;">*from all sources</span></td><td></td></tr>')
		totalsfile.write('</table>')
		totalsfile.write('<hr />')

		totalsfile.write('<table class="table-condensed table-bordered table-hover">')
		totalsfile.write('<tr><td></td><!--<td>enhanced</td>--><td>headings</td><td>not_found</td><td>found</td><td>%</td></tr>')
		totalsfile.write('<tr><td>names</td><!--<td></td>--><td>%s</td><td><a href="reports/all_names_not_found.csv">%s</a></td><td><a href="reports/all_names_found.csv">%s</a></td><td><div id="pie1"></div></td></tr>' % (n_headings,n_notfound,n_found))
		totalsfile.write('<tr><td>subjects</td><!--<td></td>--><td>%s</td><td><a href="reports/all_subjects_not_found.csv">%s</a></td><td><a href="reports/all_subjects_found.csv">%s</a></td><td><div id="pie2"></div></td></tr>' % (s_headings,s_notfound,s_found))
		totalsfile.write('</table>')
		totalsfile.write('<hr />')
		totalsfile.write('</div></body>')
		d3stuff = '''
		<script>
		var total = 0;
		var width,
		height,
		widthSquares = 25,
		heightSquares = 4,
		squareSize = 15,
		squareValue = 0,
		gap = 1,
		theData = [];  

		//var color = d3.scale.category10();
		var color = d3.scale.ordinal()
		    .range(["#d6616b", "#ccc"]);
		
		d3.csv("waffle.csv", function(error, data)
		{
		  //total
		  total = d3.sum(data, function(d) { return d.number; });
		
		  //value of a square
		  squareValue = total / (widthSquares*heightSquares);
		  
		  //remap data
		  data.forEach(function(d, i) 
		  {
		      d.number = +d.number;
		      d.units = Math.round(d.number/squareValue);
		      theData = theData.concat(
		        Array(d.units+1).join(1).split('').map(function()
		          {
		            return {  squareValue:squareValue,                      
		                      units: d.units,
		                      number: d.number,
		                      groupIndex: i};
		          })
		        );
		  });
		
		  width = (squareSize*widthSquares) + widthSquares*gap + 25;
		  height = (squareSize*heightSquares) + heightSquares*gap + 25;
		
		  var waffle = d3.select("#waffle")
		      .append("svg")
		      .attr("width", width)
		      .attr("height", height)
		      .append("g")
		      .selectAll("div")
		      .data(theData)
		      .enter()
		      .append("rect")
		      .attr("width", squareSize)
		      .attr("height", squareSize)
		      .attr("fill", function(d)
		      {
		        return color(d.groupIndex);
		      }
		      )
		      .attr("x", function(d, i)
		        {
		          //group n squares for column
		          col = Math.floor(i/heightSquares);
		          return (col*squareSize) + (col*gap);
		        })
		      .attr("y", function(d, i)
		      {
		        row = i%heightSquares;
		        return (heightSquares*squareSize) - ((row*squareSize) + (row*gap))
		      })
		      .append("title")
		        .text(function (d,i) 
		          {
		            return data[d.groupIndex].group + ": " +  d.number + " (~" + d.units + "%)"
		          });
		});
		
	</script>'''

		pies = '''<script>
		var w = 30,                        
	    h = 30,                            
	    r = 10
	
	
	    name_data = [{label:"found",percentage:%s,value:%s},
				{label:"not found",percentage:%s,value:%s}];
	
	    
	    subj_data = [{label:"found",percentage:%s,value:%s},
				{label:"not found",percentage:%s,value:%s}];
	    
			var vis = d3.select("#pie1")
	        .append("svg:svg")
	        .data([name_data])
	            .attr("width", w)
	            .attr("height", h)
	        .append("svg:g") 
	            .attr("transform", "translate(" + r + "," + r + ")")
	        
	
	       var vis2 = d3.select("#pie2")
	        .append("svg:svg")
	        .data([subj_data])
	            .attr("width", w)
	            .attr("height", h)
	        .append("svg:g") 
	            .attr("transform", "translate(" + r + "," + r + ")")
	
			var arc = d3.svg.arc()
	        .outerRadius(r);
	
			var pie = d3.layout.pie()
	        .value(function(d) { return d.value; });
	
			var arcs = vis.selectAll("g.slice")
	        .data(pie) 
	        .enter() 
	            .append("svg:g")
	                .attr("class", "slice");
	        arcs.append("svg:path")
	                .attr("fill", function(d, i) { return color(i); } )
	                .attr("d", arc);
	        arcs.append("svg:title") 
						.text(function(d) { return d.data.percentage + "%% " + d.data.label; });
	
			var arcs = vis2.selectAll("g.slice")
	        .data(pie) 
	        .enter() 
	            .append("svg:g")
	                .attr("class", "slice");
	        arcs.append("svg:path")
	                .attr("fill", function(d, i) { return color(i); } )
	                .attr("d", arc);
			arcs.append("svg:title") 
						.text(function(d) { return d.data.percentage + "%% " + d.data.label; });
	
	        </script>''' % ("{:.2}".format(float(n_found)/float(n_headings) * 100),n_found,"{:.2}".format(float(n_notfound)/float(n_headings) * 100),n_notfound,
	        "{:.2}".format(float(s_found)/float(s_headings) * 100),s_found,"{:.2}".format(float(s_notfound)/float(s_headings) * 100),s_notfound)

		totalsfile.write(d3stuff)
		totalsfile.write(pies)
		totalsfile.write('</html>')
		print('wrote totals.html')

		#=============
		# loads.html
		#=============
		loadsfile.write(header % ('loads','',' class="active"'))
		loadsfile.write(start_main_table)
		
		for k,v in sorted(dct.items(), reverse=True):
			run = k
			bibs = v[0][1]
			first = v[0][2]
			last = v[0][3]
			enhanced = v[0][8]
			db = v[0][9]

			loadsfile.write('<tr><td><!--<a href="reports/%s">-->%s<!--</a>--></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><!--<td>' % (run,run,bibs,first,last, enhanced,db))
			innertablehead = '''<table id="loads" class="table-condensed table-hover" style="font-size:.75em;">'''
			loadsfile.write(innertablehead)
			for scheme in sorted(v):
				s = re.search('^_(\D{3})_',scheme[0])
				if s:
					sch = s.group(1)
				records_enhanced = scheme[4]
				headings_total = scheme[5]
				headings_found = scheme[6]
				headings_not_found = scheme[7]

				#loadsfile.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>' % (sch,records_enhanced,headings_total,headings_found, headings_not_found))
				
			loadsfile.write('</table>')
			loadsfile.write('</td>--></tr>')

	loadsfile.write(end_main_table)
	loadsfile.write('</body></html>')
	print('wrote loads.html')
		

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

