# sub0

### under construction (naturally)

<i>This is for local experimentation at PUL. If you're searching around for something to enhance your MARC records, you might check out [MarcEdit](http://marcedit.reeset.net/)'s MARCNext tools.</i>

<b>Linked data prep.</b> Retrieve URIs and (optionally) insert $0 (subfield 0, sub0) into MARCXML records, in bulk.

The starting point is a list of Voyager bib ids in a csv file in the `./csv` directory, or a file of MARCXML records in the `./in` directory. (`vger.py` will grab the next batch of n records from Voyager, keeping a cache in a little sqlite db.)

Name and subject authority files are downloaded from [id.loc.gov](http://id.loc.gov/download/) (skos/rdf nt) and imported into [4store](http://4store.org/). When a given heading isn't found there, the script checks a cache of recent searches against id.loc.gov. If not found in the cache, id.loc.gov can be queried directly.

For now anyway, we're just using [known-label retrieval](http://id.loc.gov/techcenter/searching.html), not 'didyoumean' or 'suggest'). 

Install [4store](http://4store.org/) (Ubuntu)
```
sudo apt-get install 4store
sudo mkdir /var/lib/4store
```
Import lcsaf...
```
sudo 4s-backend-setup lcsaf
sudo 4s-backend lcsaf
sudo 4s-import --verbose lcsaf ~/Downloads/subjects-skos-20140306.nt
sudo 4s-httpd lcsaf -p 8000
```
Do the same for lcnaf, using say port 8001 (note: Oct 2014 file takes about 20G of disk space).

### Examples
Retrieve Voyager records, check names and subjects against lcnaf and lcsaf, and generate enhanced copies as well as csv reports: 

`python uris.py -vnsr -F csv/20160401.csv`

Another option: Get a file of records into the `./IN` directory...

`python uris.py -v -f csv/20160401.csv` 

... then parse them, getting URIs for names and subjects ... 

`python uris.py -vns`

Enhanced MARCXML records will be in the `./OUT` dir.

For 'help':

`python uris.py -h`

For reporting, `summaries.py` outputs totals to a TOTALS file as well as an html file.

### Current workflow
* wait until late afternoon / early evening
* `python vger.py` # get list of unsuppressed bibs into ./csv/yyyymmdd.csv
* `python uris.py -vnsr -F yyyymmdd` # process them, outputting reports as well as marcxml and MARC21 (mrc)
* load mrc file into Voyager using Gary Strawn's [RecordReloader](http://files.library.northwestern.edu/public/RecordReloader/)
* `python summaries.py -r yyyymmdd` # add stats to index.html file
* post newly generated index.html and report files to local server

### Dependencies
 * [4store](http://4store.org/)
 * libxml2 (`sudo apt-get install python-libxml2`)
 * <strike>sqlite3 (`sudo apt-get install sqlite3 libsqlite3-dev`)</strike>
 * [pymarc](https://github.com/edsu/pymarc) (`pip install pymarc`)
 * [requests](http://docs.python-requests.org/en/latest/index.html)
   (`pip install requests`)
 * xmllint (`sudo apt-get install libxml2-utils`)
   
### Makes use of
 * [id.loc.gov](http://id.loc.gov/)
 * <strike>[OCLC xID service](https://www.oclc.org/developer/develop/web-services/xid-api.en.html) (production-level access)</strike>
 * local (Princeton) bibdata service (marc_liberation)
 * [voyager_helpers](https://github.com/pulibrary/voyager_helpers)
 * [cmarcedit.exe](http://marcedit.reeset.net/cmarcedit-exe-using-the-command-line)
