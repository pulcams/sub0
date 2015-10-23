# sub0

### under construction (naturally)

<b>Linked data prep.</b> Retrieve URIs and (optionally) insert $0 (subfield 0, sub0) into MARCXML records, in bulk.

The starting point is a list of bib ids in a csv file in the `./csv` directory, or a set of MARCXML records in the `./in` directory.

Name and Subject authority files are downloaded from [id.loc.gov](http://id.loc.gov/download/) (skos/rdf nt) and imported into [4store](http://4store.org/).
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
Do the same for lcnaf.

### Examples

Retrieve Voyager records, check names and subjects against lcnaf and lcsaf, and generate enhanced copies as well as csv reports: 

`python uris.py -vcns -F csv/my_bibs.csv`

Get a bunch of records into the `./in` directory...
`python uris.py -v -f csv/my_bibs.csv` 
...then parse them... 
`python uris.py -sn`

For 'help':

`python uris.py -h`

### Dependencies:
 * [4store](http://4store.org/)
 * libxml2 (`sudo apt-get install python-libxml2`)
 * sqlite3 (`sudo apt-get install sqlite3 libsqlite3-dev`)
 * [pymarc](https://github.com/edsu/pymarc) (`pip install pymarc`)
 * [requests](http://docs.python-requests.org/en/latest/index.html)
   (`pip install requests`)
 * xmllint (`sudo apt-get install libxml2-utils`)
   
### Makes use of:
 * [id.loc.gov](http://id.loc.gov/)
 * <strike>[OCLC xID service](https://www.oclc.org/developer/develop/web-services/xid-api.en.html) (production-level access)</strike>
 * local (Princeton) bibdata service (marc_liberation)
