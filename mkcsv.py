#!/usr/bin/env  python
# create csv file as expected by openmaptiles/extracts

import os,sys
import json
import uuid

EXTRACT_DIR = '/opt/iiab/extracts'

CATALOG = './map-catalog.json'

with open(CATALOG,'r') as catalog_fp:
   try:
      data = json.loads(catalog_fp.read())
   except:
      print("json error reading map-catalog.json")
      sys.exit(1)

#target = EXTRACT_DIR + '/iiab.csv'
#with open(target,'w') as csv_fp:
#csv_fp.write(headers)

headers = 'map_id,id,country,city,left,bottom,right,top\n'
outstr = headers
for map_id in data['maps'].keys():
  outstr += '%s,%s,%s,%s,%s,%s,%s,%s\n'%(map_id,uuid.uuid4().hex,'','',
     data['maps'][map_id]['west'],data['maps'][map_id]['south'],
     data['maps'][map_id]['east'],data['maps'][map_id]['north'])
#csv_fp.write(outstr)
#csv_fp.close()
print(outstr)
