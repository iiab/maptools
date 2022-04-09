#!/usr/bin/env  python3
# Upload the Regional osm-vector maps to InernetArchive

"""
Prep for running this program:
    pip3 install internetarchive
    pip3 install urllib3
    pip3 install certifi

    get authentication tokens to ~/.config/ia.ini
    set PREFIX to location of map mbtiles
"""
import os,sys
import json
import shutil
import subprocess
import internetarchive
import re
from datetime import datetime
import urllib3
import certifi

MAP_DATE = '2020-01-13'
CATALOG = './map-catalog.json'
CATALOG_URL = 'https://timmoody.com/iiab-files/maps/map-catalog.json'
SOURCE_URL_DIR = 'https://timmoody.com/iiab-files/maps'

DOWNLOAD_URL = 'https://archive.org/download'
GENERATED_TILES = '/library/www/html/internetarchive'
PLANET_MBTILES = GENERATED_TILES + "/osm_planet_z11-z14_2019.mbtiles"
PREFIX = '/hd/maps/maps-2020/staged'

def process_catalog_list(group):
   for mbtile in data[group].keys():
      if True:
         print(mbtile)
         if not os.path.exists(PREFIX + '/' + mbtile):
             print('Local file %s not found.'%(PREFIX + '/' + mbtile))
         else:
             src = SOURCE_URL_DIR + '/' + mbtile
             pool = urllib3.PoolManager()
             s = pool.request("GET",src,preload_content=False)
             if s.status == 200:
                # print("length:%s"%s.headers['Content-Length'])
                if int(s.headers['Content-Length']) != data[group][mbtile]['size']:
                    print('Size mismatch')
             else:
                print("Failed to open %s"%src) 

         # Fetch the md5 to see if local file needs uploading
         local_mbtile = PREFIX + '/' + mbtile
         with open(local_mbtile + '.md5','r') as md5_fp:
            instr = md5_fp.read()
            md5 = instr.split(' ')[0]
         if len(md5) == 0:
            print('md5 was zero length. ABORTING')
            sys.exit(1)

         perma_ref = 'en-osm-omt_' + data[group][mbtile]['region']
         identifier = mbtile[0:mbtile.find('.mbtiles')]

         # Gather together the metadata for archive.org
         md = {}
         md['title'] = "OSM Vector Server for %s"%mbtile
         #md['collection'] = "internetinabox"
         md["creator"] = "Internet in a Box" 
         md["subject"] = "rpi" 
         md["subject"] = "maps" 
         md["perma_ref"] = perma_ref
         md["licenseurl"] = "http://creativecommons.org/licenses/by-sa/4.0/"
         md["md5sum"] = md5
         md["mediatype"] = "software"
         md["description"] = "This client/server IIAB package makes OpenStreetMap data in vector format browsable from clients running Windows, Android, iOS browsers." 

         # Check is this has already been uploaded
         item = internetarchive.get_item(identifier)
         print('Identifier: %s. Filename: %s'%(identifier,local_mbtile,))
         if item.metadata:
            if item.metadata['md5sum'] == md5:
               # already uploaded
               print('local file md5:%s  metadata md5:%s'%(md5,item.metadata['md5sum']))
               print('Skipping %s -- checksums match'%mbtile)
               continue
            else:
               print('md5sums for %s do not match'%mbtile)
               r = item.modify_metadata({"md5sum":"%s"%md5})
         else:
            print('Archive.org does not have file with identifier: %s'%identifier) 

         # Debugging information
         print('Uploading %s'%mbtile)
         print('MetaData: %s'%md)
         
         #continue
         # upload to archive.org
         try:
            r = internetarchive.upload(identifier, files=[local_mbtile], metadata=md)
            print(r[0].status_code) 
            status = r[0].status_code
         except Exception as e:
            status = 'error'
            with open('./work/upload.log','a+') as ao_fp:
               ao_fp.write("Exception from internetarchive:%s"%e) 
         with open('./work/upload.log','a+') as ao_fp:
            now = datetime.now()
            date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
            ao_fp.write('Uploaded %s at %s Status:%s\n'%(identifier,date_time,status))
           

outstr = ''
http = urllib3.PoolManager()
r = http.request("get",CATALOG_URL,headers={"Accept":"*/*"})
if r.status < 400:
    data = json.loads(r.data)
else:
    print('Failed to open %s Status:%s'%(CATALOG_URL,r.status_code))
    sys.exit(1)
   
process_catalog_list('maps')
process_catalog_list('base')

outstr = json.dumps(data,indent=2,sort_keys=True) 
#print(outstr)
sys.exit(0)
