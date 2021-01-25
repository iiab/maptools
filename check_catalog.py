#!/usr/bin/env  python3
# read map_catalog.json and write map_catalog.json to stdout

import os,sys
import json
import sqlite3
import datetime
import glob
import requests

MAP_DATE = '2019-10-08'
CATALOG = './map-catalog.json'
DOWNLOAD_URL = 'https://archive.org/download'
GENERATED_TILES = '/library/www/html/internetarchive'
BASE_SATELLITE_SIZE = "976416768"
BASE_SATELLITE_URL = "https://archive.org/download/satellite_z0-z9_v3.mbtiles/satellite_z0-z9_v3.mbtiles"
BASE_PLANET_SIZE = "1870077952"
BASE_PLANET_URL = "https://archive.org/download/osm-planet_z0-z10_2019.mbtiles/osm-planet_z0-z10_2019.mbtiles"
PLANET_MBTILES = GENERATED_TILES + "/osm_planet_z11-z14_2019.mbtiles"

def process_catalog_list(map):
   global map_catalog
   if map_catalog.get(map,'') == '': return
   for region in map_catalog[map].keys():
      map_id = os.path.basename(map_catalog[map][region]['detail_url'])
      for (key, value) in map_catalog[map][region].items():
         map_catalog[map][map_id].update( {key : value} )
      map_catalog[map][map_id]['date'] = MAP_DATE
      map_catalog[map][map_id]['sat_url'] = 'not used'
      map_catalog[map][map_id]['archive_url'] = os.path.join(DOWNLOAD_URL,map_id,map_id)
      map_catalog[map][map_id]['bittorrent_url'] = os.path.join(DOWNLOAD_URL,map_id,map_id + '_archive.torrent')
      size = os.path.getsize(GENERATED_TILES + '/' + map_id)
      map_catalog[map][map_id]['mbtile_size'] = size
      map_catalog[map][map_id]['osm_size'] = size
      map_catalog[map][map_id]['sat_size'] = BASE_SATELLITE_SIZE
      map_catalog[map][map_id]['size'] = size + int(BASE_PLANET_SIZE) + int(BASE_SATELLITE_SIZE)
      if not check_url(map_catalog[map][map_id]['detail_url']):
            print('Map region at %s not found'%map_catalog[map][map_id]['detail_url'])
            sys.exit(1)
    
def check_url(url):
    r = requests.head(url)
    if r.status_code < 400:
        return True
    return False

outstr = ''
with open(CATALOG,'r') as catalog_fp:
   try:
      map_catalog = json.loads(catalog_fp.read())
   except:
      print("json error reading regions.json")
      sys.exit(1)
   process_catalog_list('maps')
   process_catalog_list('base')

   outstr = json.dumps(map_catalog,indent=2,sort_keys=True) 
   print(outstr)
   print('All detail_url links are satisfied')
   sys.exit(0)

