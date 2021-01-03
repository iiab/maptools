#!/usr/bin/env  python3
# Read all mbtiles in curdir. Write bboxes.geojson to ./output/bboxes.geojson

import os,sys
import json
import sqlite3
import glob
import argparse
import tools
import geojson
from geojson import Feature, Point, FeatureCollection, Polygon
#import pdb;pdb.set_trace()

# Globals
MBTILE_DIR = '.'
ZOOM = 11
if not os.path.exists('./output'):
   os.mkdir('./output')

if len(sys.argv)< 3:
   print('Assuming zoom level 11')
if len(sys.argv)< 2:
   print('Examining mbtiles in current directory')
else:
   MBTILE_DIR = argv[1]
   print('Esamining mbtiles in %s'%MBTILE_DIR)
MAX_TILE_AT_THIS_ZOOM = 2 ** ZOOM

# initialize the output string
outstr = ''
features = []
for f in glob.glob(MBTILE_DIR + '/*.mbtiles'):
   print(f)
   try: 
      conn = sqlite3.connect(f)
      c = conn.cursor()
      sql = 'select value from metadata where name = "bounds"'
      c.execute(sql)
   except Exception as e:
      print("ERROR -no access to metadata in region:%s"%region)
      print("Path:%s"%f)
      print("sql error: %s"%e)
   row = c.fetchone()
   print("Bounds from metadata:%s"%row[0])
   try:
      sql = 'select tile_column,tile_row from map where zoom_level = %s order by tile_column'%ZOOM
      c.execute(sql)
   except Exception as e:
      print("Path:%s"%MBTILE_DIR)
      print("sql error: %s"%e)
      sys.exit(1)
   rows = c.fetchall()
   # check for a region stradling the dateline
   #print("Max number of tile in either direction:%s on zoom_level:%s"%(MAX_TILE_AT_THIS_ZOOM,ZOOM))
   #print('number of rows:%s value at last row:%s'%(len(rows),rows[len(rows)-1][1]))
   if rows[0][0] == 0 and rows[len(rows)-1][0] == MAX_TILE_AT_THIS_ZOOM - 1:
      # Yes we straddle dateline
      cur = 0
      while rows[cur][0] < MAX_TILE_AT_THIS_ZOOM / 2:
         cur += 1
      print("row index:%s  max_X:%s"%(cur,rows[cur][0]))
      max_x = rows[cur][0]
      min_x = rows[cur+1][0]
      sql = 'select min(tile_row), max(tile_row) from map where zoom_level = %s group by zoom_level'%ZOOM
      c.execute(sql)
      rows = c.fetchall()
      min_y = rows[0][0] 
      max_y = rows[0][1] 
   else:
      print("we do not straddle dateline")
      max_x = 0
      max_y = 0
      min_x = MAX_TILE_AT_THIS_ZOOM
      min_y = MAX_TILE_AT_THIS_ZOOM
      for i in range(len(rows)):
         if rows[i][0] > max_x:
            max_x = rows[i][0] 
         if rows[i][0] < min_x:
            min_x = rows[i][0] 
         if rows[i][1] > max_y:
            max_y = rows[i][1] 
         if rows[i][1] < min_y:
            min_y = rows[i][1] 
      print('min_x:%s max_x:%s min_y:%s max_y:%s'%(min_x,max_x,min_y,max_y))
      n,w = tools.xy2latlon(min_x,MAX_TILE_AT_THIS_ZOOM-max_y,ZOOM)
      s,e = tools.xy2latlon(max_x,MAX_TILE_AT_THIS_ZOOM-min_y,ZOOM)
      print('Observed bounds in mbtile(w:%s s:%s e:%s n:%s'%(w,s,e,n))      
      poly = Polygon([[[w,s],[e,s],[e,n],[w,n],[w,s]]])
      features.append(Feature(geometry=poly,properties={"name":os.path.basename(f)}))

      collection = FeatureCollection(features)
      outstr += geojson.dumps(collection, indent=2, sort_keys=True)
print(outstr)
     
with open('./output/bboxes.geojson','w') as fp:
   fp.write(outstr)
