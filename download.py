#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# Download satellite images from Sentinel Cloudless
# notes to set up this exploration
#  -- the symbolic link satellite.mbtiles is set to source
#  -- output placed in ./work/satellite_z0-z10_<name>.mbtiles
#  -- just a -z option throw app into viewer mode
#  -- Use -h for the available options

# help from https://github.com/TimSC/pyMbTiles/blob/master/MBTiles.py

import sqlite3
import sys, os
import argparse
from PIL import Image
from io import BytesIO
import curses
import certifi
import urllib3
import tools
import subprocess
import json
import math
import uuid
import shutil
from multiprocessing import Process, Lock
import time
from datetime import datetime


# Download source of satellite imagry
url =  "https://tiles.maps.eox.at/wmts?layer=s2cloudless-2020_3857&style=default&tilematrixset=g&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image%2Fjpeg&TileMatrix={z}&TileCol={x}&TileRow={y}"
ATTRIBUTION = os.environ.get('METADATA_ATTRIBUTION', '<a href="http://openmaptiles.org/" target="_blank">&copy; OpenMapTiles</a> <a href="http://www.openstreetmap.org/about/" target="_blank">&copy; OpenStreetMap contributors</a>')
VERSION = os.environ.get('METADATA_VERSION', '3.3')
src = object # the open url source
# tiles smaller than this are probably ocean
threshold = 2000

# GLOBALS
mbTiles = object
args = object
bounds = {}
regions = {}
bbox_zoom_start = 0 
bbox_limits = {}
stdscr = object # cursors object for progress feedback
config_fn = 'config.json'
config = {}
earth_around = 40075 # in KM
tile_metadata = {}

class MBTiles():
   def __init__(self, filename):
      self.conn = sqlite3.connect(filename)
      self.conn.row_factory = sqlite3.Row
      self.conn.text_factory = str
      self.c = self.conn.cursor()
      self.schemaReady = False

   def __del__(self):
      self.conn.commit()
      self.c.close()
      del self.conn

   def ListTiles(self):
      rows = self.c.execute("SELECT zoom_level, tile_column, tile_row FROM tiles")
      out = []
      for row in rows:
         out.append((row[0], row[1], row[2]))
      return out

   def GetTile(self, zoomLevel, tileColumn, tileRow):
      rows = self.c.execute("SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?", 
         (zoomLevel, tileColumn, tileRow))
      rows = list(rows)
      if len(rows) == 0:
         raise RuntimeError("Tile not found")
      row = rows[0]
      return row[0]

   def CheckSchema(self):     
      sql = 'CREATE TABLE IF NOT EXISTS map (zoom_level INTEGER,tile_column INTEGER,tile_row INTEGER,tile_id TEXT,grid_id TEXT)'
      self.c.execute(sql)

      sql = 'CREATE TABLE IF NOT EXISTS images (tile_data blob,tile_id text)'
      self.c.execute(sql)

      sql = 'CREATE TABLE IF NOT EXISTS satdata (zoom_level INTEGER,name text,value text)'
      self.c.execute(sql)

      sql = 'CREATE TABLE IF NOT EXISTS metadata (zoom_level INTEGER,name text,value text)'
      self.c.execute(sql)

      sql = 'CREATE VIEW IF NOT EXISTS tiles AS SELECT map.zoom_level AS zoom_level, map.tile_column AS tile_column, map.tile_row AS tile_row, images.tile_data AS tile_data FROM map JOIN images ON images.tile_id = map.tile_id'
      self.c.execute(sql)

      self.schemaReady = True

   def GetAllMetaData(self):
      rows = self.c.execute("SELECT name, value FROM metadata")
      out = {}
      for row in rows:
         out[row[0]] = row[1]
      return out

   def SetMetaData(self, name, value):
      if not self.schemaReady:
         self.CheckSchema()

      self.c.execute("UPDATE metadata SET value=? WHERE name=?", (value, name))
      if self.c.rowcount == 0:
         self.c.execute("INSERT INTO metadata (name, value) VALUES (?, ?);", (name, value))

      self.conn.commit()

   def DeleteMetaData(self, name):
      if not self.schemaReady:
         self.CheckSchema()

      self.c.execute("DELETE FROM metadata WHERE name = ?", (name,))
      self.conn.commit()
      if self.c.rowcount == 0:
         raise RuntimeError("Metadata name not found")

   def SetSatMetaData(self, zoomLevel, name, value):
      if not self.schemaReady:
         self.CheckSchema()

      self.c.execute("UPDATE satdata SET value=? WHERE zoom_level=? AND name = ?", (value, zoomLevel, name))
      if self.c.rowcount == 0:
         self.c.execute("INSERT INTO satdata (zoom_level, name, value) VALUES (?, ?, ?);", (zoomLevel, name, value))

      self.conn.commit()

   def GetSatMetaData(self,zoomLevel):
      rows = self.c.execute("SELECT name, value FROM satdata WHERE zoom_level = ?",(str(zoomLevel),))
      out = {}
      for row in rows:
         out[row[0]] = row[1]
      return out

   def DeleteSatData(self, zoomLevel, name):
      if not self.schemaReady:
         self.CheckSchema()

      self.c.execute("DELETE FROM satdata WHERE name = ? AND zoom_level = ?", (zoomLevel, name,))
      self.conn.commit()
      if self.c.rowcount == 0:
         raise RuntimeError("SatData name %s not found"%name)

   def SetTile(self, zoomLevel, tileColumn, tileRow, data):
      if not self.schemaReady:
         self.CheckSchema()

      tile_id = self.TileExists(zoomLevel, tileColumn, tileRow)
      if tile_id: 
         tile_id = uuid.uuid4().hex
         operation = 'update images'
         self.c.execute("DELETE FROM images  WHERE tile_id = ?;", ([tile_id]))
         self.c.execute("INSERT INTO images (tile_data,tile_id) VALUES ( ?, ?);", (sqlite3.Binary(data),tile_id))
         if self.c.rowcount != 1:
            raise RuntimeError("Failure %s RowCount:%s"%(operation,self.c.rowcount))
         self.c.execute("""UPDATE map SET tile_id=? where zoom_level = ? AND 
               tile_column = ? AND tile_row = ?;""", 
            (tile_id, zoomLevel, tileColumn, tileRow))
         if self.c.rowcount != 1:
            raise RuntimeError("Failure %s RowCount:%s"%(operation,self.c.rowcount))
         self.conn.commit()
         return
      else: # this is not an update
         tile_id = uuid.uuid4().hex
         self.c.execute("INSERT INTO images ( tile_data,tile_id) VALUES ( ?, ?);", (sqlite3.Binary(data),tile_id))
         if self.c.rowcount != 1:
            raise RuntimeError("Insert image failure")
         operation = 'insert into map'
         self.c.execute("INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (?, ?, ?, ?);", 
            (zoomLevel, tileColumn, tileRow, tile_id))
      if self.c.rowcount != 1:
         raise RuntimeError("Failure %s RowCount:%s"%(operation,self.c.rowcount))
      self.conn.commit()
   

   def DeleteTile(self, zoomLevel, tileColumn, tileRow):
      if not self.schemaReady:
         self.CheckSchema()

      tile_id = self.TileExists(zoomLevel, tileColumn, tileRow)
      if not tile_id:
         raise RuntimeError("Tile not found")

      self.c.execute("DELETE FROM images WHERE tile_id = ?;",tile_id) 
      self.c.execute("DELETE FROM map WHERE tile_id = ?;",tile_id) 
      self.conn.commit()

   def TileExists(self, zoomLevel, tileColumn, tileRow):
      if not self.schemaReady:
         self.CheckSchema()

      sql = 'select tile_id from map where zoom_level = ? and tile_column = ? and tile_row = ?'
      self.c.execute(sql,(zoomLevel, tileColumn, tileRow))
      row = self.c.fetchall()
      if len(row) == 0:
         return None
      return str(row[0][0])

   def DownloadTile(self, zoomLevel, tileColumn, tileRow, lock):
      # if the tile already exists, do nothing
      lock.acquire()
      tile_id = self.TileExists(zoomLevel, tileColumn, tileRow)
      lock.release()
      if tile_id:
         #print('tile already exists -- skipping')
         return 
      try:
         #wmts_row = int(2 ** zoomLevel - tileRow - 1)
         r = src.get(zoomLevel,tileColumn,tileRow)
      except Exception as e:
         raise RuntimeError("Source data failure;%s"%e)
         
      if r.status == 200:
         lock.acquire()
         self.SetTile(zoomLevel, tileColumn, tileRow, r.data)
         self.conn.commit()
         lock.release()
      else:
         print('Sat data error, returned:%s'%r.status)

   def Commit(self):
      self.conn.commit()

   def get_bounds(self):
     global bounds
     sql = 'select zoom_level, min(tile_column),max(tile_column),min(tile_row),max(tile_row), count(zoom_level) from tiles group by zoom_level;'
     resp = self.c.execute(sql)
     rows = resp.fetchall()
     for row in rows:
         bounds[row['zoom_level']] = { 'minX': row['min(tile_column)'],\
                                  'maxX': row['max(tile_column)'],\
                                  'minY': row['min(tile_row)'],\
                                  'maxY': row['max(tile_row)'],\
                                  'count': row['count(zoom_level)'],\
                                 }
     outstr = json.dumps(bounds,indent=2)
     # diagnostic info
     with open('./work/bounds.json','w') as bounds_fp:
        bounds_fp.write(outstr)
     return bounds

   def summarize(self):
     sql = 'select zoom_level, min(tile_column),max(tile_column),min(tile_row),max(tile_row), count(zoom_level) from tiles group by zoom_level;'
     self.c.execute(sql)
     rows = self.c.fetchall()
     print('Zoom Levels Found:%s'%len(rows))
     for row in rows:
       if row[2] != None and row[1] != None and row[3] != None and row[4] != None:
         print('%s %s %s %s %s %s %s'%(row[0],row[1],row[2],row[3],row[4],\
              row[5], (row[2]-row[1]+1) * ( row[4]-row[3]+1)))
         self.SetSatMetaData(row[0],'minX',row[1])
         self.SetSatMetaData(row[0],'maxX',row[2])
         self.SetSatMetaData(row[0],'minY',row[3])
         self.SetSatMetaData(row[0],'maxY',row[4])
         self.SetSatMetaData(row[0],'count',row[5])
         
         
  
   def CountTiles(self,zoom):
      self.c.execute("select tile_data from tiles where zoom_level = ?",(zoom,))
      num = 0
      while self.c.fetchone():
         num += 1 
      return num

   def execute_script(self,script):
      self.c.executescript(script)

   def copy_zoom(self,zoom,src):
      sql = 'ATTACH DATABASE "%s" as src'%src
      self.c.execute(sql)
      sql = 'INSERT INTO map SELECT * from src.map where src.map.zoom_level=?'
      self.c.execute(sql,[zoom])
      sql = 'INSERT OR IGNORE INTO images SELECT src.images.tile_data, src.images.tile_id from src.images JOIN src.map ON src.map.tile_id = src.images.tile_id where map.zoom_level=?'
      self.c.execute(sql,[zoom])
      sql = 'DETACH DATABASE src'
      self.c.execute(sql)

   def copy_mbtile(self,src):
      sql = 'ATTACH DATABASE "%s" as src'%src
      self.c.execute(sql)
      sql = 'INSERT INTO map SELECT * from src.map where true'
      self.c.execute(sql,[zoom])
      sql = 'INSERT OR IGNORE INTO images SELECT src.images.tile_data, src.images.tile_id from src.images JOIN src.map ON src.map.tile_id = src.images.tile_id where true'
      self.c.execute(sql,[zoom])
      sql = 'DETACH DATABASE src'
      self.c.execute(sql)

   def delete_zoom(self,zoom):
      sql = 'DELETE FROM images where tile_id in (SELECT tile_id from map WHERE map.zoom_level=?)'
      self.c.execute(sql,[zoom])
      sql = 'DELETE FROM map where zoom_level=?'
      self.c.execute(sql,[zoom])
      sql = "vacuum"
      self.c.execute(sql)
      self.Commit()

class WMTS(object):

   def __init__(self, template):
      self.template = template
      self.http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',\
           ca_certs=certifi.where(),maxsize=10)

   def get(self,z,x,y):
      srcurl = "%s"%self.template
      srcurl = srcurl.replace('{z}',str(z))
      srcurl = srcurl.replace('{x}',str(x))
      srcurl = srcurl.replace('{y}',str(y))
      #print(srcurl[-50:])
      resp = (self.http.request("GET",srcurl,retries=10))
      return(resp)
      
class Extract(object):

    def __init__(self, extract, top, left, bottom, right,
                 min_zoom=0, max_zoom=14, center_zoom=10):
        self.extract = extract

        self.min_lon = left
        self.min_lat = bottom
        self.max_lon = right
        self.max_lat = top

        self.min_zoom = min_zoom
        self.max_zoom = max_zoom
        self.center_zoom = center_zoom

    def bounds(self):
        return '%s,%s,%s,%s'%(self.min_lon, self.min_lat,
                                    self.max_lon, self.max_lat)

    def center(self):
        center_lon = (float(self.min_lon) + float(self.max_lon)) / 2.0
        center_lat = (float(self.min_lat) + float(self.max_lat))/ 2.0
        return '%s,%s,%s'%(center_lon, center_lat, self.center_zoom)

    def metadata(self):
        return {
            "type": os.environ.get('METADATA_TYPE', 'baselayer'),
            "attribution": ATTRIBUTION,
            "version": VERSION,
            "minzoom": self.min_zoom,
            "maxzoom": self.max_zoom,
            "name": os.environ.get('METADATA_NAME', 'OpenMapTiles'),
            "id": os.environ.get('METADATA_ID', 'openmaptiles'),
            "description": os.environ.get('METADATA_DESC', "Extract from http://openmaptiles.org"),
            "bounds": self.bounds(),
            "center": self.center(),
            "basename": os.path.basename(self.extract),
            "scheme": 'tms',
            "filesize": os.path.getsize(self.extract)
        }


def put_config():
   global config
   with open(config_fn,'w') as cf:
     cf.write(json.dumps(config,indent=2))
 
def get_config():
   global config
   if not os.path.exists(config_fn):
      put_config()

   with open(config_fn,'r') as cf:
     config = json.loads(cf.read())
    
def set_up_target_db():
   # create output target, use default input source
   global mbTiles,config,dbpath

   # attach to the correct output database
   dbname = 'satellite_z%s-z%s_%s.mbtiles'%(bbox_zoom_start,args.zoom,args.date)
   dbpath = './work/%s'%dbname
   if not os.path.exists(dbpath):
      shutil.copyfile(args.mbtiles,dbpath) 
   print('DESTINATION: %s'%dbpath)
   mbTiles = MBTiles(dbpath)
   mbTiles.CheckSchema()
   mbTiles.get_bounds()
   config['last_dest'] = dbpath
   put_config()

def set_up_new_target_db(region):
   # create output target, use default input source
   global mbTiles,config
   # destroying object closes any open database
   mbTiles = None

   # attach to the correct output database
   dbname = 'satellite_z%s-z9_%s.mbtiles'%(bbox_zoom_start,args.date)
   dbpath = './work/%s'%dbname
   if not os.path.exists(dbpath):
      mbTiles = MBTiles(dbpath)
      print('Initializing schema for %s'%dbpath)
      with open('tilelive.schema','r') as fp:
         script = fp.read()
      mbTiles.execute_script(script)
      print('Copying zoom %s from %s to %s'%(bbox_zoom_start-1,args.mbtiles,dbpath))
      mbTiles.copy_zoom(str(bbox_zoom_start-1),args.mbtiles)
   else:
      mbTiles = MBTiles(dbpath)
   mbTiles.CheckSchema()
   mbTiles.get_bounds()
   config['last_dest'] = dbpath
   put_config()

def to_dir():
   # write out file tree from mbtiles database
   if args.dir != ".":
      prefix = os.path.join(args.dir,'work')
   else:
      prefix = './work'
   for zoom in range(5):
      n = numTiles(zoom)
      for row in range(n):
         for col in range(n):
            this_path = os.path.join(prefix,str(zoom),str(col),str(row)+'.jpeg')
            if not os.path.isdir(os.path.dirname(this_path)):
               os.makedirs(os.path.dirname(this_path))
            raw = get_tile(zoom,col,row)
            with open(this_path,'w') as fp:
               fp.write(raw)

def list_tile_sizes():
   bounds = mbTiles.get_bounds()
   for zoom in sorted(bounds):
      if bounds[zoom]['minX'] != 0:
          break
   for i in range(zoom,14):
      header = True
      if bounds.get(i,0) == 0: continue
      for y in range(bounds[i]['minY'],bounds[i]['maxY']):
         tilelen={}
         outstr = '%s  '%y
         lower = bounds[i]['minX']
         upper = bounds[i]['maxX'] 
         if header:
            print('%s   %s   %s'%(i, lower, upper))
            header = False
         for x in range(lower,upper):
            data = mbTiles.GetTile(i, x, y)
            tilelen[x] = len(data)
            if len(data) > threshold:
               outstr  += 'X'
            else:
               outstr += 'O'
         print(outstr)
         print(str(tilelen))
         
def debug_one_tile():
   if not args.x:
      args.x = 2
      args.y = 2
      args.zoom = 2
   
   global src # the opened url for satellite images
   try:
      src = WMTS(url)
   except:
      print('failed to open source')
      sys.exit(1)
   response = src.get(args.zoom,args.x,args.y)
   print(response.status) 
   print(len(response.data))
   
def parse_args():
    parser = argparse.ArgumentParser(description="Display mbtile image.")
    parser.add_argument("-c","--copy", help='Copy -m as src and extend.',action='store_true')
    parser.add_argument("-d","--dir", help='Output to this directory (use "." for ./work/)')
    parser.add_argument("-e", "--extend", help="Get z10-13.",action="store_true")
    parser.add_argument("-g", "--get", help='get WMTS tiles from this URL(of "." for Sentinel Cloudless).')
    parser.add_argument("-i", "--inspect", help="Command line inspection.",action="store_true")
    parser.add_argument("-l", "--list", help="List tile sizes.",action="store_true")
    parser.add_argument("-m", "--mbtiles", help="mbtiles filename.")
    parser.add_argument("-n", "--date", help="mbtiles date component.")
    parser.add_argument("-o", "--onetile", help="Get one tile from source.",action="store_true")
    parser.add_argument("--lat", help="Latitude degrees.",type=float)
    parser.add_argument("--lon", help="Longitude degrees.",type=float)
    parser.add_argument("-r", "--region", help="Region to operate upon.")
    parser.add_argument("-s", "--summarize", help="Data about each zoom level.",action="store_true")
    parser.add_argument("-t", "--appendmd", help="Append metadata.",action="store_true")
    parser.add_argument("-x",  help="tileX", type=int)
    parser.add_argument("-y",  help="tileY", type=int)
    parser.add_argument('-z',"--zoom", help="zoom level. (Default=2)", type=int)
    return parser.parse_args()

def numTiles(z):
  return(pow(2,z))

def show_metadata():
   metadata = mbTiles.GetAllMetaData()
   for k in metadata:
      print (k, metadata[k])

def get_tile(zoom,tilex,tiley):
   try:
      data = mbTiles.GetTile(zoom, tilex, tiley)
   except RuntimeError as err:
      print (err)
   return(data)

def replace_tile(src,zoom,tileX,tileY):
   global total_tiles
   try:
      r = src.get(zoom,tileX,tileY)
   except Exception as e:
      print(str(e))
      sys.exit(1)
   if r.status == 200:
      raw = r.data
      line = bytearray(raw)
      if line.find("DOCTYPE") != -1:
         print('still getting html from sentinel cloudless')
         return False
      else:
         try:
            image = Image.open(BytesIO(raw))
            #image.show(BytesIO(raw))
         except Exception as e:
            print('exception:%s'%e)
            sys.exit()
         #raw_input("PRESS ENTER")
         mbTiles.SetTile(zoom, tileX, tileY, r.data)
         returned = mbTiles.GetTile(zoom, tileX, tileY)
         if returned != r.data:
            print('read verify in replace_tile failed')
            return False
         return True
   else:
      print('get url in replace_tile returned:%s'%r.status)
      return False

def get_regions():
   global regions
   # error out if environment is missing

   REGION_INFO = './regions.json'
   with open(REGION_INFO,'r') as region_fp:
      try:
         data = json.loads(region_fp.read())
         regions = data['regions']
      except:
         print("regions.json parse error")
         sys.exit(1)
   
def set_metadata(region):
   global extract,dbpath
   extract = Extract(dbpath,
        left=regions[region]['west'],
        right=regions[region]['east'],
        top=regions[region]['north'],
        bottom=regions[region]['south'],
        center_zoom=regions[region]['zoom'],
        min_zoom=bbox_zoom_start,
        max_zoom=args.zoom)
   outdict = extract.metadata()
   for key in outdict.keys():
      mbTiles.SetMetaData(key,outdict[key])
      # print(key,outdict[key])
   mbTiles.Commit()

def view_tiles(stdscr):
   # permits viewing of individual image tiles (-x,-y,-y parameters)
   global args
   global mbTiles
   global src # the opened url for satellite images
   try:
      src = WMTS(url)
   except:
      print('failed to open source')
      sys.exit(1)
   if args.zoom:
      zoom = args.zoom
   else:
      zoom = 2
   state = { 'zoom': zoom}
   if args.x:
      state['tileX'] = args.x
   else:
      state['tileX'] = bounds[state['zoom']]['minX']
   if args.y:
      state['tileY'] = 2 ** zoom  - args.y - 1
      state['tileY'] = args.y
   else:
      state['tileY'] = bounds[state['zoom']]['minY']
   state['source'] = 'tile'
   while 1:
      try:
         if state['source'] == 'tile':
            raw = mbTiles.GetTile(state['zoom'],state['tileX'],state['tileY'])
         else:
            resp = src.get(state['zoom'],state['tileX'],state['tileY'])
            if resp.status == 200:
              raw = resp.data
            else:
               stdscr.addstr(1,0,"failed to fetch SAT data from URL")
               continue
         proc = subprocess.Popen(['killall','display'])
         proc.communicate()
         stdscr.clear()
         stdscr.addstr(0,0,'zoom:%s lon:%s lat:%s'%(state['zoom'],state['tileX'],state['tileY']))
         stdscr.addstr(0,40,'Size of tile:%s'%len(raw))
         stdscr.refresh() 
         image = Image.open(BytesIO(raw))
         image.show()
      except Exception as e:  
         stdscr.addstr(1,0,'Exception:%s. x:%s y:%s'%(e,state['tileX'],state['tileY']))
         stdscr.refresh() 

      n = numTiles(state['zoom'])
      ch = stdscr.getch()
      if ch == ord('q'):
         proc = subprocess.Popen(['killall','display'])
         proc.communicate()
         break  # Exit the while()
      if ch == curses.KEY_UP:
         if not state['tileY'] == bounds[state['zoom']]['minY']:
            state['tileY'] -= 1
      elif ch == curses.KEY_RIGHT:
         if not state['tileX'] == bounds[state['zoom']]['maxX']:
            state['tileX'] += 1
      elif ch == curses.KEY_LEFT:
         if not state['tileX'] == bounds[state['zoom']]['minX']:
            state['tileX'] -= 1
      elif ch == curses.KEY_DOWN:
         if not state['tileY'] == bounds[state['zoom']]['minY']-1:
            state['tileY'] += 1
      elif ch == ord('s'):
            state['source'] = 'sat'
      elif ch == ord('t'):
            state['source'] = 'tile'
      elif ch == ord('p'):
         replace_tile(src,state['zoom'],state['tileX'],state['tileY'])
      elif ch == ord('='):
         if not state['zoom'] == 14:
            state['tileX'] *= 2
            state['tileY'] *= 2
            state['zoom'] += 1
      elif ch == ord('-'):
         if not state['zoom'] == 1:
            state['tileX'] /= 2
            state['tileY'] /= 2
            state['zoom'] -= 1

def sec2hms(n):
    days = n // (24 * 3600) 
  
    n = n % (24 * 3600) 
    hours = n // 3600
  
    n %= 3600
    minutes = n // 60
  
    n %= 60
    seconds = n 
    return '%s days, %s hours, %s minutes %s seconds'%(days,hours,minutes,seconds)

def coordinates2WmtsTilesNumbers(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(float(lat_deg))
  n = 2.0 ** zoom
  xtile = int((float(lon_deg) + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
  # the following would accomodate bottom left origin
  #ytile = int(n - ytile - 1)
  return (xtile, ytile)

def bbox_tile_limits(west, south, east, north, zoom):
   #print('west:%s south:%s east:%s north:%s zoom:%s'%(west,south,east,north,zoom))
   sw = coordinates2WmtsTilesNumbers(south,west,zoom)
   ne = coordinates2WmtsTilesNumbers(north,east,zoom)
   x_num = ne[0]+1 - sw[0]
   y_num = ne[1]+1 - sw[1]
   #print('ymin:%s ymax:%s'%(sw[1],ne[1]))
   #print('ne_x:%s x_num:%s ne_y:%s y_num:%s'%(ne[0],x_num,ne[1],y_num))
   #print('number of tiles of zoom %s:%s = %d seconds at 8/second'%(zoom,y_num*x_num,y_num*x_num/8))
   #return(xmin,xmax,ymin,ymax)
   return(sw[0],ne[0]+1, ne[1], sw[1]+1)

def record_bbox_debug_info(region):
   #cur_box = regions[region]
   for zoom in range(bbox_zoom_start-1,14):
      xmin,xmax,ymin,ymax = bbox_tile_limits(cur_box['west'],cur_box['south'],\
            cur_box['east'],cur_box['north'],zoom)
      #print(xmin,xmax,ymin,ymax,zoom)
      tot_tiles = mbTiles.CountTiles(zoom)
      bbox_limits[zoom] = { 'minX': xmin,'maxX':xmax,'minY':ymin,'maxY':ymax,                              'count':tot_tiles}
   with open('./work/bbox_limits','w') as fp:
      fp.write(json.dumps(bbox_limits,indent=2))

def put_accumulators(zoom,ocean=0,land=0,count=0,done='False'):
   mbTiles.SetSatMetaData(zoom,'ocean',str(ocean))
   mbTiles.SetSatMetaData(zoom,'land',str(land))
   mbTiles.SetSatMetaData(zoom,'count',str(count))
   mbTiles.SetSatMetaData(zoom,'done',str(done))

def get_accumulators(zoom):
   data = mbTiles.GetSatMetaData(zoom)
   #print str(data)
   return (\
      int(str(data.get('ocean',0))),\
      int(str(data.get('land',0))),\
      int(str(data.get('tileX',0))),\
      int(str(data.get('tileY',0))),\
      int(str(data.get('count',0))),\
      bool(str(data.get('done',False)))\
   )

def fetch_quad_for(tileX, tileY, zoom):
   # get 4 tiles for zoom+1
   lock = Lock()
   p1 = Process(target=mbTiles.DownloadTile, args=(zoom+1,tileX*2,tileY*2,lock))
   p1.start()
   p2 = Process(target=mbTiles.DownloadTile, args=(zoom+1,tileX*2+1,tileY*2,lock))
   p2.start()
   p3 = Process(target=mbTiles.DownloadTile, args=(zoom+1,tileX*2,tileY*2+1,lock))
   p3.start()
   p4 = Process(target=mbTiles.DownloadTile, args=(zoom+1,tileX*2+1,tileY*2+1,lock))
   p4.start()
   p1.join()
   p2.join()
   p3.join()
   p4.join()


def is_done(zoom):
   data = mbTiles.GetSatMetaData(zoom)
   #print('zoom:%s data:%s'%(zoom,data))
   return bool(data.get('done',False))
  
def download_region(region):
   global src # the opened url for satellite images

   # attach to the correct output database
   dbname = 'sat-%s-sentinel-z0_13.mbtiles'%region
   dbpath = './work/%s'%dbname
   if not os.path.exists(dbpath):
      shutil.copyfile('./satellite.mbtiles',dbpath) 
   mbTiles = MBTiles(dbpath)
   mbTiles.get_bounds()
   # print some summary info for this region
   stdscr.addstr(1,0,"ZOOM")
   stdscr.addstr(0,15,region)
   stdscr.addstr(1,10,'PRESENT')
   stdscr.addstr(1,20,'NEEDED')
   stdscr.addstr(1,30,'PERCENT')
   stdscr.addstr(1,50,"DAYS")

   # 
   cur_box = regions[region]
   for zoom in range(14):
      stdscr.addstr(zoom+2,0,str(zoom))
      xmin,xmax,ymin,ymax = bbox_tile_limits(cur_box['west'],cur_box['south'],\
            cur_box['east'],cur_box['north'],zoom)
      #print(xmin,xmax,ymin,ymax,zoom)
      bbox_limits[zoom] = { 'minX': xmin,'maxX':xmax,'minY':ymin,'maxY':ymax}

      if bounds.get(zoom,-1) == -1: continue
      tiles = (bounds[zoom]['maxX']-bounds[zoom]['minX'])*\
              (bounds[zoom]['maxY']-bounds[zoom]['minY'])
      stdscr.addstr(zoom+2,10,str(tiles))
      
   for zoom in range(bbox_zoom_start,13):
      stdscr.addstr(zoom+2,0,str(zoom))
      tiles = (xmax-xmin)*(ymax-ymin)
      stdscr.addstr(zoom+2,20,str(tiles))
      hours = tiles/3600/24.0
      stdscr.addstr(zoom+2,50,'%0.2f'%hours)
      if processed % 10 == 0:
               stdscr.addstr(zoom+2,10,"%d"%processed)
               stdscr.refresh()

      stdscr.refresh()

def test(region):

   set_up_target_db()
   download_world(region)

def append_metadata():
   set_up_target_db()
   set_metadata('world')
   
def download_world(region='world'):
   #record_bbox_debug_info(args.region)
   ocean, land, startx, starty, count, done = get_accumulators(bbox_zoom_start)

   # Open a WMTS source
   global src # the opened url for satellite images
   try:
      src = WMTS(url)
   except:
      print('failed to open source')
      sys.exit(1)
   # Skip over the tiles we alrady have 
   start = start_pd = time.time()
   for zoom in range(bbox_zoom_start,args.zoom+1):
      print("new zoom level:%s"%zoom)
      maxY = maxX = numTiles(args.zoom)

      ocean, land, startx, starty, count, done = get_accumulators(zoom)
      start_pd = time.time()

      for ytile in range(bbox_zoom_start+1,maxY+1):
         mbTiles.SetSatMetaData(zoom,'tileY',str(ytile))
         land_pd = land
         ocean_pd = ocean
         for xtile in range(bbox_zoom_start+1,maxX+1):
            if not mbTiles.TileExists(zoom,xtile,ytile):
               try:
                  r = src.get(zoom,xtile,ytile)
               except Exception as e:
                  print(str(e))
                  sys.exit(1)
               if r.status == 200:
                  raw = r.data
                  line = bytes(raw)
                  if line.find(b"DOCTYPE") != -1:
                     print('still getting html from sentinel cloudless')
                     continue
                  else:
                     try:
                        image = Image.open(BytesIO(raw))
                        #image.show(BytesIO(raw))
                     except Exception as e:
                        print('exception:%s'%e)
                        sys.exit()
                     #raw_input("PRESS ENTER")
                     try:
                        mbTiles.SetTile(zoom, xtile, ytile, r.data)
                     except Exception as e:
                        print('exception:%s'%e)
                        sys.exit()
                     land += 1
                     if land % 50 == 0:
                         print('+',flush=True,end="")
                     '''
                     returned = mbTiles.GetTile(zoom, tileX, tileY)
                     if bytearray(returned) != r.data:
                        print('read verify in replace_tile failed')
                        return False
                     return True
                     '''
               else:
                   print('status returned:%s X:%s  Y:%s'%(r.status,xtile,ytile))
            else:
                ocean += 1
                if ocean % 50 == 0:
                   print('.',flush=True,end="")
      print("ytile row completed:%s."%ytile)
      print('Total time:%s Total_tiles:%s'%(time.time()-start,land))
      # Print a summary of rate and activitys
      mbTiles.SetSatMetaData(zoom,'xtile',str(xtile))
      rate = (land - land_pd) / (time.time() - start_pd)
      start_pd = time.time()
      land_pd = land
      print('\nRate:%s Tiles::%s'%(rate,land,))
         
   print('zoom %s completed'%zoom)
   done = True
   put_accumulators(zoom,ocean,land,count,done)
   ocean, land, startx, starty, count, done = get_accumulators(zoom)
   print('Total time:%s Total_tiles:%s'%(time.time()-start,land))
   #mbTiles.delete_zoom(bbox_zoom_start-1)
   set_metadata(region)

def make_sat_extension(region):
   set_up_new_target_db(region)
   download_world(region)

def copy_and_extend(region):
   set_up_target_db(region)
   download_world(region)

def download(scr):
    global stdscr
    stdscr = scr
    k=0
    # Clear and refresh the screen for a blank canvas
    stdscr.clear()
    stdscr.refresh()

    # Start colors in curses
    curses.start_color()
    curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK)

    # Loop where k is the last character pressed
    while (k != ord('q')):
         #for region in regions.keys():

         download_region('central_america')
         # Refresh the screen
         stdscr.refresh()

         # Wait for next input
    k = stdscr.getch()


def main():
   global args
   global mbTiles
   if not os.path.isdir('./work'):
      os.mkdir('./work')
   get_config()
   args = parse_args()
   get_regions() # read the json region into global dictionary

   if not args.mbtiles: #remember current project in/out setup
      if config.get('last_src','') != '':
         args.mbtiles = config['last_src']
      else:  # fall back to symbolic link
         args.mbtiles = '%s/satellite.mbtiles'%os.getcwd()
   print(args.mbtiles)
   mbTiles  = MBTiles(args.mbtiles)
   bounds = mbTiles.get_bounds()
   print('SOURCE mbtiles filename:%s'%args.mbtiles)

   if args.summarize:
      mbTiles.summarize()
      sys.exit(0)
   if args.onetile:
      debug_one_tile()
      sys.exit(0)
   if args.list:
      list_tile_sizes()
      sys.exit(0)
   if args.lon and args.lat:
      if not args.zoom:
         args.zoom = 2
      print('inputs to tileXY: lat:%s lon:%s zoom:%s'%(args.lat,args.lon,args.zoom))
      args.x,args.y = tools.tileXY(args.lat,args.lon,args.zoom)
   if  args.get != None:
      print('get specified')
      set_url()
   if args.dir != None:
      to_dir()
      sys.exit(0)
   if args.region == None:
      args.region = 'world'
   if args.date == None:
      args.date = year
   if args.appendmd != None:
      append_metadata()
      sys.exit(0)

   test('san_jose')
   sys.exit()
   curses.wrapper(download) 
   

if __name__ == "__main__":
       if not os.path.isdir('./work'):
          os.mkdir('./work')
       now = datetime.now()
       year = now.strftime("%Y")

        # Run the main routine
       main()
