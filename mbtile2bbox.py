#!/usr/bin/env  python3
# Read a mbtiles sqlite3 database, report bbox for given zoom

import os,sys
import json
import sqlite3

if len(sys.argv)< 3:
   print('Usage: %s <mbtiles filename> <zoom>'%sys.argv[0])
   sys.exit(1)

try:
   conn = sqlite3.connect(sys.argv[1])
   c = conn.cursor()
   sql = 'select value from metadata where name = "bounds"'
   c.execute(sql)
except Exception as e:
   print("ERROR -no access to metadata in region:%s"%region)
   print("Path:%s"%mbtile)
   print("sql error: %s"%e)
   sys.exit(1)
row = c.fetchone()
print(row[0])
row = c.fetchone()
