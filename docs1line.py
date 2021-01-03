#!/usr/bin/env python2
# print the second line in each file in this directory to stdout
import glob
import os
#import pdb;pdb.set_trace()

flist = glob.glob("./*")
for f in sorted(flist):
   if f[-1:] == '~': continue
   lineno = 1
   if os.path.isfile(f):
      with open(f,"r") as fd:
         lines = fd.readlines()
         if len(lines) < 2: continue
         if len(lines[lineno]) == 0 or lines[lineno][0] != '#': 
            lineno += 1
            continue
         if lineno > 4: continue
         print("%20s -- %s"%(f,lines[lineno][:-1]))

