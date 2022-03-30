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
         #<<<<<<< HEAD
         if len(lines) < 2 : continue
         if len(lines[1])== 0: continue
         if lines[1][0] != '#': continue
         print("%8s -- %s"%(f,lines[1][:-1]))
         '''
         if len(lines) == 0: continue
         if len(lines[lineno]) == 0 or lines[lineno][0] != '#': 
            lineno += 1
            continue
         print("%s -- %s"%(f,lines[lineno][:-1]))
         '''

