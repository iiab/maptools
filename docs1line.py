#!/usr/bin/env python2
# print the second line in each file in this directory to stdout
import glob
import os

flist = glob.glob("./*")
for f in sorted(flist):
   if f[-1:] == '~': continue
   if os.path.isfile(f):
      with open(f,"r") as fd:
         lines = fd.readlines()
         if len(lines) < 2 : continue
         if len(lines[1])== 0: continue
         if lines[1][0] != '#': continue
         print("%8s -- %s"%(f,lines[1][:-1]))

