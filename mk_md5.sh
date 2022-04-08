#!/bin/bash 
# insure that every *.mbtiles file in PREFIX has a md5sum file alongside

PREFIX=/hd/maps/maps-2020/staged
for f in $PREFIX/*.mbtiles;do
   if [ ! -f $PREFIX.md5 ]; then
      echo Executing md5sum $f \> $f.md5
      md5sum $f \> $f.md5
   fi
done

