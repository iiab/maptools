# maptools
  ./check_catalog.py -- # read map_catalog.json and write map_catalog.json to stdout
      ./docs1line.py -- # print the second line in each file in this directory to stdout
    ./make_bboxes.py -- # create spec for bounding boxes used in IIAB vector map subsets to stdout
     ./mbdir2bbox.py -- # Read all mbtiles in curdir. Write bboxes.geojson to ./output/bboxes.geojson
    ./mbtile2bbox.py -- # Read a mbtiles sqlite3 database, report bbox for given zoom
     ./merge_regions -- # Combine vector data in mbtiles format
          ./mkcsv.py -- # create csv file as expected by openmaptiles/extracts
