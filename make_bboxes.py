#!/usr/bin/env python3
# create spec for bounding boxes used in IIAB vector map subsets to stdout

from geojson import Feature, Point, FeatureCollection, Polygon
import geojson
import json
import os

CATALOG = './map-catalog.json'

def main():
    features = []
    map_catalog = {}
    with open(CATALOG,'r') as catalog_fp:
       try:
          data = json.loads(catalog_fp.read())
       except:
          print("json error reading map_ids.json")
          sys.exit(1)
       for map_id in data['maps'].keys():
            west = float(data['maps'][map_id]['west'])
            south = float(data['maps'][map_id]['south'])
            east = float(data['maps'][map_id]['east'])
            north = float(data['maps'][map_id]['north'])
            poly = Polygon([[[west,south],[east,south],[east,north],[west,north],[west,south]]])
            features.append(Feature(geometry=poly,properties={"name":map_id}))

       collection = FeatureCollection(features)
       outstr = geojson.dumps(collection, indent=2, sort_keys=True)
       print(outstr)

if __name__ == '__main__':
   main()
