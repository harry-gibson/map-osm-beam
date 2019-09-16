#!/bin/bash
REGION=$1
COUNTRY=$2
echo $REGION
wget http://download.geofabrik.de/$REGION-latest.osm.bz2
gsutil -m cp $COUNTRY-latest.osm.bz2 gs://map-osm/inputs/$REGION/
rm -r $COUNTRY-latest.osm.bz2