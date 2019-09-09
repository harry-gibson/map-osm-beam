#!/bin/bash
export GOOGLE_APPLICATION_CREDENTIALS="/home/cavargasru/Documents/map-osm-accessibility/map-visualization-dev-dataflow.json"
input=$1
while IFS= read -r line
do
 REGION=$line
 COUNTRY="$(echo ${line} | cut -d"/" -f2)"
 echo "${COUNTRY}"
 mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=uk.ac.ox.map.osm.OSMToRoadClassificationSurface -Dexec.args="--project=map-visualization-dev --stagingLocation=gs://map-osm/staging/ --inputFile=gs://map-osm/inputs/${REGION}/${COUNTRY}-latest.osm.bz2 --output=gs://map-osm/outputs/${REGION}/road-class --region=europe-west1 --workerMachineType=n1-standard-1 --maxNumWorkers=8 --numWorkers=8 --experiments=shuffle_mode=service --runner=DataflowRunner"
done < "$input"

