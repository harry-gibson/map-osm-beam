#!/bin/bash
DIR=$1
ROOT=$(pwd)
for i in ${DIR}/* ; do
  if [ -d "$i" ]; then
    rm -r ${i}/*.csv
    rm -r ${i}/*.tif
    rm -r ${i}/*.vrt
    cat ${i}/road-class-* > ${i}/road-surface.csv
    cp road-surface.vrt ${i}/road-surface.vrt
    COUNTRY=$(basename "$i")
    cd ${i}
    gdal_rasterize -3d -tr 0.00833333 0.00833333 -a_nodata 0 -l road-surface road-surface.vrt ${ROOT}/${DIR}/${COUNTRY}.tif
    echo "${ROOT}"
    cd ${ROOT}
  fi
done