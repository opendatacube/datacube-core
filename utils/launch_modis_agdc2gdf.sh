#!/bin/bash
# launch_gdf2gdf.sh

# script is in utils directory under gdf root dir
gdf_root=$(dirname $(readlink -e $(dirname $0)))
echo gdf_root = ${gdf_root}

level='MOD09'
xmin=13
xmax=14
ymin=-4
ymax=-3
tmin=2010
tmax=2011
storage_type='MOD09'
satellite='MT'
sensors='MODIS-Terra'
temp_dir='/jobfs'
config="${gdf_root}/utils/agdc2gdf_modis.conf"

for x in $(seq ${xmin} ${xmax})
do
    for y in $(seq ${ymin} ${ymax})
    do
        for t in $(seq ${tmin} ${tmax})
        do
            echo qsub -v gdf_root=${gdf_root},config=${config},storage_type=${storage_type},satellite=${satellite},sensors=${sensors},level=${level},xmin=${x},xmax=${x},ymin=${y},ymax=${y},tmin=${t},tmax=${t},temp_dir=${temp_dir} ${gdf_root}/utils/agdc2gdf_qsub.sh
#            qsub -v gdf_root=${gdf_root},config=${config},storage_type=${storage_type},satellite=${satellite},sensors=${sensors},level=${level},xmin=${x},xmax=${x},ymin=${y},ymax=${y},tmin=${t},tmax=${t},temp_dir=${temp_dir} ${gdf_root}/utils/agdc2gdf_qsub.sh
        done
    done
done
