#!/bin/bash
# launch_gdf2gdf.sh

script_dir=$(readlink -e $(dirname $0))
echo script_dir = ${script_dir}

storage_type='LS7ETM'
satellite='LS7'
sensor='ETM+'
level='NBAR'
xmin=139
xmax=141
ymin=-37
ymax=-35
tmin=1987
tmax=2015

#for x in $(seq ${xmin} ${xmax})
#do
#    for y in ${ymin} ${ymax})
#    do
        for t in $(seq ${tmin} ${tmax})
        do
            qsub -v script_dir=${script_dir},storage_type=${storage_type},satellite=${satellite},sensor=${sensor},level=${level},xmin=${xmin},xmax=${xmax},ymin=${ymin},ymax=${ymax},tmin=${t},tmax=${t} ${script_dir}/agdc2gdf_qsub.sh
        done
#    done
#done
