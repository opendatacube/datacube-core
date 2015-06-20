#!/bin/bash
# launch_gdf2gdf.sh

# script is in utils directory under gdf root dir
gdf_root=$(dirname $(readlink -e $(dirname $0)))
echo gdf_root = ${gdf_root}

level='PQA'
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
            storage_type='LS5TMPQ'
            satellite='LS5'
            sensors='TM'
            echo qsub -v gdf_root=${gdf_root},storage_type=${storage_type},satellite=${satellite},sensors=${sensors},level=${level},xmin=${xmin},xmax=${xmax},ymin=${ymin},ymax=${ymax},tmin=${t},tmax=${t} ${gdf_root}/utils/agdc2gdf_qsub.sh
            qsub -v gdf_root=${gdf_root},storage_type=${storage_type},satellite=${satellite},sensors=${sensors},level=${level},xmin=${xmin},xmax=${xmax},ymin=${ymin},ymax=${ymax},tmin=${t},tmax=${t} ${gdf_root}/utils/agdc2gdf_qsub.sh

            storage_type='LS7ETMPQ'
            satellite='LS7'
            sensors='ETM+'
            echo qsub -v gdf_root=${gdf_root},storage_type=${storage_type},satellite=${satellite},sensors=${sensors},level=${level},xmin=${xmin},xmax=${xmax},ymin=${ymin},ymax=${ymax},tmin=${t},tmax=${t} ${gdf_root}/utils/agdc2gdf_qsub.sh
            qsub -v gdf_root=${gdf_root},storage_type=${storage_type},satellite=${satellite},sensors=${sensors},level=${level},xmin=${xmin},xmax=${xmax},ymin=${ymin},ymax=${ymax},tmin=${t},tmax=${t} ${gdf_root}/utils/agdc2gdf_qsub.sh

            storage_type='LS8OLIPQ'
            satellite='LS8'
            sensors='OLI\,OLI_TIRS'
            echo qsub -v gdf_root=${gdf_root},storage_type=${storage_type},satellite=${satellite},sensors=${sensors},level=${level},xmin=${xmin},xmax=${xmax},ymin=${ymin},ymax=${ymax},tmin=${t},tmax=${t} ${gdf_root}/utils/agdc2gdf_qsub.sh
            qsub -v gdf_root=${gdf_root},storage_type=${storage_type},satellite=${satellite},sensors=${sensors},level=${level},xmin=${xmin},xmax=${xmax},ymin=${ymin},ymax=${ymax},tmin=${t},tmax=${t} ${gdf_root}/utils/agdc2gdf_qsub.sh
        done
#    done
#done
