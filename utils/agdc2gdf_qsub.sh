#!/bin/bash
#@#PBS -P v10
#PBS -P r78
#PBS -q normal
#PBS -l walltime=12:00:00,mem=2GB,ncpus=1
#PBS -l wd
#@#PBS -m e
#PBS -M alex.ip@ga.gov.au

umask u=rwx,g=rwx,o=rx

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH
module load SQLAlchemy
module load netCDF4/1.0.7
module load python/2.7.3
module load pyephem/3.7.5.1
module load numexpr/2.2
module load psycopg2/2.5.1
module load gdal/1.10.1
module load eo-tools/0.4
export PYTHONPATH=${gdf_root}:$PYTHONPATH

python ${gdf_root}/utils/agdc2gdf.py --config=${config} --storage_type=${storage_type} --satellite=${satellite} --sensors=${sensors} --level=${level} --xmin=${xmin} --xmax=${xmax} --ymin=${ymin} --ymax=${ymax} --tmin=${tmin} --tmax=${tmax} --temp_dir=${temp_dir} --debug
