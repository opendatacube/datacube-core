#!/bin/bash

source /etc/bashrc
module use -a /g/data/v10/public/modules/modulefiles
module load agdc-py2-prod
datacube-ingest -v stack --executor multiproc $NCPUS "$@"

