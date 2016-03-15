#!/bin/bash

source /etc/bashrc

NTASKS=$#
NNODES=$((PBS_NCPUS/NCPUS))
OFFSET=1
NODEID=0

while [ $NTASKS -gt 0 ]
do
  SLICE=$(((NTASKS+NNODES-1)/NNODES))
  DOCS=${@:$OFFSET:$SLICE}

  pbsdsh -n $NODEID -- /bin/bash -c "source /etc/bashrc;\
module use -a /g/data/v10/public/modules/modulefiles;\
module load agdc-py2-prod;\
datacube-ingest -v ingest --executor multiproc $NCPUS $DOCS;" &
  
  OFFSET=$((OFFSET+SLICE))
  NTASKS=$((NTASKS-SLICE))
  NNODES=$((NNODES-1))
  NODEID=$((NODEID+NCPUS))
done

wait

