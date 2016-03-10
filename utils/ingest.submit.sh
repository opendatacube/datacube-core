#!/bin/bash

source /etc/bashrc

NTASKS=$#
NNODES=$((PBS_NCPUS/NCPUS))
OFFSET=1
NODEID=0

while [ $NTASKS -gt 0 ]
do
  SLICE=$(((NTASKS+NNODES-1)/NNODES))
  ARGS=${@:$OFFSET:$SLICE}
  
  pbsdsh -n $NODEID -- /bin/bash -c "source /etc/bashrc;\
module use -a /g/data/v10/public/modules/modulefiles;\
module load agdc-py2-dev;\
export DATACUBE_CONFIG_PATH=~/.datacube.conf;\
datacube-ingest -v ingest --executor multiproc $NCPUS $ARGS;" &
  
  OFFSET=$((OFFSET+SLICE))
  NTASKS=$((NTASKS-SLICE))
  NNODES=$((NNODES-1))
  NODEID=$((NODEID+NCPUS))
done

wait

