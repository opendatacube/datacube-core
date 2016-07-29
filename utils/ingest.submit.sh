#!/usr/bin/env bash

source /etc/bashrc
module use -a /g/data/v10/public/modules/modulefiles
module load agdc-py2-prod

SCHEDULER_NODE=`sed '1q;d' $PBS_NODEFILE`
SCHEDULER_PORT=45454
SCHEDULER_ADDR=$SCHEDULER_NODE:$SCHEDULER_PORT

pbsdsh -n 0 -- /bin/bash -c "source /etc/bashrc;\
module use -a /g/data/v10/public/modules/modulefiles;\
module load agdc-py2-prod;\
dscheduler --port $SCHEDULER_PORT"&
sleep 5s

for ((i=0; i<PBS_NCPUS; i+=NCPUS)); do
  pbsdsh -n $i -- /bin/bash -c "source /etc/bashrc;\
module use -a /g/data/v10/public/modules/modulefiles;\
module load agdc-py2-prod;\
dworker $SCHEDULER_ADDR --nprocs 1 --nthreads 1"&
done
sleep 5s

datacube-ingest -v ingest --executor distributed $SCHEDULER_ADDR "$@"
