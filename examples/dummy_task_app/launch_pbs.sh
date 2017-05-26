#!/bin/bash

#PBS -P u46
#PBS -q express
#PBS -l walltime=00:02:00
#PBS -l mem=63GB
#PBS -l ncpus=32


QMODE=${1:-${QMODE:-celery}} # celery|distributed|dask(==distributed)
PYENV=${2:-${PYENV:-agdc}}
PORT=${3:-${PORT:-3333}}

ADDR=${HOSTNAME}:$PORT

# start common part
source pbs_helpers.sh
pbs_check_or_exit
pbs_display_info
pbs_launch_workers
# end common part

# script specific part
source activate $PYENV
cd ${PBS_O_WORKDIR}

./dummy_task_app.py --executor $QMODE localhost:$PORT --app-config dummy_cfg.yml
