#!/bin/bash

#PBS -P u46
#PBS -q express
#PBS -l walltime=00:02:00
#PBS -l mem=63GB
#PBS -l ncpus=32

set -e

QMODE=${1:-${QMODE:-celery}} # celery|distributed|dask(==distributed)
PYENV=${2:-${PYENV:-agdc}}
PORT=${3:-${PORT:-3333}}

ADDR=${HOSTNAME}:$PORT

# start common part
[ -z "$PBS_O_PATH" ] || export PATH="$PBS_O_PATH"
source activate $PYENV
source pbs_helpers.sh

pbs_check_or_exit
pbs_display_info
pbs_launch_workers
# end common part

# script specific part
cd ${PBS_O_WORKDIR}
./dummy_task_app.py --executor $QMODE localhost:$PORT --app-config dummy_cfg.yml
