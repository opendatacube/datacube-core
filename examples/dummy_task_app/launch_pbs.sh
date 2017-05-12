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


pbs_check_or_exit () {
    if [ -z "$PBS_JOBID" ]; then
        cat <<EOF
** WARNING **
**
** This script can only run under PBS, use qsub to submit a job.
**
** WARNING **
EOF
        exit 1
    fi

    #bring in invocation shell env vars in
    export PATH=$PBS_O_PATH
    export LANG=$PBS_O_LANG
}


pbs_display_info () {
    cat <<EOF
** ENVIRONMENT **
Task Queue Mode   : $QMODE
PythonEnv         : $PYENV
Port              : $PORT
ADDR              : $ADDR

Total cores ${PBS_NCPUS} with ${NCPUS} per node.

List of nodes:
$(uniq < $PBS_NODEFILE)
.

PWD: $PWD
PATH: $PATH
"================================================================================"
$(env)
"================================================================================"
EOF
}

pbs_launch_workers () {

    _pbs_worker_runner () {
        # this function runs on distributed nodes via pbdsh, it doesn't have access
        # to the same environment as the rest of the script.

        export LANG=$PBS_O_LANG; #python strings complain otherwise

        activate=$1;
        env=$2;

        shift 2
        source "${activate}" "${env}"

        exec $@
    }

    ff=$(declare -f _pbs_worker_runner | base64 -w 0)

    for ((i=0; i<PBS_NCPUS; i+=NCPUS)); do
        # For main node use all CPUs but 2, but at least 1
        # For other nodes use all CPUs

        ncores=$(( $i>0?$NCPUS: ($NCPUS>2?$NCPUS-2:1) ))
        cmd="datacube-worker --executor $QMODE $ADDR --nprocs $ncores"

        pbsdsh -n $i -- /bin/bash -c "eval \"\$(echo $ff|base64 -d)\"; _pbs_worker_runner $(which activate) $PYENV $cmd" &
    done
}

pbs_check_or_exit
pbs_display_info
pbs_launch_workers

source activate $PYENV
cd ${PBS_O_WORKDIR}
./dummy_task_app.py --executor $QMODE localhost:$PORT --app-config dummy_cfg.yml
