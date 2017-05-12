#!/bin/bash

PORT=9993
ADDR=localhost:$PORT

datacube-worker --executor celery $ADDR --nprocs 2 > worker.out.txt 2> worker.err.txt &
python dummy_task_app.py --executor celery $ADDR --app-config dummy_cfg.yml
