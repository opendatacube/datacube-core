#!/bin/bash

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOF
  CREATE USER odc WITH SUPERUSER;
  CREATE DATABASE odc;
EOF
for db in "pgintegration" "pgisintegration"; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOF
    CREATE DATABASE $db;
    GRANT ALL PRIVILEGES ON DATABASE $db TO odc;
EOF
done
