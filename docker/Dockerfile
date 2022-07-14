##
## This file is part of the Open Data Cube, see https://opendatacube.org for more information
##
## Copyright (c) 2015-2020 ODC Contributors
## SPDX-License-Identifier: Apache-2.0
##
ARG V_PG=12
FROM osgeo/gdal:ubuntu-small-3.4.1

# Update and install Ubuntu packages

USER root
RUN apt-get update -y \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --allow-change-held-packages --fix-missing --no-install-recommends \
        libpq-dev libudunits2-dev \
        git \
        python3-dev python3-distutils python3-pip \
        build-essential \
        postgresql \
        redis-server \
        postgresql-client-${V_PG} \
        postgresql-${V_PG} \
        sudo make graphviz \
        tini \
    && apt-get clean

# Build constrained python environment

# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8

COPY docker/constraints.in /conf/requirements.txt
COPY docker/constraints.txt docker/nobinary.txt /conf/


RUN python3 -m pip install \
       -r /conf/requirements.txt \
       -c /conf/constraints.txt

# Copy datacube-core source code into container and install from source (with addons for tests).
COPY . /code

RUN python3 -m pip install '/code/[all]' \
    && python3 -m pip install /code/examples/io_plugin \
    && python3 -m pip install /code/tests/drivers/fail_drivers

# Copy bootstrap script into image.
COPY docker/assets/with_bootstrap /usr/local/bin/

# prep db
RUN  install --owner postgres --group postgres -D -d /var/run/postgresql /srv/postgresql \
  && sudo -u postgres "$(find /usr/lib/postgresql/ -type f -name initdb)" -D "/srv/postgresql" --auth-host=md5 --encoding=UTF8

# users and groups.
RUN groupadd --gid 1000 odc \
  && useradd --gid 1000 \
  --uid 1000 \
  --create-home \
  --shell /bin/bash -N odc \
  && adduser odc users \
  && adduser odc sudo \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && install -d -o odc -g odc /env \
  && install -d -o odc -g odc /code \
  && true

USER root
VOLUME /code
WORKDIR /code

ENTRYPOINT ["/bin/tini", "-s", "--", "/usr/local/bin/with_bootstrap"]