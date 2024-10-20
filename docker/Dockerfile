##
## This file is part of the Open Data Cube, see https://opendatacube.org for more information
##
## Copyright (c) 2015-2024 ODC Contributors
## SPDX-License-Identifier: Apache-2.0
##
# gdal:ubuntu-small no longer comes with netcdf support compiled into gdal
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.9.3 AS builder
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.9.3
ARG V_PG=16
ARG V_PGIS=16-postgis-3

# Update and install Ubuntu packages

USER root
RUN apt-get update -y \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --allow-change-held-packages --fix-missing --no-install-recommends \
        git \
        libpq-dev libudunits2-dev libproj-dev \
        libhdf5-dev libnetcdf-dev libgeos-dev libudunits2-dev \
        python3-dev virtualenv \
        build-essential \
        postgresql \
        redis-server \
        postgresql-client-${V_PG} \
        postgresql-${V_PG} \
        postgresql-${V_PGIS} \
        sudo make graphviz \
        tini \
    && apt-get clean

# Build constrained python environment

RUN virtualenv /env
# Set the locale, this is required for some of the Python packages
ENV PYENV=/env \
    GDAL_CONFIG=/usr/bin/gdal-config \
    LC_ALL=C.UTF-8

# Needed to build cf-units wheels.
ARG UDUNITS2_XML_PATH=/usr/share/xml/udunits/udunits2-common.xml

COPY docker/constraints.in /conf/requirements.txt
COPY docker/constraints.txt docker/nobinary.txt /conf/


RUN . /env/bin/activate && python3 -m pip install --upgrade pip setuptools
RUN . /env/bin/activate && python3 -m pip install -r /conf/requirements.txt \
                           -c /conf/constraints.txt \
                           -c /conf/nobinary.txt

# Copy datacube-core source code into container and install from source (with addons for tests).
COPY . /code

RUN . /env/bin/activate && python3 -m pip install '/code/[all]' \
    && python3 -m pip install /code/examples/io_plugin \
    && python3 -m pip install /code/tests/drivers/fail_drivers

# Copy bootstrap script into image.
COPY docker/assets/with_bootstrap /usr/local/bin/

# prep db
RUN  install --owner postgres --group postgres -D -d /var/run/postgresql /srv/postgresql \
  && sudo -u postgres "$(find /usr/lib/postgresql/ -type f -name initdb)" -D "/srv/postgresql" --auth-host=md5 --encoding=UTF8

# users and groups.
RUN groupmod ubuntu -n odc  \
  && usermod ubuntu -l odc \
  && adduser odc users \
  && adduser odc sudo \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && chown -R odc:odc /env \
  && true

USER root
VOLUME /code
WORKDIR /code

ENTRYPOINT ["/bin/tini", "-s", "--", "/usr/local/bin/with_bootstrap"]
