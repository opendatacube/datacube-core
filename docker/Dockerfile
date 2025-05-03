##
## This file is part of the Open Data Cube, see https://opendatacube.org for more information
##
## Copyright (c) 2015-2024 ODC Contributors
## SPDX-License-Identifier: Apache-2.0
##
# gdal:ubuntu-small no longer comes with netcdf support compiled into gdal
# syntax=docker/dockerfile:1
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.10.3 AS builder

ARG UV=https://github.com/astral-sh/uv/releases/download/0.7.2/uv-x86_64-unknown-linux-gnu.tar.gz
ADD --checksum=sha256:cfaab1b5166a6439ff66f020333d3a12bbdf622deee3b510718283e8f06c9de7 --chown=root:root --chmod=644 --link $UV uv.tar.gz

RUN tar xf uv.tar.gz -C /usr/local/bin --strip-components=1 --no-same-owner

FROM ghcr.io/osgeo/gdal:ubuntu-full-3.10.3

COPY --from=builder --link /usr/local/bin/uv* /usr/local/bin/

ARG V_PG=16
ARG V_PGIS=16-postgis-3

# Update and install Ubuntu packages

USER root
RUN apt-get update -y \
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

# Copy bootstrap script into image.
COPY --chown=root:root docker/assets/with_bootstrap /usr/local/bin/

# users and groups.
RUN groupmod ubuntu -n odc  \
  && usermod -m -d /home/odc ubuntu -l odc \
  && adduser odc users \
  && adduser odc sudo \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && mkdir /env /code \
  && chown odc:odc /env /code

# Build constrained python environment

USER odc

WORKDIR /env

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
COPY --chown=odc:odc . /code

RUN . /env/bin/activate && python3 -m pip install '/code/[all]' \
    && python3 -m pip install /code/examples/io_plugin \
    && python3 -m pip install /code/tests/drivers/fail_drivers

USER root

# prep db
RUN  install --owner postgres --group postgres -D -d /var/run/postgresql /srv/postgresql \
  && sudo -u postgres "$(find /usr/lib/postgresql/ -type f -name initdb)" -D "/srv/postgresql" --auth-host=md5 --encoding=UTF8

VOLUME /code
WORKDIR /code

ENTRYPOINT ["/bin/tini", "-s", "--", "/usr/local/bin/with_bootstrap"]
