##
## This file is part of the Open Data Cube, see https://opendatacube.org for more information
##
## Copyright (c) 2015-2024 ODC Contributors
## SPDX-License-Identifier: Apache-2.0
##
# gdal:ubuntu-small no longer comes with netcdf support compiled into gdal
# syntax=docker/dockerfile:1
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.10.3 AS base

FROM base AS builder

ARG UV=https://github.com/astral-sh/uv/releases/download/0.7.2/uv-x86_64-unknown-linux-gnu.tar.gz
ADD --checksum=sha256:cfaab1b5166a6439ff66f020333d3a12bbdf622deee3b510718283e8f06c9de7 --chown=root:root --chmod=644 --link $UV uv.tar.gz

RUN tar xf uv.tar.gz -C /usr/local/bin --strip-components=1 --no-same-owner

FROM base

COPY --from=builder --link /usr/local/bin/uv* /usr/local/bin/

ARG V_PG=16
ARG V_PGIS=16-postgis-3

# Update and install Ubuntu packages

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    export DEBIAN_FRONTEND=noninteractive \
    && apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
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
        tini

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

COPY docker/constraints.txt docker/nobinary.txt /conf/

RUN --mount=type=cache,id=odc-python-312-cache,target=/home/odc/.cache \
    sudo chown odc:odc /home/odc/.cache \
    && . /env/bin/activate \
    && python3 -m pip --disable-pip-version-check install --upgrade pip setuptools \
    && python3 -m pip --disable-pip-version-check install -r /conf/constraints.txt \
                           -c /conf/nobinary.txt

# Copy datacube-core source code into container and install from source (with addons for tests).
COPY --chown=odc:odc . /code

RUN  --mount=type=cache,id=odc-python-312-cache,target=/home/odc/.cache \
    . /env/bin/activate && python3 -m pip --disable-pip-version-check install '/code/[all]' \
    && python3 -m pip --disable-pip-version-check install /code/examples/io_plugin \
    && python3 -m pip --disable-pip-version-check install /code/tests/drivers/fail_drivers

USER root

# prep db
RUN  install --owner postgres --group postgres -D -d /var/run/postgresql /srv/postgresql \
  && sudo -u postgres "$(find /usr/lib/postgresql/ -type f -name initdb)" -D "/srv/postgresql" --auth-host=md5 --encoding=UTF8

VOLUME /code
WORKDIR /code

ENTRYPOINT ["/bin/tini", "-s", "--", "/usr/local/bin/with_bootstrap"]
