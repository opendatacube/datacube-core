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

ARG UV=https://github.com/astral-sh/uv/releases/download/0.7.13/uv-x86_64-unknown-linux-gnu.tar.gz
ADD --checksum=sha256:909278eb197c5ed0e9b5f16317d1255270d1f9ea4196e7179ce934d48c4c2545 --chown=root:root --chmod=644 --link $UV uv.tar.gz

RUN tar xf uv.tar.gz -C /usr/local/bin --strip-components=1 --no-same-owner

FROM base

COPY --from=builder --link /usr/local/bin/uv* /usr/local/bin/

ARG V_PG=16

# Update and install Ubuntu packages

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    export DEBIAN_FRONTEND=noninteractive \
    && apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        git \
        gosu \
        libpq-dev libudunits2-dev libproj-dev \
        libhdf5-dev libnetcdf-dev libgeos-dev libudunits2-dev \
        python3-dev virtualenv \
        build-essential \
        postgresql-client-${V_PG} \
        sudo make graphviz \
        tini

# Copy bootstrap script into image.
COPY --chown=root:root docker/assets/with_bootstrap /usr/local/bin/
# Add login-script for UID/GID-remapping.
COPY --chown=root:root --link docker/assets/remap-user.sh /usr/local/bin/remap-user.sh

# users and groups.
RUN usermod -aG sudo ubuntu \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && mkdir /env /code \
  && chown ubuntu:ubuntu /env /code

# Build constrained python environment

USER ubuntu

COPY --chown=ubuntu:ubuntu docker/assets/datacube_integration.conf /home/ubuntu/.datacube_integration.conf

WORKDIR /env

RUN virtualenv /env
# Set the locale, this is required for some of the Python packages
ENV PYENV=/env \
    GDAL_CONFIG=/usr/bin/gdal-config \
    LC_ALL=C.UTF-8

COPY docker/constraints.txt docker/nobinary.txt /conf/

RUN --mount=type=cache,id=odc-python-312-cache,target=/home/ubuntu/.cache \
    sudo chown ubuntu:ubuntu /home/ubuntu/.cache \
    && . /env/bin/activate \
    && python3 -m pip --disable-pip-version-check install --upgrade pip setuptools \
    && python3 -m pip --disable-pip-version-check install -r /conf/constraints.txt \
                           -c /conf/nobinary.txt

# Copy datacube-core source code into container and install from source (with addons for tests).
COPY --chown=ubuntu:ubuntu . /code

RUN  --mount=type=cache,id=odc-python-312-cache,target=/home/ubuntu/.cache \
    . /env/bin/activate && python3 -m pip --disable-pip-version-check install '/code/[all]' \
    && python3 -m pip --disable-pip-version-check install /code/examples/io_plugin \
    && python3 -m pip --disable-pip-version-check install /code/tests/drivers/fail_drivers

USER root

VOLUME /code
WORKDIR /code

ENTRYPOINT ["/usr/local/bin/remap-user.sh"]
CMD ["/usr/local/bin/with_bootstrap"]
