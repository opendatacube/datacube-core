##
## This file is part of the Open Data Cube, see https://opendatacube.org for more information
##
## Copyright (c) 2015-2024 ODC Contributors
## SPDX-License-Identifier: Apache-2.0
##
# gdal:ubuntu-small no longer comes with netcdf support compiled into gdal
# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.9.12@sha256:0eaa66c625730a3b13eb0b7bfbe085ed924b5dca6240b6f0632b4256cfb53f31 AS uv

FROM ghcr.io/osgeo/gdal:ubuntu-full-3.10.3@sha256:b50822a4059629f6a579908584e25e6adccfc70a57914290dd01217f19871d08 AS base

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

# Both the builder image and the regular image needs build tools, otherwise "uv sync"
# will fail.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        # For shapely with --no-binary.
        libgeos-dev \
        libhdf5-dev \
        libnetcdf-dev \
        libudunits2-dev \
        libproj-dev \
        # For psycopg2.
        libpq-dev \
        python3-dev

FROM base AS builder

ENV UV_COMPILE_BYTECODE=0 \
    UV_PROJECT_ENVIRONMENT=/app \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.12

WORKDIR /build

COPY --link --from=uv /uv /uvx /usr/local/bin/

COPY --link pyproject.toml uv.lock /build/

# Use a separate cache volume for uv on opendatacube projects, so it is
# not inseparable from pip/poetry/npm/etc. cache stored in /root/.cache.
RUN --mount=type=cache,id=opendatacube-uv-cache,target=/root/.cache \
    uv sync --locked --all-extras --group=dev --group=doc  --no-install-project \
      --no-binary-package fiona \
      --no-binary-package netcdf4 \
      --no-binary-package psycopg \
      --no-binary-package rasterio \
      --no-binary-package shapely

FROM base

COPY --from=builder --link /usr/local/bin/uv* /usr/local/bin/

ARG V_PG=18

# Update and install Ubuntu packages
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    export DEBIAN_FRONTEND=noninteractive \
    && apt-get -qq update \
    && apt-get upgrade -y \
    && apt-get -qq -y --no-install-recommends install dirmngr gpg-agent  > /dev/null \
    && export GNUPGHOME="$(mktemp -d)" \
    && pg_key="B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8" \
    && gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "$pg_key" \
    && gpg --batch --export --armor "$pg_key" > /etc/apt/keyrings/postgres.gpg.asc \
    && gpgconf --kill all \
    && echo "deb [signed-by=/etc/apt/keyrings/postgres.gpg.asc] http://apt.postgresql.org/pub/repos/apt noble-pgdg main $PG_VERSION" | tee /etc/apt/sources.list.d/postgres.list \
    && apt-get -qq update \
    && apt-get install -y --no-install-recommends \
        gosu \
        graphviz \
        less \
        # Required for Sphinx.
        pandoc \
        postgresql-client-${V_PG} \
        sudo \
        vim \
        tini

# Add login-script for UID/GID-remapping.
COPY --chown=root:root --link docker/assets/remap-user.sh /usr/local/bin/remap-user.sh

# users and groups.
RUN usermod -aG sudo ubuntu \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && mkdir /app /code \
  && chown ubuntu:ubuntu /app /code

# Build constrained python environment

USER ubuntu

ENV PATH=/app/bin:$PATH

COPY --chown=1000:1000 --link docker/assets/datacube_integration.conf /home/ubuntu/.datacube_integration.conf

COPY --from=builder --link --chown=1000:1000 /app /app

# Copy datacube-core source code into container and install from source (with addons for tests).
COPY --chown=1000:1000 --link . /code

WORKDIR /code

USER root

# Install as root to have the right permissions in cache, but chown result to ubuntu.
RUN  --mount=type=cache,id=opendatacube-uv-cache,target=/root/.cache \
    . /app/bin/activate \
    && export UV_COMPILE_BYTECODE=0 \
    && export UV_PROJECT_ENVIRONMENT=/app \
    && export UV_PYTHON_DOWNLOADS=never \
    && export UV_PYTHON=python3.12 \
    && uv pip install /code/ \
    && uv pip install /code/examples/io_plugin \
    && uv pip install /code/tests/drivers/fail_drivers \
    && loc=$(uv pip show dcio-example --files | grep Location | awk -F": " '{print $2}') \
    && chown -R ubuntu:ubuntu "$loc/dcio_example"* "$loc/dc_tests_io"*

ENTRYPOINT ["/usr/local/bin/remap-user.sh"]
CMD ["/bin/bash", "--login"]
