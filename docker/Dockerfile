##
## This file is part of the Open Data Cube, see https://opendatacube.org for more information
##
## Copyright (c) 2015-2020 ODC Contributors
## SPDX-License-Identifier: Apache-2.0
##
ARG ORG=opendatacube
ARG V_BASE=3.3.0
ARG V_PG=12
FROM ${ORG}/geobase-builder:${V_BASE} as env_builder

# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8

COPY assets/mkenv /usr/bin/
COPY constraints.txt /conf/constraints.txt
COPY requirements.txt /conf/requirements.txt
COPY ./dist/datacube-*.whl /conf/

RUN mkenv \
  && rm -rf /tmp/wheels \
  && rm -rf /root/.cache/pip \
  && echo done

#################################################################################
# Runner stage
#################################################################################
FROM ${ORG}/geobase-runner:${V_BASE}

# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends --allow-change-held-packages \
  # datacube tests need redis
  redis-server \
  # include db for running tests
  postgresql \
  postgresql-client-${V_PG} \
  postgresql-${V_PG} \
  # to become test user
  sudo \
  # git is needed for sdist|bdist_wheel
  git \
  # for docs
  make \
  graphviz \
  && rm -rf /var/lib/apt/lists/*

# if using binary rasterio package, to support https
RUN mkdir -p /etc/pki/tls/certs \
&& ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt;

# prep db
RUN  install --owner postgres --group postgres -D -d /var/run/postgresql /srv/postgresql \
  && sudo -u postgres "$(find /usr/lib/postgresql/ -type f -name initdb)" -D "/srv/postgresql" --auth-host=md5 --encoding=UTF8

RUN groupadd --gid 1000 odc \
  && useradd --gid 1000 \
  --uid 1000 \
  --create-home \
  --shell /bin/bash -N odc \
  && adduser odc users \
  && adduser odc sudo \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && true

COPY --from=env_builder --chown=odc:users /env /env
COPY --from=env_builder /bin/tini /bin/tini

COPY assets/with_bootstrap /usr/local/bin/

VOLUME /src/datacube-core
WORKDIR /src/datacube-core

ENTRYPOINT ["/bin/tini", "-s", "--", "/usr/local/bin/with_bootstrap"]
