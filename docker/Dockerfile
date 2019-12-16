FROM buildpack-deps:bionic
# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# First add the UbuntuGIS repo
RUN apt-get update \
  && apt-get install -y --fix-missing --no-install-recommends \
  software-properties-common \
  && add-apt-repository ppa:ubuntugis/ppa \
  && rm -rf /var/lib/apt/lists/*


RUN apt-get update \
  && apt-get install -y --fix-missing --no-install-recommends \
  python3 \
  python3-dev \
  python3-pip \
  python3-venv \
  gdal-bin \
  gdal-data \
  proj-bin \
  proj-data \
  libgdal-dev \
  libgeos-dev \
  libproj-dev \
  libnetcdf-dev \
  libhdf4-alt-dev \
  libhdf5-serial-dev \
  libatlas-base-dev \
  gfortran \
  git \
  libudunits2-0 \
  libudunits2-dev \
  # datacube tests need redis
  redis-server \
  # include db for running tests
  postgresql \
  postgresql-client-10 \
  postgresql-10 \
  # to become test user
  sudo \
  # for debugging
  less \
  vim \
  && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache --no-cache-dir --upgrade \
         pip setuptools wheel cython numpy \
  && hash -r \
  && true

COPY requirements.txt /conf/requirements.txt

RUN mkdir -p /wheels \
  && echo "GDAL==$(gdal-config --version)" > /tmp/constraints.txt \
  && pip wheel \
  --no-cache \
  --no-cache-dir \
  --wheel-dir=/wheels \
  --constraint=/tmp/constraints.txt \
  --requirement=/conf/requirements.txt \
  && rm /tmp/constraints.txt \
  && find /wheels/ -type f -name "datacube-*whl" -delete \
  && ls -lh /wheels/ \
  && echo "================================================================================"


COPY assets/env-build-tool /usr/local/bin/

# build environment with all dependencies of datacube but no datacube
RUN find /wheels/ -type f -name "*whl" > /tmp/reqs.txt \
&& env-build-tool new /tmp/reqs.txt /wheels /env

# prep db
RUN  install --owner postgres --group postgres -D -d /var/run/postgresql /srv/postgresql \
  && sudo -u postgres "/usr/lib/postgresql/10/bin/initdb" -D "/srv/postgresql" --auth-host=md5 --encoding=UTF8

RUN groupadd --gid 1000 odc \
  && useradd --gid 1000 \
  --uid 1000 \
  --create-home \
  --shell /bin/bash -N odc \
  && adduser odc sudo \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && true


COPY assets/dc_config_render.py /usr/local/bin/
COPY assets/with_bootstrap /usr/local/bin/

VOLUME /src/datacube-core
WORKDIR /src/datacube-core

ENTRYPOINT ["/usr/local/bin/with_bootstrap"]
