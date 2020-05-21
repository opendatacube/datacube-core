ARG ORG=opendatacube
ARG V_BASE=-3.0.4
FROM ${ORG}/geobase:wheels${V_BASE} as env_builder

# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8

COPY requirements.txt /conf/requirements.txt
COPY ./dist/datacube-*.whl /conf/

RUN find /wheels -type f -name '*whl' > /tmp/constraints.txt \
  && find /conf -type f -name 'datacube-*.whl' | head -1 >> /tmp/constraints.txt \
  && mkdir -p /wheels-tmp \
  && echo "Constraints:" \
  && cat /tmp/constraints.txt \
  && echo "..." \
  && pip3 wheel \
     --no-cache \
     --no-cache-dir \
     --wheel-dir=/wheels-tmp \
     --find-links=/wheels/ \
     --find-links=/conf/ \
     --constraint=/tmp/constraints.txt \
     --requirement=/conf/requirements.txt \
  && rm /tmp/constraints.txt \
  && find /wheels-tmp/ -type f -name "datacube-*whl" -delete \
  && ls -lh /wheels-tmp/ \
  && rm -rf /root/.cache/pip \
  && echo "================================================================================" \
  && find /wheels-tmp/ -type f -name "*whl" > /tmp/reqs.txt \
# make env root:users with write permissions for group
  && (umask 002 \
  && mkdir -p /env \
  && chgrp users /env \
  && chmod g+s /env \
  && env-build-tool new_no_index /tmp/reqs.txt /env /wheels-tmp ) \
  && rm -rf /wheels-tmp \
  && rm -rf /root/.cache/pip \
  && echo done


#################################################################################
# Runner stage
#################################################################################
FROM opendatacube/geobase:runner${V_BASE}

# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends \
  # datacube tests need redis
  redis-server \
  # include db for running tests
  postgresql \
  postgresql-client-10 \
  postgresql-10 \
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
  && sudo -u postgres "/usr/lib/postgresql/10/bin/initdb" -D "/srv/postgresql" --auth-host=md5 --encoding=UTF8

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
