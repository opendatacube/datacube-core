# THIS IS DEPRECATED see issue #777
FROM ubuntu:18.04

# First add the UbuntuGIS repo
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

RUN add-apt-repository ppa:ubuntugis/ppa

# And now install apt dependencies, including a few of the heavy Python projects
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Core requirements from travis.yml
    gdal-bin gdal-data libgdal-dev libgdal20 libudunits2-0 \
    python3 python3-setuptools python3-dev python3-pip python3-wheel \
    # Git to work out the ODC version number
    git \
    g++ \
    # compliance-checker
    libudunits2-dev \
    && rm -rf /var/lib/apt/lists/*

# Ensure base python packages are fresher than what ubuntu offers
RUN pip3 install --no-cache-dir --upgrade pip \
  && hash -r \
  && pip install --no-cache-dir --upgrade cython numpy\
  && rm -rf $HOME/.cache/pip

# GDAL python binding are special
RUN pip3 install --no-cache-dir GDAL==$(gdal-config --version)


# Get the code, and put it in /code
ENV APPDIR=/tmp/code
RUN mkdir -p $APPDIR
COPY . $APPDIR
WORKDIR $APPDIR

# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8

# Install ODC
RUN pip install '.[all]' --no-cache-dir --no-binary psycopg2 \
&& rm -rf $HOME/.cache/pip

# Move docs and utils somewhere else, and remove the temp folder
RUN mkdir -p /opt/odc \
    && chmod +rwx /opt/odc \
    && mv $APPDIR/utils /opt/odc/ \
    && mv $APPDIR/docs /opt/odc/ \
    && rm -rf $APPDIR

# Fix an issue with libcurl shipped in rasterio
RUN mkdir -p /etc/pki/tls/certs \
    && ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt;

WORKDIR /opt/odc
CMD ["datacube","--help"]
