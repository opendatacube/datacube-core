FROM ubuntu:16.04
# This Dockerfile should follow the Travis configuration process
# available here: https://github.com/opendatacube/datacube-core/blob/develop/.travis.yml

# First add the NextGIS repo
RUN apt-get update && apt-get install -y --no-install-recommends \
    python-software-properties \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

RUN add-apt-repository ppa:nextgis/ppa

# And now install apt dependencies, including a few of the heavy Python projects
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Core requirements from travis.yml
    gdal-bin libgdal-dev libgdal20 libudunits2-0 \
    # Extra python components, to speed things up
    python3 python3-setuptools python3-dev python3-numpy python3-netcdf4 \
    # Need pip to install more python packages later
    python3-pip \
    # G++ because GDAL decided it needed compiling
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Get the code, and put it in /code
ENV APPDIR=/tmp/code
RUN mkdir -p $APPDIR
COPY . $APPDIR
WORKDIR $APPDIR

# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8
# And some GDAL variables
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install dependencies. First update pip
RUN pip3 install --upgrade pip setuptools wheel \
    && rm -rf $HOME/.cache/pip

# Install psycopg2 as a special case, to quiet the warning message 
RUN pip3 install --no-cache --no-binary :all: psycopg2 \
    && rm -rf $HOME/.cache/pip

# Now use the setup.py file to identify dependencies
RUN pip3 install '.[test,celery,s3]' --upgrade \
    && rm -rf $HOME/.cache/pip

# Install ODC
RUN python3 setup.py install

# Move docs and utils somewhere else, and remove the temp folder
RUN mkdir -p /opt/odc \
    && chmod +rwx /opt/odc \
    && mv $APPDIR/utils /opt/odc/ \
    && mv $APPDIR/docs /opt/odc/ \
    && mv $APPDIR/docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh \
    && rm -rf $APPDIR

# Set up an entrypoint that drops environment variables into the config file
ENTRYPOINT ["docker-entrypoint.sh"]

WORKDIR /opt/odc
CMD ["datacube","--help"]
