FROM ubuntu:16.04
# This Dockerfile should follow the Travis configuration process
# available here: https://github.com/opendatacube/datacube-core/blob/develop/.travis.yml

# First add the NextGIS repo
RUN apt-get update && apt-get install -y \
    python-software-properties \
    software-properties-common

RUN add-apt-repository ppa:nextgis/ppa

# And now install apt dependencies, including a few of the heavy Python projects
RUN apt-get update && apt-get install -y \
    gdal-bin libgdal-dev libgdal20 libudunits2-0 \
    python3 python3-gdal python3-setuptools python3-dev python3-numpy python3-netcdf4 \
    python3-pip

# Install psychopg2 as a special case, to quiet the warning message 
RUN pip3 install --no-cache --no-binary :all: psycopg2

# Get the code, and put it in /code
ENV APPDIR=/code
RUN mkdir -p $APPDIR
COPY . $APPDIR

# Install the dependencies
WORKDIR $APPDIR

# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8

# Install dependencies
RUN pip3 install '.[test,analytics,celery,s3]' --upgrade
RUN pip3 install ./tests/drivers/fail_drivers --no-deps --upgrade

# Install ODC
RUN python3 setup.py develop

CMD datacube --help
