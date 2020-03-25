"""
py.test configuration fixtures

This module defines any fixtures or other extensions to py.test to be used throughout the
tests in this and sub packages.
"""

import os

import numpy as np
import pytest
import xarray
from affine import Affine

from datacube import Datacube
from datacube.utils import geometry
from datacube.model import Measurement


AWS_ENV_VARS = ("AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN"
                "AWS_DEFAULT_REGION AWS_DEFAULT_OUTPUT AWS_PROFILE "
                "AWS_ROLE_SESSION_NAME AWS_CA_BUNDLE "
                "AWS_SHARED_CREDENTIALS_FILE AWS_CONFIG_FILE").split(" ")


@pytest.fixture
def example_gdal_path(data_folder):
    """Return the pathname of a sample geotiff file

    Use this fixture by specifiying an argument named 'example_gdal_path' in your
    test method.
    """
    return str(os.path.join(data_folder, 'sample_tile_151_-29.tif'))


@pytest.fixture
def no_crs_gdal_path(data_folder):
    """Return the pathname of a GDAL file that doesn't contain a valid CRS."""
    return str(os.path.join(data_folder, 'no_crs_ds.tif'))


@pytest.fixture
def data_folder():
    """Return a string path to the location `test/data`"""
    return os.path.join(os.path.split(os.path.realpath(__file__))[0], 'data')


@pytest.fixture
def example_netcdf_path(request):
    """Return a string path to `sample_tile.nc` in the test data dir"""
    return str(request.fspath.dirpath('data/sample_tile.nc'))


netcdf_num = 1


@pytest.fixture
def tmpnetcdf_filename(tmpdir):
    """Return a generated filename for a non-existant netcdf file"""
    global netcdf_num
    filename = str(tmpdir.join('testfile_np_%s.nc' % netcdf_num))
    netcdf_num += 1
    return filename


@pytest.fixture
def odc_style_xr_dataset():
    """An xarray.Dataset with ODC style coordinates and CRS, and no time dimension.

    Contains an EPSG:4326, single variable 'B10' of 100x100 int16 pixels."""
    affine = Affine.scale(0.1, 0.1) * Affine.translation(20, 30)
    geobox = geometry.GeoBox(100, 100, affine, geometry.CRS(GEO_PROJ))

    return Datacube.create_storage({}, geobox, [Measurement(name='B10', dtype='int16', nodata=0, units='1')])


@pytest.fixture
def without_aws_env(monkeypatch):
    for e in AWS_ENV_VARS:
        monkeypatch.delenv(e, raising=False)


GEO_PROJ = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],' \
           'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],' \
           'AUTHORITY["EPSG","4326"]]'


@pytest.fixture(scope="module")
def dask_client():
    from distributed import Client
    client = Client(processes=False,
                    threads_per_worker=1,
                    dashboard_address=None)
    yield client
    client.close()
    del client
