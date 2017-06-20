"""
py.test configuration fixtures

This module defines any fixtures or other extensions to py.test to be used throughout the
tests in this and sub packages.
"""
from __future__ import print_function, absolute_import

import os
import pytest


@pytest.fixture
def example_gdal_path(data_folder):
    """Return the pathname of a sample geotiff file

    Use this fixture by specifiying an argument named 'example_gdal_path' in your
    test method.
    """
    return str(os.path.join(data_folder, 'sample_tile_151_-29.tif'))


@pytest.fixture
def data_folder():
    return os.path.join(os.path.split(os.path.realpath(__file__))[0], 'data')


@pytest.fixture
def example_netcdf_path(request):
    return str(request.fspath.dirpath('data/sample_tile.nc'))

netcdf_num = 1


@pytest.fixture
def tmpnetcdf_filename(tmpdir):
    global netcdf_num
    filename = str(tmpdir.join('testfile_np_%s.nc' % netcdf_num))
    netcdf_num += 1
    return filename
