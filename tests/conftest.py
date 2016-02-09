from __future__ import print_function, absolute_import

import pytest

'''
py.test configuration plugin

This module defines any fixtures or other extensions to py.test to be used throughout the
tests in this and sub packages.
'''


@pytest.fixture
def example_gdal_path(request):
    """Return the pathname of a sample geotiff file

    Use this fixture by specifiying an argument named 'example_gdal_path' in your
    test method.
    """
    return str(request.fspath.dirpath('data/sample_tile_151_-29.tif'))


@pytest.fixture
def example_netcdf_path(request):
    return str(request.fspath.dirpath('data/sample_tile.nc'))


@pytest.fixture
def tmpnetcdf_filename(tmpdir):
    filename = str(tmpdir.join('testfile_np.nc'))
    return filename
