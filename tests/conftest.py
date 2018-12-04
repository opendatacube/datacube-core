"""
py.test configuration fixtures

This module defines any fixtures or other extensions to py.test to be used throughout the
tests in this and sub packages.
"""
from __future__ import print_function, absolute_import

import os

import numpy as np
import pytest
import xarray
from affine import Affine

from datacube.utils import geometry


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
def sample_document_files(data_folder):
    files = [('multi_doc.yml', 3),
             ('multi_doc.yml.gz', 3),
             ('multi_doc.nc', 3),
             ('single_doc.yaml', 1),
             ('sample.json', 1)]

    files = [(str(os.path.join(data_folder, f)), num_docs)
             for f, num_docs in files]

    return files


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
    dataset = xarray.Dataset(attrs={'extent': geobox.extent, 'crs': geobox.crs})
    for name, coord in geobox.coordinates.items():
        dataset[name] = (name, coord.values, {'units': coord.units, 'crs': geobox.crs})
    dataset['B10'] = (geobox.dimensions,
                      np.arange(10000, dtype='int16').reshape(geobox.shape),
                      {'nodata': 0, 'units': '1', 'crs': geobox.crs})

    # To include a time dimension:
    # dataset['B10'] = (('time', 'latitude', 'longitude'), np.arange(10000, dtype='int16').reshape((1, 100, 100)),
    #  {'nodata': 0, 'units': '1', 'crs': geobox.crs})

    return dataset


GEO_PROJ = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],' \
           'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],' \
           'AUTHORITY["EPSG","4326"]]'
