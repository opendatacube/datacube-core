#    Copyright 2015 Geoscience Australia
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


from __future__ import absolute_import, division, print_function

import os
import numpy

from datacube.cubeaccess.core import Coordinate, Variable
from datacube.cubeaccess.core import StorageUnitVariableProxy, StorageUnitDimensionProxy, StorageUnitStack
from datacube.cubeaccess.storage import FauxStorageUnit
from datacube.cubeaccess.indexing import Range

DATA_DIR = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'data')


ds1 = FauxStorageUnit({
    't': Coordinate(numpy.int, 100, 400, 4),
    'y': Coordinate(numpy.float32, 0, 9.5, 20),
    'x': Coordinate(numpy.float32, 9, 0, 10)
}, {
    'B10': Variable(numpy.float32, numpy.nan, ('t', 'y', 'x'))
})
ds2 = FauxStorageUnit({
    't': Coordinate(numpy.int, 500, 600, 3),
    'y': Coordinate(numpy.float32, 0, 9.5, 20),
    'x': Coordinate(numpy.float32, 9, 0, 10)
}, {
    'B10': Variable(numpy.float32, numpy.nan, ('t', 'y', 'x'))
})


def test_common_storage_unit():
    data = ds1.get('B10')
    assert data.values.shape == (4, 20, 10)
    assert data.dims == ('t', 'y', 'x')
    assert (data.values.ravel() == numpy.arange(data.values.size)).all()

    expected = numpy.array([
        [
            [223, 224, 225, 226],
            [233, 234, 235, 236],
            [243, 244, 245, 246]
        ],
        [
            [423, 424, 425, 426],
            [433, 434, 435, 436],
            [443, 444, 445, 446]
        ]
    ])

    data = ds1.get('B10', t=slice(1, 3), y=slice(2, 5), x=slice(3, 7))
    assert data.values.shape == (2, 3, 4)
    assert (data.values == expected).all()

    dest = numpy.zeros((3, 4, 5))
    data = ds1.get('B10', dest=dest, t=slice(1, 3), y=slice(2, 5), x=slice(3, 7))
    assert data.values.shape == (2, 3, 4)
    assert (data.values == expected).all()
    assert (dest[:2, :3, :4] == expected).all()


def test_storage_unit_variable_proxy():
    su = StorageUnitVariableProxy(ds1, {'greg': 'B10'})
    expected = ds1.get('B10')
    result = su.get('greg')
    assert result.values.shape == expected.values.shape
    assert result.dims == expected.dims
    assert (result.values == expected.values).all()


def test_storage_unit_dimension_proxy():
    su = StorageUnitDimensionProxy(ds1, ('greg', 12.0))
    data = su.get_coord('greg')[0]
    assert data == numpy.array([12.0])

    data1 = su.get('B10')
    data2 = ds1.get('B10')
    assert data1.values.shape == (1,) + data2.values.shape
    assert data1.dims == ('greg',) + data2.dims
    assert (data1.values.ravel() == data2.values.ravel()).all()

    data = su.get('B10', greg=Range(13, 14))
    assert data.values.size == 0


def test_storage_unit_stack():
    stack = StorageUnitStack([ds1, ds2], 't')
    expected = numpy.array([
        [
            [624, 625, 626],
            [634, 635, 636]
        ],
        [
            [24, 25, 26],
            [34, 35, 36]
        ]
    ])

    data = stack.get('B10', t=Range(400, 500), x=Range(3, 5), y=Range(1, 1.5))
    assert len(data.coords['t']) == 2
    assert len(data.coords['x']) == 3
    assert len(data.coords['y']) == 2
    assert (data.values == expected).all()

    data = stack.get('B10', t=slice(3, 5), x=slice(4, 7), y=slice(2, 4))
    assert len(data.coords['t']) == 2
    assert len(data.coords['x']) == 3
    assert len(data.coords['y']) == 2
    assert (data.values == expected).all()


def test_geotif_storage_unit():
    from datacube.cubeaccess.storage import GeoTifStorageUnit

    su = GeoTifStorageUnit(DATA_DIR+'/test.tif')
    assert set(su.coordinates.keys()) == ({'x', 'y'})
    assert su.variables['2'].nodata == -999

    # floating point dodge... am I doing something wrong?
    data = su.get('2', x=Range(144.5, 144.7-0.00001), y=Range(-35.5, -35.2-0.00001))
    assert len(data.coords['x']) == 800
    assert len(data.coords['y']) == 600
    assert (data.values == 2).all()

    data = su.get('1', x=slice(500), y=slice(1400, None))
    assert len(data.coords['x']) == 500
    assert len(data.coords['y']) == 600
    assert (data.values == 1).all()


def test_netcdf_storage_unit():
    from datacube.cubeaccess.storage import NetCDF4StorageUnit

    su = NetCDF4StorageUnit(DATA_DIR+'/test.nc')
    assert set(su.coordinates.keys()) == ({'longitude', 'latitude', 'time'})
    assert su.variables['B2'].nodata == -999

    data = su.get('B2', longitude=Range(151.5, 151.7-0.00001), latitude=Range(-29.5, -29.2-0.00001))
    assert len(data.coords['time']) == 3
    assert len(data.coords['longitude']) == 800
    assert len(data.coords['latitude']) == 600
    assert (data.values == 2).all()
