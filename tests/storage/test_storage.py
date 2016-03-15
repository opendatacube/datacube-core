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

import numpy
import netCDF4
from pathlib import Path
from affine import Affine

from datacube.model import Coordinate, Variable, GeoBox
from datacube.storage.access.backends.geobox import GeoBoxStorageUnit
from datacube.storage.storage import write_access_unit_to_netcdf


GEO_PROJ = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],' \
           'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],' \
           'AUTHORITY["EPSG","4326"]]'


def test_write_access_unit_to_netcdf(tmpnetcdf_filename):
    affine = Affine.scale(0.1, 0.1)*Affine.translation(20, 30)
    geobox = GeoBox(100, 100, affine, GEO_PROJ)
    ds1 = GeoBoxStorageUnit(geobox,
                            {'time': Coordinate(numpy.dtype(numpy.int), begin=100, end=400, length=4, units='seconds')},
                            {
                                'B10': Variable(numpy.dtype(numpy.float32),
                                                nodata=numpy.nan,
                                                dimensions=('time', 'latitude', 'longitude'),
                                                units='1')
                            })
    write_access_unit_to_netcdf(ds1, {}, {}, {}, Path(tmpnetcdf_filename))

    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        assert 'B10' in nco.variables
        var = nco.variables['B10']
        assert (var[:] == ds1.get('B10').values).all()
