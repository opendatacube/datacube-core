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
import numpy
import xarray as xr

from datacube.testutils import isclose

from datacube.api import geo_xarray

# HACK: to deal with https://github.com/mapbox/rasterio/issues/694
# try:
#     from rasterio.env import defenv
#     defenv()
# except ImportError:
#     pass


def test_geo_xarray():
    width = 1000
    height = 1000
    da = xr.DataArray(
        data=numpy.ones((4, 1000, 1000)),
        coords={
            'time': numpy.linspace(1, 5, 4),
            'longitude': numpy.linspace(148, 148.24975, width),
            'latitude': numpy.linspace(-35.24975, -35, height),
        },
        dims=['time', 'latitude', 'longitude'])
    shape = geo_xarray._get_shape(da)
    assert shape == (height, width)

    res = geo_xarray._get_resolution(da)
    assert isclose(res, 0.00025)
