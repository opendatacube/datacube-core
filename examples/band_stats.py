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
#: pylint: disable=too-many-locals
"""
Example showing usage of search api and data access api to calculate some band time statistics
"""

from __future__ import absolute_import, division, print_function

from itertools import groupby

import click
import numpy
import rasterio
from rasterio.coords import BoundingBox

from affine import Affine
from osgeo import osr

from datacube.model import Coordinate, Variable
from datacube.api import make_storage_unit
from datacube.index import index_connect
from datacube.storage.access.core import StorageUnitBase, StorageUnitStack, StorageUnitDimensionProxy,\
    StorageUnitVariableProxy
from datacube.ui import parse_expressions

from datacube.storage.storage import fuse_sources, DatasetSource, RESAMPLING, RESAMPLING_METHODS
from datacube.storage.utils import datetime_to_seconds_since_1970


def group_storage_units_by_location(sus):
    dims = ('x', 'y')
    stacks = {}
    for su in sus:
        stacks.setdefault(tuple(su.coordinates[dim].begin for dim in dims), []).append(su)
    return stacks


def ndv_to_nan(a, ndv=-999):
    a = a.astype(numpy.float32)
    a[a == ndv] = numpy.nan
    return a


def ls_pqa_mask(pqa):
    masked = 255 | 256 | 15360
    return (pqa & masked) != masked


def filter_duplicates(sus):
    return {su.id_: su for su in sus}.values()


# pylint: disable=too-many-locals
def get_data(datasets,
             measurement_id,
             bounds,
             resolution,
             crs,
             nodata,
             group_func,
             fuse_func=None):
    """
    :type bounds: BoundingBox
    """
    datasets = [dataset for dataset in datasets if measurement_id in dataset.metadata.measurements_dict]
    datasets.sort()
    groups = [(key, [DatasetSource(dataset, measurement_id) for dataset in group])
              for key, group in groupby(datasets, group_func)]

    shape = (len(groups),
             int((bounds.top - bounds.bottom) / abs(resolution[1]) + 0.5),
             int((bounds.right - bounds.left) / abs(resolution[0]) + 0.5))
    transform = Affine(resolution[0], 0.0, bounds.right if resolution[0] < 0 else bounds.left,
                       0.0, resolution[1], bounds.top if resolution[1] < 0 else bounds.bottom)

    result = numpy.empty(shape, dtype=numpy.int16)
    for index, (key, sources) in enumerate(groups):
        fuse_sources(sources,
                     result[index],
                     transform,
                     crs,
                     nodata,
                     resampling=RESAMPLING.nearest,
                     fuse_func=fuse_func)
    return result


class NoSpoonStorageUnit(StorageUnitBase):
    def __init__(self, datasets, bounds, resolution, crs, mapping, fuse_func=None):
        if not datasets:
            raise RuntimeError('Shall not make empty StorageUnit')

        self._datasets = datasets
        self._bounds = bounds
        self._res = resolution
        self.crs = crs
        self._varmap = {attrs['varname']: name for name, attrs in mapping.items()}
        self._mapping = mapping
        self._fuse_func = fuse_func

        self.shape = (int((bounds.top - bounds.bottom) / abs(resolution[1]) + 0.5),
                      int((bounds.right - bounds.left) / abs(resolution[0]) + 0.5))
        self.affine = Affine(resolution[0], 0.0, bounds.right if resolution[0] < 0 else bounds.left,
                             0.0, resolution[1], bounds.top if resolution[1] < 0 else bounds.bottom)

        height, width = self.shape
        self.geo_extents = [(0, 0), (0, height), (width, height), (width, 0)]
        self.affine.itransform(self.geo_extents)

        xs = numpy.arange(width) * self.affine.a + self.affine.c + self.affine.a / 2
        ys = numpy.arange(height) * self.affine.e + self.affine.f + self.affine.e / 2

        crs = osr.SpatialReference()
        crs.SetFromUserInput(self.crs)

        self.coord_data = {}
        self.coordinates = {}

        if crs.IsGeographic():
            self.coord_data['longitude'] = xs
            self.coord_data['latitude'] = ys
            self.coordinates['latitude'] = Coordinate(ys.dtype, ys[0], ys[-1], ys.size, 'degrees_north')
            self.coordinates['longitude'] = Coordinate(xs.dtype, xs[0], xs[-1], xs.size, 'degrees_east')
            dimensions = 'latitude', 'longitude'

        elif crs.IsProjected():
            units = crs.GetAttrValue('UNIT')
            self.coord_data['x'] = xs
            self.coord_data['y'] = ys
            self.coordinates['x'] = Coordinate(xs.dtype, xs[0], xs[-1], xs.size, units)
            self.coordinates['y'] = Coordinate(ys.dtype, ys[0], ys[-1], ys.size, units)
            dimensions = 'x', 'y'

            wgs84 = osr.SpatialReference()
            wgs84.ImportFromEPSG(4326)
            transform = osr.CoordinateTransformation(self.crs, wgs84)
            self.geo_extents = transform.TransformPoints(self.geo_extents)

        else:
            raise RuntimeError('Crazy CRS')

        self.variables = {
            attrs['varname']: Variable(numpy.dtype(attrs['dtype']),
                                       attrs['nodata'], dimensions,
                                       attrs.get('units', '1'))
            for name, attrs in mapping.items()
        }

    def get_crs(self):
        return self.crs

    def _get_coord(self, dim):
        return self.coord_data[dim]

    def _fill_data(self, name, index, dest):
        affine = self.affine*Affine.translation(index[1].start, index[0].start)
        measurement_id = self._varmap[name]
        sources = [DatasetSource(dataset, measurement_id) for dataset in self._datasets]
        fuse_sources(sources,
                     dest,
                     affine,
                     self.crs,
                     self.variables[name].nodata,
                     resampling=RESAMPLING_METHODS[self._mapping[measurement_id]['resampling_method']],
                     fuse_func=self._fuse_func)


def do_no_spoon(stats, bands, query, index):
    datasets = index.datasets.search_eager(*query)
    group_func = lambda ds: ds.time
    datasets.sort(key=group_func)
    groups = [(key, list(group)) for key, group in groupby(datasets, group_func)]

    sus = []
    for v, group in groups:
        su = NoSpoonStorageUnit(group,
                                bounds=BoundingBox(149.00000001, -34.9999999, 149.99999999, -34.00000001),
                                resolution=(0.00025, -0.00025),
                                crs='EPSG:4326',
                                mapping={
                                    '10': {'varname': 'blue',
                                           'dtype': numpy.int16,
                                           'nodata': -999,
                                           'resampling_method': 'cubic'}
                                })
        v = datetime_to_seconds_since_1970(v)
        sus.append(
            StorageUnitDimensionProxy(su, ('time', v, numpy.dtype(numpy.float64), 'seconds since 1970-01-01 00:00:00')))
    data = StorageUnitStack(sus, 'time')
    print (data)
    print (data.get('blue', longitude=slice(0, 6), latitude=slice(0, 12)))


def get_descriptors(index, *query):
    """run a query and turn results into StorageUnitStacks"""
    sus = index.storage.search_eager(*query)
    sus = filter_duplicates(sus)

    nbars = [make_storage_unit(su) for su in sus if 'PQ' not in su.path]
    pqs = [make_storage_unit(su) for su in sus if 'PQ' in su.path]

    nbars = group_storage_units_by_location(nbars)
    pqs = group_storage_units_by_location(pqs)

    result = []
    for key in nbars:
        result.append({
            'NBAR': StorageUnitStack(sorted(nbars[key], key=lambda su: su.coordinates['time'].begin), 'time'),
            'PQ': StorageUnitStack(sorted(pqs[key], key=lambda su: su.coordinates['time'].begin), 'time')
        })
    return result


def do_storage_unit(stats, bands, query, index):
    # map bands to meaningfull names
    ls57_var_map = {
        'blue': 'band_10',
        'green': 'band_20',
        'red': 'band_30',
        'nir': 'band_40',
        'ir1': 'band_50',
        'ir2': 'band_70'
    }
    pqa_var_map = {
        'pqa': 'band_pixelquality'
    }

    descriptors = get_descriptors(index, *query)

    n = 200

    # split the work across time stacks
    for descriptor in descriptors:
        nbars = StorageUnitVariableProxy(descriptor['NBAR'], ls57_var_map)
        pqas = StorageUnitVariableProxy(descriptor['PQ'], pqa_var_map)

        nbands = len(stats) * len(bands)
        name = "%s_%s.tif" % (nbars.coordinates['longitude'].begin, nbars.coordinates['latitude'].end)
        with rasterio.open(name, 'w', driver='GTiff',
                           width=nbars.coordinates['longitude'].length,
                           height=nbars.coordinates['latitude'].length,
                           count=nbands, dtype=numpy.int16,
                           # crs=proj, transform=geotr, TODO: transform/projection
                           nodata=-999,
                           INTERLEAVE="BAND", COMPRESS="LZW", TILED="YES") as raster:
            for band_idx, b in enumerate(bands):
                for lat in range(0, nbars.coordinates['latitude'].length, n):
                    band_num = band_idx * len(stats) + 1
                    chunk = dict(latitude=slice(lat, lat + n))

                    # TODO: use requested portion of the data, not the whole tile
                    pqa_mask = ls_pqa_mask(pqas.get('pqa', **chunk).values)
                    data = nbars.get(b, **chunk).values
                    data = ndv_to_nan(data, nbars.variables[b].nodata)
                    data[pqa_mask] = numpy.nan

                    for stat in stats:
                        result = getattr(numpy, 'nan' + stat)(data, axis=0)
                        raster.write(result.astype('int16'), indexes=band_num,
                                     window=((lat, lat + result.shape[0]), (0, result.shape[1])))
                        band_num += 1


@click.command()
@click.argument('expression', nargs=-1)
@click.option('--band', multiple=True, help="bands to calculate statistics on",
              type=click.Choice(['blue', 'green', 'red', 'nir', 'ir1', 'ir2']))
@click.option('--no-spoon', is_flag=True, help="bypass storage unit access")
# @click.option('--percentile', type=int, multiple=True)
@click.option('--mean', is_flag=True, help="calculate mean of the specified bands")
def main(expression, no_spoon, band, **features):
    """
    Calculate some band stats and dump them into tif files in the current directory
    """
    stats = [stat for stat, enabled in features.items() if enabled]
    if len(stats) == 0 or len(band) == 0:
        print('nothing to do')
        return

    index = index_connect()
    query = parse_expressions(index.storage.get_field_with_fallback, *expression)

    if no_spoon:
        do_no_spoon(stats, band, query, index)
    else:
        do_storage_unit(stats, band, query, index)


if __name__ == "__main__":
    main()
