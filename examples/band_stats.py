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

import click
import numpy
import rasterio

from datacube.api import make_storage_unit, group_storage_units_by_location
from datacube.index import index_connect
from datacube.storage.access.core import StorageUnitStack, StorageUnitVariableProxy
from datacube.ui import parse_expressions


def ndv_to_nan(a, ndv=-999):
    a = a.astype(numpy.float32)
    a[a == ndv] = numpy.nan
    return a


def ls_pqa_mask(pqa):
    masked = 255 | 256 | 15360
    return (pqa & masked) != masked


def get_descriptors(*query):
    """run a query and turn results into StorageUnitStacks"""
    index = index_connect()
    sus = index.storage.search_eager(*query)

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


@click.command()
@click.argument('expression', nargs=-1)
@click.option('--band', multiple=True, help="bands to calculate statistics on",
              type=click.Choice(['blue', 'green', 'red', 'nir', 'ir1', 'ir2']))
# @click.option('--percentile', type=int, multiple=True)
@click.option('--mean', is_flag=True, help="calculate mean of the specified bands")
def main(expression, band, **features):
    """
    Calculate some band stats and dump them into tif files in the current directory
    """
    stats = [stat for stat, enabled in features.items() if enabled]
    if len(stats) == 0 or len(band) == 0:
        print('nothing to do')
        return

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

    index = index_connect()
    query = parse_expressions(index.storage.get_field_with_fallback, *expression)

    # get the data
    descriptors = get_descriptors(*query)

    n = 200

    # split the work across time stacks
    for descriptor in descriptors:
        nbars = StorageUnitVariableProxy(descriptor['NBAR'], ls57_var_map)
        pqas = StorageUnitVariableProxy(descriptor['PQ'], pqa_var_map)

        nbands = len(stats) * len(band)
        name = "%s_%s.tif" % (nbars.coordinates['longitude'].begin, nbars.coordinates['latitude'].end)
        with rasterio.open(name, 'w', driver='GTiff',
                           width=nbars.coordinates['longitude'].length,
                           height=nbars.coordinates['latitude'].length,
                           count=nbands, dtype=numpy.int16,
                           # crs=proj, transform=geotr, TODO: transform/projection
                           nodata=-999,
                           INTERLEAVE="BAND", COMPRESS="LZW", TILED="YES") as raster:
            for band_idx, b in enumerate(band):
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


if __name__ == "__main__":
    main()
