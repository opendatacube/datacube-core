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

from datacube.api._storage import make_storage_unit
from datacube.index import index_connect
from datacube.storage.storage import storage_unit_to_access_unit
from datacube.storage.access.core import StorageUnitStack, StorageUnitVariableProxy
from datacube.ui import parse_expressions


def group_storage_units_by_type(sus):
    groups = {}
    for su in sus:
        groups.setdefault(su.storage_type, []).append(su)
    return groups


def group_storage_units_by_location(sus):
    stacks = {}
    for su in sus:
        stacks.setdefault(su.tile_index, []).append(su)
    return stacks


def ndv_to_nan(a, ndv=-999):
    a = a.astype(numpy.float32)
    a[a == ndv] = numpy.nan
    return a


def ls_pqa_mask(pqa):
    masked = 255 | 256 | 15360
    return (pqa & masked) != masked


def filter_duplicates(sus):
    return {su.id: su for su in sus}.values()


def get_descriptors(index, *query):
    """run a query and turn results into StorageUnitStacks"""
    sus = index.storage.search_eager(*query)
    sus = filter_duplicates(sus)

    nbars = pqs = {}
    grouped_by_type = group_storage_units_by_type(sus)
    for storage_type in grouped_by_type:
        product_type = storage_type.document['match']['metadata']['product_type']
        if product_type == 'NBAR':
            nbars = grouped_by_type[storage_type]
        if product_type == 'PQ':
            pqs = grouped_by_type[storage_type]

    assert nbars and pqs

    nbars = group_storage_units_by_location(nbars)
    pqs = group_storage_units_by_location(pqs)

    result = []
    for key in nbars:
        nbars[key].sort(key=lambda su: su.coordinates['time'].begin)
        pqs[key].sort(key=lambda su: su.coordinates['time'].begin)
        result.append({
            'NBAR': StorageUnitStack([storage_unit_to_access_unit(su) for su in nbars[key]], 'time'),
            'PQ': StorageUnitStack([storage_unit_to_access_unit(su) for su in pqs[key]], 'time')
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

    index = index_connect()
    query = parse_expressions(index.storage.get_field_with_fallback, *expression)
    do_storage_unit(stats, band, query, index)


if __name__ == "__main__":
    main()
