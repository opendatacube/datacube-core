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

"""
Example showing usage of search api and data access api to calculate some band time statistics
"""

from __future__ import absolute_import, division, print_function

from collections import defaultdict

from datacube.index import index_connect

import numpy

from datacube.cubeaccess.core import Coordinate, Variable
from datacube.cubeaccess.core import StorageUnitStack, StorageUnitVariableProxy
from datacube.cubeaccess.storage import NetCDF4StorageUnit
from common import ndv_to_nan, do_work


# TODO: this should be in a lib somewhere
def make_storage_unit(su):
    """turn db search result into StorageUnit object"""
    coordinates = {name: Coordinate(dtype=numpy.dtype(attrs['dtype']),
                                    begin=attrs['begin'],
                                    end=attrs['end'],
                                    length=attrs['length'],
                                    units=attrs.get('units', None))
                   for name, attrs in su.descriptor['coordinates'].items()}
    variables = {name: Variable(dtype=numpy.dtype(attrs['dtype']),
                                nodata=attrs['nodata'],
                                dimensions=attrs['dimensions'],
                                units=attrs.get('units', None))
                 for name, attrs in su.descriptor['measurements'].items()}
    return NetCDF4StorageUnit(su.filepath, coordinates=coordinates, variables=variables)


def group_storage_units_by_location(sus):
    """group_storage_units_by_location so they can be stacked by time"""
    dims = ('longitude', 'latitude')
    stacks = defaultdict(list)
    for su in sus:
        stacks[tuple(su.coordinates[dim].begin for dim in dims)].append(su)
    return stacks


def get_descriptors(query=None):
    """run a query and turn results into StorageUnitStacks"""
    index = index_connect()
    sus = index.storage.search_eager()

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

def main(argv):
    # map bands to meaningfull names
    LS57varmap = {'blue': 'band_10',
                  'green': 'band_20',
                  'red': 'band_30',
                  'nir': 'band_40',
                  'ir1': 'band_50',
                  'ir2': 'band_70'}
    PQAvarmap = {'pqa': 'band_pixelquality'}

    # get the data
    descriptors = get_descriptors()

    qs = [10, 50, 90]
    num_workers = 16
    N = 4000//num_workers

    # split the work across time stacks
    for descriptor in descriptors:
        # split the work along the latitude axis in N-sized chunks
        for lat in range(0, 4000, N):
            data = do_work(StorageUnitVariableProxy(descriptor['NBAR'], LS57varmap),
                           StorageUnitVariableProxy(descriptor['PQ'], PQAvarmap),
                           qs,
                           time='time',
                           latitude=slice(lat, lat + N))

        # nir = ndv_to_nan(descriptor['NBAR'].get('band_40').values)
        # red = ndv_to_nan(descriptor['NBAR'].get('band_30').values)
        # ndvi = numpy.mean((nir-red)/(nir+red), axis=0)
        # print ("NDVI Whoo!!!")
        # print (ndvi)

if __name__ == "__main__":
    import sys
    main(sys.argv)
