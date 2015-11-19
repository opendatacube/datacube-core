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

from collections import defaultdict

from datacube import index

import numpy

from datacube.cubeaccess.core import Coordinate, Variable
from datacube.cubeaccess.core import StorageUnitStack
from datacube.cubeaccess.storage import NetCDF4StorageUnit
from common import ndv_to_nan


# TODO: this should be in a lib somewhere
def make_storage_unit(su):
    coordinates = {name: Coordinate(dtype=numpy.dtype(attrs['dtype']),
                                    begin=attrs['begin'],
                                    end=attrs['end'],
                                    length=attrs['length'],
                                    units='1')  # TODO: should be this: attrs['units'])
                   for name, attrs in su.descriptor['coordinates'].items()}
    variables = {name: Variable(dtype=numpy.dtype(attrs['dtype']),
                                nodata=attrs['nodata'],
                                dimensions=attrs['dimensions'],
                                units=attrs['units'])
                 for name, attrs in su.descriptor['measurements'].items()}
    return NetCDF4StorageUnit(su.path, coordinates=coordinates, variables=variables)


def group_storage_units_by_location(sus):
    dims = ('longitude', 'latitude')
    stacks = defaultdict(list)
    for su in sus:
        stacks[tuple(su.coordinates[dim].begin for dim in dims)].append(su)
    return stacks


def get_descriptors(query=None):
    data_index = index.data_management_connect()
    sus = data_index.get_storage_units()

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
    descriptors = get_descriptors()

    for descriptor in descriptors:
        nir = ndv_to_nan(descriptor['NBAR'].get('band_40').values)
        red = ndv_to_nan(descriptor['NBAR'].get('band_30').values)
        ndvi = numpy.mean((nir-red)/(nir+red), axis=0)
        print ("NDVI Whoo!!!")
        print (ndvi)

if __name__ == "__main__":
    import sys
    main(sys.argv)
