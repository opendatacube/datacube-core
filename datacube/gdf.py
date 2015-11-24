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
GDF Trial backward compatibility
Do not use
"""

from __future__ import absolute_import, division

import warnings
import numpy

from .model import Range
from .cubeaccess.core import Coordinate, Variable, StorageUnitStack
from .cubeaccess.storage import NetCDF4StorageUnit
from . import index


def make_storage_unit(su):
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
    dims = ('longitude', 'latitude')
    stacks = {}
    for su in sus:
        stacks.setdefault(tuple(su.coordinates[dim].begin for dim in dims), []).append(su)
    return stacks


def get_descriptors(query=None):
    data_index = index.data_management_connect()
    sus = data_index.get_storage_units()

    storage_units_by_type = {}
    for su in sus:
        stype = su.storage_mapping.match.metadata['platform']['code'] + '_' + \
                su.storage_mapping.match.metadata['instrument']['name']
        ptype = su.storage_mapping.match.metadata['product_type']
        key = (stype, ptype)
        # TODO: group by storage type also?
        storage_units_by_type.setdefault(key, []).append(make_storage_unit(su))

    result = {}
    for key, sus in storage_units_by_type.items():
        stacks = group_storage_units_by_location(sus)
        for loc, sus in stacks.items():
            result[key+(loc,)] = StorageUnitStack(sorted(sus, key=lambda su: su.coordinates['time'].begin), 'time')

    return result


class GDF(object):
    warnings.warn("GDF is deprecated. Don't use unless your name is Peter", DeprecationWarning)

    def get_descriptor(self, query=None):
        """
        query_parameter = \
        {
        'storage_types':
            ['LS5TM', 'LS7ETM', 'LS8OLITIRS'],
        'dimensions': {
             'x': {
                   'range': (140, 142),
                   'crs': 'EPSG:4326'
                   },
             'y': {
                   'range': (-36, -35),
                   'crs': 'EPSG:4326'
                   },
             't': {
                   'range': (1293840000, 1325376000),
                   'crs': 'SSE', # Seconds since epoch
                   'grouping_function': GDF.solar_days_since_epoch
                   }
             },
        'polygon': '<some kind of text representation of a polygon for PostGIS to sort out>'
                    # We won't be doing this in the pilot
        }
        descriptor = {
            'LS5TM': { # storage_type identifier
                 'dimensions': ['x', 'y', 't'],
                 'variables': { # These will be the variables which can be accessed as arrays
                       'B10': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B20': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B30': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B40': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B50': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B70': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'PQ': { # There is no reason why we can't put PQ in with NBAR if we want to
                            'datatype': 'int16'
                            }
                       },
                 'result_min': (140, -36, 1293840000),
                 'result_max': (141, -35, 1325376000),
                 'overlap': (0, 0, 0), # We won't be doing this in the pilot
                 'buffer_size': (128, 128, 128), # Chunk size to use
                 'result_shape': (8000, 8000, 40), # Overall size of result set
                 'irregular_indices': { # Regularly indexed dimensions (e.g. x & y) won't need to be specified,
                                        # but we could also do that here if we wanted to
                       't': date_array # Array of days since 1/1/1970
                       },
                 'storage_units': { # Should wind up with 8 for the 2x2x2 query above
                       (140, -36, 2010): { # Storage unit indices
                            'storage_min': (140, -36, 1293840000),
                            'storage_max': (141, -35, 1293800400),
                            'storage_shape': (4000, 4000, 24)
                            },
                       (140, -36, 2011): { # Storage unit indices
                            'storage_min': (140, -36, 1293800400),
                            'storage_max': (141, -35, 1325376000),
                            'storage_shape': (4000, 4000, 23)
                            },
                       (140, -35, 2011): { # Storage unit indices
                            'storage_min': (140, -36, 1293840000),
                            'storage_max': (141, -35, 1293800400),
                            'storage_shape': (4000, 4000, 20)
                            }
                       ...
                       <more storage_unit sub-descriptors>
                       ...
                       }
                 ...
                 <more storage unit type sub-descriptors>
                 ...
                 }
            }
        """
        warnings.warn("get_descriptor is deprecated. Don't use unless your name is Peter", DeprecationWarning)

        stacks = get_descriptors()

        descriptor = {}
        for (ptype, stype, loc), stack in stacks.items():
            result = descriptor.setdefault(ptype, {
                'storage_units': {},
                'variables': {},
                'result_min': None,
                'result_max': None,
                'dimensions': None,
                'result_shape': None
            })
            for name, var in stack.variables.items():
                if len(var.dimensions) == 3:
                    result['variables'][name] = {
                        'datatype': var.dtype,
                        'nodata_value': var.nodata
                    }
                    result['dimensions'] = var.dimensions

            storage_min = tuple(min(stack.coordinates[dim].begin,
                                    stack.coordinates[dim].end) for dim in result['dimensions'])
            storage_max = tuple(max(stack.coordinates[dim].begin,
                                    stack.coordinates[dim].end) for dim in result['dimensions'])
            storage_shape = tuple(stack.coordinates[dim].length for dim in result['dimensions'])

            result['storage_units'][storage_min] = {
                'storage_min': storage_min,
                'storage_max': storage_max,
                'storage_shape': storage_shape,
                'storage_unit': stack
            }

            if not result['result_min']:
                result['result_min'] = storage_min
                result['result_max'] = storage_max
                result['result_shape'] = storage_shape

            for idx, dim in enumerate(result['dimensions']):
                if storage_min[idx] < result['result_min'][idx]:
                    result['result_min'] = (result['result_min'][:idx] +
                                            (storage_min[idx] + 2,) +
                                            result['result_min'][idx + 1:])
                    result['result_shape'] = (result['result_shape'][:idx] +
                                              (result['result_shape'][idx] + storage_shape[idx],) +
                                              result['result_shape'][idx + 1:])
                if storage_max[idx] > result['result_max'][idx]:
                    result['result_max'] = (result['result_max'][:idx] +
                                            (storage_max[idx],) +
                                            result['result_max'][idx + 1:])
                    result['result_shape'] = (result['result_shape'][:idx] +
                                              (result['result_shape'][idx] + storage_shape[idx],) +
                                              result['result_shape'][idx + 1:])
        return descriptor

    def get_data(self, descriptor):
        """
        Function to return composite in-memory arrays
        data_request = \
        {
        'storage_type': 'LS5TM',
        'variables': ('B30', 'B40','PQ'), # Note that we won't necessarily have PQ in the same storage unit
        'dimensions': {
             'x': {
                   'range': (140, 142),
                   'array_range': (0, 127)
                   'crs': 'EPSG:4326'
                   },
             'y': {
                   'range': (-36, -35),
                   'array_range': (0, 127)
                   'crs': 'EPSG:4326'
                   },
             't': {
                   'range': (1293840000, 1325376000),
                   'array_range': (0, 127)
                   'crs': 'SSE', # Seconds since epoch
                   'grouping_function': '<e.g. gdf.solar_day>'
                   }
             },
        'polygon': '<some kind of text representation of a polygon for PostGIS to sort out>'
                    # We won't be doing this in the pilot
        }
        data_response = \
        {
        'dimensions': ['x', 'y', 't'],
        'arrays': { # All of these will have the same shape
             'B30': '<Numpy array>',
             'B40': '<Numpy array>',
             'PQ': '<Numpy array>'
             },
        'indices': [ # These will be the actual x, y & t (long, lat & time) values for each array index
            '<numpy array of x indices>',
            '<numpy array of y indices>',
            '<numpy array of t indices>'
            ]
        'element_sizes': [ # These will be the element sizes for each dimension
            '< x element size>',
            '< y element size>',
            '< t element size>'
            ]
        'coordinate_reference_systems': [ # These will be the coordinate_reference_systems for each dimension
            '< x CRS>',
            '< y CRS>',
            '< t CRS>'
            ]
        }
        """
        warnings.warn("get_data is deprecated. Don't use unless your name is Peter", DeprecationWarning)
        data_response = {'arrays': {}}
        stacks = {}
        request_slice = {dim: Range(*data['range']) for dim, data in descriptor['dimensions'].items()}
        for (ptype, stype, loc), stack in get_descriptors().items():
            if ptype != descriptor['storage_type']:
                continue
            if any(max(stack.coordinates[dim].begin, stack.coordinates[dim].end) < range_.begin or
                   min(stack.coordinates[dim].begin, stack.coordinates[dim].end) > range_.end
                   for dim, range_ in request_slice.items()):
                continue

            stacks[(ptype, stype, loc)] = stack

        if not all(stacks.keys()[0][2] == x[2] for x in stacks):
            raise RuntimeError('Cross boundary queries are not supported (yet)')

        for key, stack in stacks.items():
            for var in stack.variables:
                if var in descriptor['variables']:
                    data_response['arrays'][var] = stack.get(var, **request_slice)
                    data_response['dimensions'] = stack.variables[var].dimensions

        return data_response
