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
"""

from __future__ import absolute_import, division

import itertools
import uuid
import numpy
import dask

from .model import Range
from .storage.access.core import Coordinate, Variable, StorageUnitStack, StorageUnitDimensionProxy
from .storage.access.backends import NetCDF4StorageUnit, GeoTifStorageUnit
from .index import index_connect


def get_storage_unit_transform(su):
    storage_type = su.attributes['storage_type']
    return [su.coordinates['longitude'].begin, storage_type['resolution']['x'], 0.0,
            su.coordinates['latitude'].begin, 0.0, storage_type['resolution']['y']]


def get_storage_unit_projection(su):
    storage_type = su.attributes['storage_type']
    return storage_type['projection']['spatial_ref']


def make_storage_unit(su):
    """convert search result into StorageUnit object"""
    def map_dims(dims):
        # TODO: remove this hack
        mapping = {'t': 'time', 'y': 'latitude', 'x': 'longitude'}
        return tuple(mapping[dim] for dim in dims)

    storage_type = su.storage_mapping.storage_type.descriptor
    coordinates = {name: Coordinate(dtype=numpy.dtype(attrs['dtype']),
                                    begin=attrs['begin'],
                                    end=attrs['end'],
                                    length=attrs['length'],
                                    units=attrs.get('units', None))
                   for name, attrs in su.descriptor['coordinates'].items()}
    variables = {
        attrs['varname']: Variable(
            dtype=numpy.dtype(attrs['dtype']),
            nodata=attrs.get('nodata', None),
            dimensions=map_dims(storage_type['dimension_order']),
            units=attrs.get('units', None))
        for attrs in su.storage_mapping.measurements.values()
    }
    attributes = {
        'storage_type': storage_type
    }

    if su.storage_mapping.storage_type.driver == 'NetCDF CF':
        return NetCDF4StorageUnit(su.filepath, coordinates=coordinates, variables=variables, attributes=attributes)

    if su.storage_mapping.storage_type.driver == 'GeoTiff':
        from datetime import datetime
        result = GeoTifStorageUnit(su.filepath, coordinates=coordinates, variables=variables, attributes=attributes)
        time = datetime.strptime(su.descriptor['extents']['time_min'], '%Y-%m-%dT%H:%M:%S.%f')
        time = (time - datetime.utcfromtimestamp(0)).total_seconds()
        return StorageUnitDimensionProxy(result, ('time', time, numpy.float64, 'seconds since 1970'))

    raise RuntimeError('unsupported storage unit access driver %s' % su.storage_mapping.storage_type.driver)


def group_storage_units_by_location(sus):
    dims = ('longitude', 'latitude')
    stacks = {}
    for su in sus:
        stacks.setdefault(tuple(su.coordinates[dim].begin for dim in dims), []).append(su)
    return stacks


def get_descriptors(**query):
    index = index_connect()
    sus = index.storage.search(**query)

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
    def get_descriptor(self, descriptor=None):
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
        if descriptor:
            query = {key: descriptor[key] for key in ('satellite', 'sensor', 'product') if key in descriptor}
            query.update({dim: Range(*data['range']) for dim, data in descriptor['dimensions'].items()})
        else:
            query = {}

        stacks = get_descriptors(**query)

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
        if descriptor:
            query = {key: descriptor[key] for key in ('satellite', 'sensor', 'product') if key in descriptor}
            if 'dimensions' in descriptor:
                reqrange = {dim: Range(*data['range']) for dim, data in descriptor['dimensions'].items()}
                query.update(reqrange)
                # TODO: talk to Jeremy about this
                hack = {
                    'lon': 'longitude',
                    'lat': 'latitude'
                }
                reqrange = {hack[dim]: data for dim, data in reqrange.items()}
            else:
                reqrange = {}
        else:
            query = reqrange = {}

        data_response = {'arrays': {}}

        index = index_connect()
        sus = index.storage.search(**query)

        storage_units_by_type = {}
        for su in sus:
            stype = su.storage_mapping.match.metadata['platform']['code'] + '_' + \
                    su.storage_mapping.match.metadata['instrument']['name']
            ptype = su.storage_mapping.match.metadata['product_type']
            #key = (stype, ptype)
            # TODO: group by storage type also?
            storage_units_by_type.setdefault(stype, {}).setdefault(ptype, []).append(make_storage_unit(su))

        if (len(storage_units_by_type)):
            raise RuntimeError('Data must come from a single storage')

        for stype, products in storage_units_by_type.items():
            dask_dict = {}
            for ptype, storage_units in products.items():
                dask_dict.update(get_dask(storage_units))  #TODO: check var names are unique accross products

            for var in stack.variables:
                if var in descriptor['variables']:
                    data_response['arrays'][var] = stack.get(var, **reqrange)
                    data_response['dimensions'] = stack.variables[var].dimensions

        return data_response


    def get_dask(self, storage_units, variables=None):
        """
        Create a dask array to call the underlying storage units
        :return dict of dask arrays.
        """
        if not len(storage_units):
            return {}

        sample = storage_units[0]
        variables = variables or [v_name for v_name, v in sample.variables.items() if len(v.dimensions) == 3]
        if not len(variables):
            return {}

        data = {}
        chunksize = {}
        nodata = {}
        dsk_id = str(uuid.uuid1())  #unique name of the requested object
        dims = ('longitude', 'latitude', 'time')  #hardcoded for now
        dim_vals = {}
        for dim in dims:
            dim_vals[dim] = sorted(set(storage_unit.coordinates[dim].begin for storage_unit in storage_units))
        for storage_unit in storage_units:
            dsk_index = tuple()
            for dim in dims:
                ordinal = dim_vals[dim].index(storage_unit.coordinates[dim].begin)
                dsk_index += (ordinal,)
            for var_name, var in storage_unit.variables.items():
                if var_name in variables:
                    nodata[var_name] = var.nodata
                    var_dsk_id = '{}_{}'.format(dsk_id, var_name)
                    var_dsk_index = (var_dsk_id,) + dsk_index
                    data.setdefault(var_name, {})[var_dsk_index] = (storage_unit.get, storage_unit, var_name)
                    chunksize.setdefault(var_name, {})[var_dsk_index] = [storage_unit.coordinates[dim].length for dim in dims]

        def nodataFunc(shape, dtype, fill):
            return numpy.empty(shape.dtype).fill(fill)

        da_dict = {}
        for var_name in variables:
            var_dsk_id = '{}_{}'.format(dsk_id, var_name)
            var_dsk_index = (var_dsk_id,)
            all_dsk_keys = set(itertools.product(var_dsk_index, *tuple(range(len(vals)) for vals in dim_vals.values())))
            data_dsk_keys = data[var_name].viewkeys()
            missing_dsk_keys = all_dsk_keys - data_dsk_keys
            for key in missing_dsk_keys:
                shape = tuple(c.length for c in sample.coordinates.values())
                dtype = sample.variables[var_name].dtype
                nodata = sample.variables[var_name].nodata
                data[var_name][key] = (nodataFunc, shape, dtype, nodata)
            chunks = [c.length for c in sample.coordinates.values()]
            shape = [coord.length * len(dim_vals[dim_name]) for dim_name, coord in sample.coordinates.items()]
            da_dict[var_name] = dask.DaskArray(data[var_name], var_dsk_index, chunks, shape)

        return da_dict