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

import datetime
import itertools
import operator
import uuid
import numpy
import dask.array as da
import xray
import rasterio.warp

from .model import Range
from .storage.access.core import Coordinate, Variable, StorageUnitDimensionProxy
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
    """convert search result into StorageUnit object
    :param su: database index storage unit
    """
    def map_dims(dims):
        # TODO: remove this hack
        mapping = {'t': 'time', 'y': 'latitude', 'x': 'longitude'}
        return tuple(mapping[dim] for dim in dims)

    storage_type = su.storage_mapping.storage_type.descriptor
    coordinates = {name: Coordinate(dtype=numpy.dtype(attributes['dtype']),
                                    begin=attributes['begin'],
                                    end=attributes['end'],
                                    length=attributes['length'],
                                    units=attributes.get('units', None))
                   for name, attributes in su.descriptor['coordinates'].items()}
    variables = {
        attributes['varname']: Variable(
            dtype=numpy.dtype(attributes['dtype']),
            nodata=attributes.get('nodata', None),
            dimensions=map_dims(storage_type['dimension_order']),
            units=attributes.get('units', None))
        for attributes in su.storage_mapping.measurements.values()
    }
    attributes = {
        'storage_type': storage_type
    }

    if su.storage_mapping.storage_type.driver == 'NetCDF CF':
        return NetCDF4StorageUnit(su.filepath, coordinates=coordinates, variables=variables, attributes=attributes)

    if su.storage_mapping.storage_type.driver == 'GeoTiff':
        result = GeoTifStorageUnit(su.filepath, coordinates=coordinates, variables=variables, attributes=attributes)
        time = datetime.datetime.strptime(su.descriptor['extents']['time_min'], '%Y-%m-%dT%H:%M:%S.%f')
        time = (time - datetime.datetime.utcfromtimestamp(0)).total_seconds()
        return StorageUnitDimensionProxy(result, ('time', time, numpy.float64, 'seconds since 1970'))

    raise RuntimeError('unsupported storage unit access driver %s' % su.storage_mapping.storage_type.driver)


def datetime_to_timestamp(dt):
    if isinstance(dt, datetime.datetime) or isinstance(dt, datetime.date):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return (dt - epoch).total_seconds()
    return dt


def dimension_ranges_to_selector(dimension_ranges, reverse_sort):
    ranges = dict((dim_name, dim['range']) for dim_name, dim in dimension_ranges.items())
    # if 'time' in ranges:
    #     ranges['time'] = tuple(datetime_to_timestamp(r) for r in ranges['time'])
    return dict((c, slice(*sorted(r, reverse=reverse_sort[c]))) for c, r in ranges.items())


def dimension_ranges_to_iselector(dim_ranges):
    array_ranges = dict((dim_name, dim['array_range']) for dim_name, dim in dim_ranges.items() if 'array_range' in dim)
    return dict((c, slice(*r)) for c, r in array_ranges.items())


def no_data_block(shape, dtype, fill):
    arr = numpy.empty(shape, dtype)
    if fill is None:
        fill = numpy.NaN
    arr.fill(fill)
    return arr


def _convert_descriptor_to_query(descriptor=None):
    descriptor = descriptor or {}

    query = {key: descriptor[key] for key in ('satellite', 'sensor', 'product') if key in descriptor}
    variables = descriptor.get('variables', [])
    dimension_ranges = descriptor.get('dimensions', {}).copy()
    input_coord = {'left': None, 'bottom': None, 'right': None, 'top': None}
    input_crs = None
    mapped_vars = {}
    for dim, data in dimension_ranges.items():
        # Convert any known dimension CRS
        if dim in ['latitude', 'lat', 'y']:
            input_crs = input_crs or data.get('crs', 'EPSG:4326')
            input_coord['top'] = data['range'][0]
            input_coord['bottom'] = data['range'][1]
            mapped_vars['lat'] = dim
        elif dim in ['longitude', 'lon', 'long', 'x']:
            input_crs = input_crs or data.get('crs', 'EPSG:4326')
            input_coord['left'] = data['range'][0]
            input_coord['right'] = data['range'][1]
            mapped_vars['lon'] = dim
        elif dim in ['time']:
            # TODO: Handle time formatting strings & other CRS's
            # Assume dateime object or seconds since UNIX epoch 1970-01-01 for now...
            data['range'] = (datetime_to_timestamp(data['range'][0]), datetime_to_timestamp(data['range'][1]))
        else:
            # Assume the search function will sort it out, add it to the query
            query[dim] = Range(*data['range'])

    search_crs = 'EPSG:4326'  # TODO: look up storage index CRS for collection
    if all(v is not None for v in input_coord.values()):
        left, bottom, right, top = rasterio.warp.transform_bounds(input_crs, search_crs, **input_coord)
        query['lat'] = Range(bottom, top)
        query['lon'] = Range(left, right)
        dimension_ranges[mapped_vars['lat']]['range'] = (top, bottom)
        dimension_ranges[mapped_vars['lon']]['range'] = (left, right)

    return query, variables, dimension_ranges


class StorageUnitCollection(object):
    """Holds a list of storage units for some convenience functions"""

    def __init__(self, storage_units=None):
        self._storage_units = storage_units or []

    def append(self, storage_unit):
        self._storage_units.append(storage_unit)

    def get_storage_units(self):
        return self._storage_units

    def get_variables(self):
        variables = {}
        for storage_unit in self._storage_units:
            for variable_name, variable in storage_unit.variables.items():
                if len(variable.dimensions) == 3:
                    variables[variable_name] = variable
        return variables

    def get_variables_by_group(self):
        variables = {}
        for storage_unit in self._storage_units:
            for variable_name, variable in storage_unit.variables.items():
                variables.setdefault(variable.dimensions, {})[variable_name] = variable
        return variables

    def group_by_dimensions(self):
        dimension_group = {}
        for storage_unit in self._storage_units:
            dim_groups = list(set(variable.dimensions for variable in storage_unit.variables.values()))
            for dims in dim_groups:
                dimension_group.setdefault(dims, StorageUnitCollection()).append(storage_unit)
        return dimension_group

    def get_dimension_bounds(self, dimensions, dimension_ranges):
        """
        Get the min, max and array width of each dimension
        :param dimensions: a list of dimension names
        :param dimension_ranges: a dict of the ranges of any cropping that needs to occur {'dim_name': (min,max)}
        :return: {
                    'result_max': (<for each dim>),
                    'result_min': (<for each dim>),
                    'result_shape': (<for each dim>),
                 }
        """
        result = {
            'result_max': tuple(),
            'result_min': tuple(),
            'result_shape': tuple(),
        }
        for dim in dimensions:
            result_max = self.get_max(dim)
            result_min = self.get_min(dim)
            result_length = self.get_length(dim)
            if dim in dimension_ranges:
                lower_bounds = min(dimension_ranges[dim]['range'])
                upper_bounds = max(dimension_ranges[dim]['range'])
                unit_width = (result_max - result_min) / float(result_length)
                if lower_bounds > result_min:
                    result_length -= int((lower_bounds - result_min) / unit_width)
                    result_min = lower_bounds
                if upper_bounds < result_max:
                    result_length -= int((result_max - upper_bounds) / unit_width)
                    result_max = upper_bounds
            result['result_max'] += (result_max,)
            result['result_min'] += (result_min,)
            result['result_shape'] += (result_length,)
        return result

    def get_length(self, dim):
        length_index = {}
        for storage_unit in self._storage_units:
            index = storage_unit.coordinates[dim].begin
            length_index[index] = storage_unit.coordinates[dim].length
        return sum(length_index.values())

    def get_min(self, dim):
        return min(
            (min([storage_unit.coordinates[dim].begin, storage_unit.coordinates[dim].end])
             for storage_unit in self._storage_units if dim in storage_unit.coordinates)
        )

    def get_max(self, dim):
        return max(
            (max([storage_unit.coordinates[dim].begin, storage_unit.coordinates[dim].end])
             for storage_unit in self._storage_units if dim in storage_unit.coordinates)
        )


class API(object):
    def __init__(self, index=None):
        self.index = index or index_connect()

    def get_descriptor(self, descriptor_request=None):
        """
        :param descriptor_request:
        query_parameter = \
        {
        'storage_types': [ {
                'satellite': 'LANDSAT_8',
                'sensor': 'OLI_TIRS',
                'product': 'EODS_NBAR',
            }, {
                'satellite': 'LANDSAT_8',
                'sensor': 'OLI_TIRS',
                'product': 'EODS_NBAR',
            } ],
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
        query, _, dimension_ranges = _convert_descriptor_to_query(descriptor_request)

        sus = self.index.storage.search(**query)

        storage_units_by_type = {}
        for su in sus:
            stype = su.storage_mapping.match.metadata['platform']['code'] + '_' + \
                    su.storage_mapping.match.metadata['instrument']['name']
            # ptype = su.storage_mapping.match.metadata['product_type']
            storage_units_by_type.setdefault(stype, StorageUnitCollection()).append(make_storage_unit(su))

        descriptor = {}
        for stype, storage_units in storage_units_by_type.items():
            # Group by dimension
            storage_units_by_dimensions = storage_units.group_by_dimensions()
            for dimensions, grouped_storage_units in storage_units_by_dimensions.items():
                if len(dimensions) != 3:
                    continue

                result = descriptor.setdefault(stype, {
                    'dimensions': dimensions,
                    'storage_units': {},
                    'variables': {},
                    'result_min': None,
                    'result_max': None,
                    'result_shape': None,
                    'buffer_size': None,
                    'irregular_indices': None,
                })

                result.update(grouped_storage_units.get_dimension_bounds(dimensions, dimension_ranges))

                variables = grouped_storage_units.get_variables()
                for var_name, var in variables.items():
                    result['variables'][var_name] = {
                        'datatype': var.dtype,
                        'nodata': var.nodata,
                    }

                for storage_unit in grouped_storage_units.get_storage_units():
                    result['storage_units']['storage_min'] = tuple(min(storage_unit.coordinates[dim])
                                                                   for dim in dimensions)
                    result['storage_units']['storage_max'] = tuple(max(storage_unit.coordinates[dim])
                                                                   for dim in dimensions)
                    result['storage_units']['storage_shape'] = tuple(storage_unit.coordinates[dim].length
                                                                     for dim in dimensions)
        return descriptor

    def get_data(self, descriptor):
        """
        Function to return composite in-memory arrays
        :param descriptor:
        data_request = \
        {
        'satellite': 'LANDSAT_8',
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

        query, variables, dimension_ranges = _convert_descriptor_to_query(descriptor)
        sus = self.index.storage.search(**query)

        storage_units_by_type = {}
        for su in sus:
            stype = su.storage_mapping.match.metadata['platform']['code'] + '_' + \
                    su.storage_mapping.match.metadata['instrument']['name']
            ptype = su.storage_mapping.match.metadata['product_type']
            # TODO: group by storage type also?
            storage_units_by_type.setdefault(stype, {}).setdefault(ptype, []).append(make_storage_unit(su))

        if len(storage_units_by_type) > 1:
            raise RuntimeError('Data must come from a single storage')

        data_response = {'arrays': {}}
        for stype, products in storage_units_by_type.items():
            # TODO: check var names are unique accross products
            # for ptype, storage_units in products.items():
            #     pass

            storage_units = list(itertools.chain(*products.values()))
            dask_dict = self._get_data_from_storage_units(storage_units, variables, dimension_ranges)
            data_response.update(dask_dict)
        return data_response

    def _get_data_from_storage_units(self, storage_units, variables=None, dimension_ranges=None):
        if not dimension_ranges:
            dimension_ranges = {}
        if not len(storage_units):
            return {}

        variables_by_dimensions = {}
        for storage_unit in storage_units:
            for var_name, v in storage_unit.variables.items():
                if variables is None or var_name in variables:
                    variables_by_dimensions.setdefault(v.dimensions, {}).setdefault(var_name, []).append(storage_unit)

        dimension_group = {}
        for dimensions, sus_by_variable in variables_by_dimensions.items():
            dimension_group[dimensions] = self._get_data_by_variable(sus_by_variable, dimensions, dimension_ranges)
        if len(dimension_group) == 1:
            return dimension_group.values()[0]
        return dimension_group

    def _get_data_by_variable(self, storage_units_by_variable, dimensions, dimension_ranges):
        dimension_group_reponse = {
            'dimensions': dimensions,
            'arrays': {},
            'indices': [],
            'element_sizes': [],
            'coordinate_reference_systems': []
        }
        dim_vals = {}
        reverse_sort = {}
        sus_with_dims = set(itertools.chain(*storage_units_by_variable.values()))
        sample = list(sus_with_dims)[0]
        for dim in dimensions:
            # Get the start value of the storage unit so we can sort them
            # Some dims are stored upside down (eg Latitude), so sort the tiles consistant with the bounding box order
            reverse_sort[dim] = sample.coordinates[dim].begin > sample.coordinates[dim].end
            dim_vals[dim] = sorted(set(su.coordinates[dim].begin for su in sus_with_dims), reverse=reverse_sort[dim])
        sus_size = {}
        coord_lists = {}
        for su in sus_with_dims:
            for dim in dimensions:
                ordinal = dim_vals[dim].index(su.coordinates[dim].begin)
                sus_size.setdefault(dim, [None] * len(dim_vals[dim]))[ordinal] = su.coordinates[dim].length
                coord_list = coord_lists.setdefault(dim, [None] * len(dim_vals[dim]))
                if coord_list[ordinal] is None:
                    coord_list[ordinal] = su.get_coord(dim)

        coord_labels = {}
        for dim in dimensions:
            coord_labels[dim] = list(itertools.chain(*[x[0] for x in coord_lists[dim]]))
        # TODO: Move handling of timestamps down to the storage level
        if 'time' in dimensions:
            coord_labels['time'] = [datetime.datetime.fromtimestamp(c) for c in coord_labels['time']]
            if 'time' in dimension_ranges and 'range' in dimension_ranges['time']:
                dimension_ranges['time']['range'] = tuple(datetime.datetime.fromtimestamp(t)
                                                          for t in dimension_ranges['time']['range'])
        selectors = dimension_ranges_to_selector(dimension_ranges, reverse_sort)
        iselectors = dimension_ranges_to_iselector(dimension_ranges)

        for var_name, sus in storage_units_by_variable.items():
            xray_data_array = self._get_array(sus, var_name, dimensions, dim_vals, sus_size, coord_labels)
            cropped = xray_data_array.sel(**selectors)
            subset = cropped.isel(**iselectors)
            dimension_group_reponse['arrays'][var_name] = subset
        x = dimension_group_reponse['arrays'][dimension_group_reponse['arrays'].keys()[0]]
        dimension_group_reponse['indices'] = [x.coords[dim].values for dim in dimensions]
        dimension_group_reponse['element_sizes'] = list(x.shape)
        dimension_group_reponse['coordinate_reference_systems'] = list(x.shape)
        return dimension_group_reponse

    # def _create_response(self, storage_units_by_variable, dimensions, request, reverse_sort):
    #     """
    #
    #     :param storage_units_by_variable: a
    #     :param dimensions: list of dimension names
    #     :param request: The request descriptor containing the ranges of the desired dimensions
    #     :param reverse_sort: a dict[dim_name] -> bool of iif the dimension should be inversed
    #     :return: dict containing the response data
    #         {
    #             'arrays': ...,
    #             'indices': ...,
    #             'element_sizes': ...,
    #             'coordinate_reference_systems': ...,
    #             'dimensions': ...
    #         }
    #     """

    @staticmethod
    def _get_array(storage_units, var_name, dimensions, dim_vals, chunksize, coord_labels):
        """
        Create a dask array to call the underlying storage units
        :return dask array.
        """
        dsk_id = str(uuid.uuid1())  # unique name for the requested dask
        dsk = {}
        sample = storage_units[0]
        dtype = sample.variables[var_name].dtype
        nodata = sample.variables[var_name].nodata

        for storage_unit in storage_units:
            dsk_index = (dsk_id,)   # Dask is indexed by a tuple of ("Name", x-index pos, y-index pos, z-index pos, ...)
            for dim in dimensions:
                ordinal = dim_vals[dim].index(storage_unit.coordinates[dim].begin)
                dsk_index += (ordinal,)
            dsk[dsk_index] = (storage_unit.get, var_name)

        dsk_index = (dsk_id,)
        all_dsk_keys = set(itertools.product(dsk_index, *tuple(range(len(dim_vals[dim])) for dim in dimensions)))
        data_dsk_keys = dsk.viewkeys()
        missing_dsk_keys = all_dsk_keys - data_dsk_keys
        for key in missing_dsk_keys:
            coords = list(key)[1:]
            shape = tuple(operator.getitem(chunksize[dim], i) for dim, i in zip(dimensions, coords))
            dsk[key] = (no_data_block, shape, dtype, nodata)
        chunks = tuple(tuple(chunksize[dim]) for dim in dimensions)
        dask_array = da.Array(dsk, dsk_id, chunks)
        coords = [(dim, coord_labels[dim]) for dim in dimensions]
        xray_data_array = xray.DataArray(dask_array, coords=coords)
        return xray_data_array
