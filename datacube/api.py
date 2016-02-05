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

from __future__ import absolute_import, division, print_function

import logging
import datetime
import itertools
import operator
import uuid
import copy
import types

import functools
import numpy
import dask.array as da
import xarray
import rasterio.warp

from .model import Range
from .storage.access.core import StorageUnitDimensionProxy, StorageUnitBase
from datacube.model import Coordinate, Variable
from .storage.access.backends import NetCDF4StorageUnit, GeoTifStorageUnit, FauxStorageUnit
from .index import index_connect


_LOG = logging.getLogger(__name__)

FLOAT_TOLERANCE = 0.0000001 # TODO: Use

# TODO: Move into storage.access.StorageUnitBase
# class NDArrayProxy(object):
#     def __init__(self, storage_unit, var_name):
#         self._storage_unit = storage_unit
#         self._var_name = var_name
#
#     @property
#     def ndim(self):
#         return len(self._storage_unit.coordinates)
#
#     @property
#     def size(self):
#         return functools.reduce(operator.mul, [coord.length for coord in self._storage_unit.coordinates])
#
#     @property
#     def dtype(self):
#         return self._storage_unit.variables[self._var_name].dtype
#
#     @property
#     def shape(self):
#         return tuple(coord.length for coord in self._storage_unit.coordinates)
#
#     def __len__(self):
#         return self.shape[0]
#
#     def __array__(self, dtype=None):
#         return self._storage_unit.get(self._var_name)
#
#     def __getitem__(self, key):
#         return self._storage_unit.get_chunk(self._var_name, key)
#
#     def __repr__(self):
#         return '%s(array=%r)' % (type(self).__name__, self.shape)


# TODO: Confirm that we don't need this code
# def get_storage_unit_transform(su):
#     storage_type = su.attributes['storage_type']
#     return [su.coordinates['longitude'].begin, storage_type['resolution']['x'], 0.0,
#             su.coordinates['latitude'].begin, 0.0, storage_type['resolution']['y']]
#
#
# def get_storage_unit_projection(su):
#     storage_type = su.attributes['storage_type']
#     return storage_type['projection']['spatial_ref']


def make_in_memory_storage_unit(su, coordinates, variables, attributes, crs):
    faux = MemoryStorageUnit(file_path=su.local_path,
                             coordinates=coordinates,
                             variables=variables,
                             attributes=attributes,
                             crs=crs)

    # TODO: Retrive from database instead of opening file
    irregular_dim_names = ['time', 't']  # TODO: Use irregular flag from database instead
    irregular_dims = [name for name, coord in coordinates.items()
                      if name in irregular_dim_names and coord.length > 2]

    if irregular_dims and su.storage_type.driver == 'NetCDF CF':
        real_su = NetCDF4StorageUnit(su.local_path,
                                     coordinates=coordinates, variables=variables, attributes=attributes)
        for coord in irregular_dims:
            coord_values, _ = real_su.get_coord(coord)
            faux.coodinate_values[coord] = coord_values
    return faux


def make_storage_unit(su, is_diskless=False):
    """convert search result into StorageUnit object
    :param su: database index storage unit
    :param is_diskless: Use a cached object for the source of data, rather than the file
    """
    storage_type = su.storage_type.definition
    crs = dict((dim, su.descriptor['coordinates'][dim].get('units', None)) for dim in storage_type['dimension_order'])
    for dim in crs.keys():
        if dim in su.storage_type.spatial_dimensions:
            crs[dim] = storage_type['crs']
    coordinates = su.coordinates
    variables = {
        attributes['varname']: Variable(
            dtype=numpy.dtype(attributes['dtype']),
            nodata=attributes.get('nodata', None),
            dimensions=storage_type['dimension_order'],
            units=attributes.get('units', None))
        for attributes in su.storage_type.measurements.values()
    }
    attributes = {
        'storage_type': storage_type
    }
    attributes.update(su.storage_type.match.metadata)

    if is_diskless:
        return make_in_memory_storage_unit(su,
                                           coordinates=coordinates,
                                           variables=variables,
                                           attributes=attributes,
                                           crs=crs)

    if su.storage_type.driver == 'NetCDF CF':
        return NetCDF4StorageUnit(su.local_path, coordinates=coordinates, variables=variables, attributes=attributes)

    if su.storage_type.driver == 'GeoTiff':
        result = GeoTifStorageUnit(su.local_path, coordinates=coordinates, variables=variables, attributes=attributes)
        time = datetime.datetime.strptime(su.descriptor['extents']['time_min'], '%Y-%m-%dT%H:%M:%S.%f')
        time = (time - datetime.datetime.utcfromtimestamp(0)).total_seconds()
        return StorageUnitDimensionProxy(result, ('time', time, numpy.float64, 'seconds since 1970'))

    raise RuntimeError('unsupported storage unit access driver %s' % su.storage_type.driver)


def datetime_to_timestamp(dt):
    if isinstance(dt, datetime.datetime) or isinstance(dt, datetime.date):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return (dt - epoch).total_seconds()
    return dt


def dimension_ranges_to_selector(dimension_ranges, reverse_sort):
    ranges = dict((dim_name, dim['range']) for dim_name, dim in dimension_ranges.items() if 'range' in dim)
    # if 'time' in ranges:
    #     ranges['time'] = tuple(datetime_to_timestamp(r) for r in ranges['time'])
    return dict((c, slice(*sorted(r, reverse=reverse_sort[c])) if isinstance(r, tuple) else r) for c, r in ranges.items())


def dimension_ranges_to_iselector(dim_ranges):
    array_ranges = dict((dim_name, dim['array_range']) for dim_name, dim in dim_ranges.items() if 'array_range' in dim)
    #TODO: Check if 'end' of array range should be inclusive or exclusive. Prefer exclusive to match with slice
    return dict((c, slice(*r)) for c, r in array_ranges.items())


def no_data_block(shape, dtype, fill):
    arr = numpy.empty(shape, dtype)
    if fill is None:
        fill = numpy.NaN
    arr.fill(fill)
    return arr


class StorageUnitCollection(object):
    """Holds a list of storage units for some convenience functions"""

    def __init__(self, storage_units=None):
        self._storage_units = storage_units or []

    def append(self, storage_unit):
        self._storage_units.append(storage_unit)

    def get_storage_units(self):
        return self._storage_units

    def get_storage_unit_stats(self, dimensions):
        stats = {}
        for storage_unit in self._storage_units:
            index = tuple(storage_unit.coordinates[dim].begin for dim in dimensions)
            stats[index] = {
                'storage_min': tuple(min(storage_unit.coordinates[dim].begin, storage_unit.coordinates[dim].end)
                                     for dim in dimensions),
                'storage_max': tuple(max(storage_unit.coordinates[dim].begin, storage_unit.coordinates[dim].end)
                                     for dim in dimensions),
                'storage_shape': tuple(storage_unit.coordinates[dim].length for dim in dimensions),
                'storage_path': str(storage_unit.file_path)
            }
        return stats

    def get_variables(self):
        variables = {}
        for storage_unit in self._storage_units:
            for variable_name, variable in storage_unit.variables.items():
                if len(variable.dimensions) == 3:
                    variables[variable_name] = variable
        return variables

    def group_by_dimensions(self):
        dimension_group = {}
        for storage_unit in self._storage_units:
            dim_groups = list(set(tuple(variable.dimensions) for variable in storage_unit.variables.values()))
            for dims in dim_groups:
                dimension_group.setdefault(dims, StorageUnitCollection()).append(storage_unit)
        return dimension_group


def _get_dimension_properties(storage_units, dimensions):
    dim_props = {
        'sus_size': {},
        'coord_labels': {},
        'dim_vals': {},
    }
    sample = list(storage_units)[0]
    # Get the start value of the storage unit so we can sort them
    # Some dims are stored upside down (eg Latitude), so sort the tiles consistant with the bounding box order
    dim_props['reverse'] = dict((dim, sample.coordinates[dim].begin > sample.coordinates[dim].end)
                                for dim in dimensions)
    for dim in dimensions:
        dim_props['dim_vals'][dim] = sorted(set(su.coordinates[dim].begin for su in storage_units),
                                            reverse=dim_props['reverse'][dim])
    dim_props['sus_size'] = {}
    coord_lists = {}
    for su in storage_units:
        for dim in dimensions:
            dim_val_len = len(dim_props['dim_vals'][dim])
            ordinal = dim_props['dim_vals'][dim].index(su.coordinates[dim].begin)
            dim_props['sus_size'].setdefault(dim, [None] * dim_val_len)[ordinal] = su.coordinates[dim].length
            coord_list = coord_lists.setdefault(dim, [None] * dim_val_len)
            # We only need the coords once, so don't open up every file if we don't need to - su.get_coord()
            if coord_list[ordinal] is None:
                coord_list[ordinal], _ = su.get_coord(dim)
    for dim in dimensions:
        dim_props['coord_labels'][dim] = list(itertools.chain(*coord_lists[dim]))
    return dim_props


def _get_extra_properties(storage_units, dimensions):
    sample_su_crs = list(storage_units)[0].get_crs()
    extra_properties = {
        'coordinate_reference_systems': [sample_su_crs[dim] for dim in dimensions],
    }
    return extra_properties


def _create_response(xarrays, dimensions, extra_properties):
    """
    :param xarrays: a dict of xarray.DataArrays
    :param dimensions: list of dimension names
    :return: dict containing the response data
        {
            'arrays': ...,
            'indices': ...,
            'element_sizes': ...,
            'dimensions': ...
        }
    """
    sample_xarray = list(xarrays.values())[0]
    response = {
        'dimensions': list(dimensions),
        'arrays': xarrays,
        'indices': dict((dim, sample_xarray.coords[dim].values) for dim in dimensions),
        'element_sizes': [(abs(sample_xarray.coords[dim].values[0] - sample_xarray.coords[dim].values[-1]) /
                           float(sample_xarray.coords[dim].size)) if sample_xarray.coords[dim].size > 1 else 0
                          for dim in dimensions],
        'size': sample_xarray.shape,
    }
    response.update(extra_properties)
    return response


def make_nodata_func(storage_units, var_name, dimensions, chunksize):
    sample = storage_units[0]
    dtype = sample.variables[var_name].dtype
    nodata = sample.variables[var_name].nodata

    def make_nodata_dask(key):
        coords = list(key)[1:]
        shape = tuple(operator.getitem(chunksize[dim], i) for dim, i in zip(dimensions, coords))
        return no_data_block, shape, dtype, nodata

    return make_nodata_dask


# def get_chunked_data_func(storage_unit, var_name):
#     # TODO: Provide dask array to chunked NetCDF calls
#     return NDArrayProxy(storage_unit, var_name)


def _get_dask_for_storage_units(storage_units, var_name, dimensions, dim_vals, dsk_id):
    dsk = {}
    for storage_unit in storage_units:
        dsk_index = (dsk_id,)   # Dask is indexed by a tuple of ("Name", x-index pos, y-index pos, z-index pos, ...)
        for dim in dimensions:
            ordinal = dim_vals[dim].index(storage_unit.coordinates[dim].begin)
            dsk_index += (ordinal,)
        # TODO: Wrap in a chunked dask for sub-file dask chunks
        dsk[dsk_index] = (storage_unit.get, var_name)
        # dsk[dsk_index] = (get_chunked_data_func, storage_unit, var_name)
    return dsk


def _fill_in_dask_blanks(dsk, storage_units, var_name, dimensions, dim_props, dsk_id):
    nodata_dsk = make_nodata_func(storage_units, var_name, dimensions, dim_props['sus_size'])

    all_dsk_keys = set(itertools.product((dsk_id,), *[[i for i, _ in enumerate(dim_props['dim_vals'][dim])]
                                                      for dim in dimensions]))
    missing_dsk_keys = all_dsk_keys - set(dsk.keys())

    for key in missing_dsk_keys:
        dsk[key] = nodata_dsk(key)
    return dsk


def _get_array(storage_units, var_name, dimensions, dim_props):
    """
    Create an xarray.DataArray
    :return xarray.DataArray
    """
    dsk_id = str(uuid.uuid1())  # unique name for the requested dask
    dsk = _get_dask_for_storage_units(storage_units, var_name, dimensions, dim_props['dim_vals'], dsk_id)
    _fill_in_dask_blanks(dsk, storage_units, var_name, dimensions, dim_props, dsk_id)

    dtype = storage_units[0].variables[var_name].dtype
    chunks = tuple(tuple(dim_props['sus_size'][dim]) for dim in dimensions)
    dask_array = da.Array(dsk, dsk_id, chunks, dtype=dtype)
    coords = [(dim, dim_props['coord_labels'][dim]) for dim in dimensions]
    xarray_data_array = xarray.DataArray(dask_array, coords=coords)
    return xarray_data_array


def _fix_custom_dimensions(dimensions, dim_props):
    dimension_ranges = dim_props['dimension_ranges']
    coord_labels = dim_props['coord_labels']
    # TODO: Move handling of timestamps down to the storage level
    if 'time' in dimensions:
        coord_labels['time'] = [datetime.datetime.fromtimestamp(c) for c in coord_labels['time']]
        if 'time' in dimension_ranges and 'range' in dimension_ranges['time']:
            dimension_ranges['time']['range'] = tuple(datetime.datetime.fromtimestamp(t)
                                                      for t in dimension_ranges['time']['range'])
    return dimension_ranges, coord_labels


def _get_data_by_variable(storage_units_by_variable, dimensions, dimension_ranges):
    sus_with_dims = set(itertools.chain(*storage_units_by_variable.values()))
    dim_props = _get_dimension_properties(sus_with_dims, dimensions)
    dim_props['dimension_ranges'] = dimension_ranges

    _fix_custom_dimensions(dimensions, dim_props)
    selectors = dimension_ranges_to_selector(dim_props['dimension_ranges'], dim_props['reverse'])
    iselectors = dimension_ranges_to_iselector(dim_props['dimension_ranges'])

    xarrays = {}
    for var_name, storage_units in storage_units_by_variable.items():
        xarray_data_array = _get_array(storage_units, var_name, dimensions, dim_props)
        for key, value in selectors.items():
            if isinstance(value, slice):
                xarray_data_array = xarray_data_array.sel(**{key: value})
            else:
                xarray_data_array = xarray_data_array.sel(method='nearest', **{key: value})
        subset = xarray_data_array.isel(**iselectors)
        xarrays[var_name] = subset
    extra_properties = _get_extra_properties(sus_with_dims, dimensions)
    return _create_response(xarrays, dimensions, extra_properties)


def _stratify_storage_unit(storage_unit, dimension):
    """
    Creates a new series of storage units for every index along an irregular dimension that must be merged together
    :param storage_unit: A storage unit
    :param dimension: The name of the irregular dimension to stratify
    :return: storage_units: list of storage_unit-like objects that point to an underlying storage unit at a particular
     value, one for each value of the irregular dimension
    """
    coord, index = storage_unit.get_coord(dimension)
    if len(coord) > 1:
        return [IrregularStorageUnitSlice(storage_unit, dimension, i) for i, c in enumerate(coord)]
    return [storage_unit]


def _stratify_irregular_dimension(storage_units, dimension):
    """
    Creates a new series of storage units for every index along an irregular dimension that must be merged together
    :param storage_units:
    :param dimension:
    :return: storage_units: list of storage_unit-like objects that point to an underlying storage unit at a particular
     value, one for each value of the irregular dimension
    """
    stratified_storage_units = [_stratify_storage_unit(storage_unit, dimension) for storage_unit in storage_units]
    return list(itertools.chain(*stratified_storage_units))


class MemoryStorageUnit(FauxStorageUnit):
    def __init__(self, coordinates, variables, attributes=None, coodinate_values=None, crs=None, file_path=None):
        super(MemoryStorageUnit, self).__init__(coordinates, variables)
        self.crs = crs or {}
        self.coodinate_values = coodinate_values or {}
        self.attributes = attributes or {}
        self.file_path = file_path

    def _get_coord(self, name):
        if name in self.coodinate_values:
            return self.coodinate_values[name]
        return super(MemoryStorageUnit, self)._get_coord(name)

    def get_crs(self):
        crs = dict((dim, {'reference_system_unit': coord.units}) for dim, coord in self.coordinates.items())
        for coord, value in self.crs.items():
            if isinstance(coord, tuple):  # Flatten grid_mappings into per-coord units
                for c in coord:
                    crs[c]['reference_system_definition'] = value
            else:
                crs[coord]['reference_system_definition'] = value
        return crs


class IrregularStorageUnitSlice(StorageUnitBase):
    """ Storage Unit interface for accessing another Storage unit at a defined coordinate  """
    def __init__(self, parent, dimension, index):
        self._parent = parent
        self._sliced_coordinate = dimension
        self._index = index
        self.coordinates = copy.copy(parent.coordinates)
        real_dim = self.coordinates[dimension]
        self._cached_coord, _ = parent.get_coord(dimension, index=slice(self._index, self._index + 1))
        fake_dim = Coordinate(dtype=real_dim.dtype,
                              begin=self._cached_coord[0],
                              end=self._cached_coord[0],
                              length=1,
                              units=real_dim.units)
        self.coordinates[dimension] = fake_dim
        self.variables = parent.variables
        self.file_path = parent.file_path

    def get_crs(self):
        return self._parent.get_crs()

    def get_coord(self, name, index=None):
        if name == self._sliced_coordinate:
            return self._cached_coord, slice(0, 1, 1)
        return self._parent.get_coord(name, index)

    def _fill_data(self, name, index, dest):
        var = self.variables[name]
        subset_slice = {self._sliced_coordinate:slice(self._index, self._index+1)}
        data = self._parent.get(name, **subset_slice)
        numpy.copyto(dest, data.data)


def _get_data_from_storage_units(storage_units, variables=None, dimension_ranges=None):
    if not dimension_ranges:
        dimension_ranges = {}
    if not len(storage_units):
        return {}

    variables_by_dimensions = {}
    for storage_unit in storage_units:
        for var_name, v in storage_unit.variables.items():
            if variables is None or var_name in variables:
                dims = tuple(v.dimensions)
                variables_by_dimensions.setdefault(dims, {}).setdefault(var_name, []).append(storage_unit)

    dimension_group = {}
    for dimensions, sus_by_variable in variables_by_dimensions.items():
        dimension_group[dimensions] = _get_data_by_variable(sus_by_variable, dimensions, dimension_ranges)
    if len(dimension_group) == 1:
        return list(dimension_group.values())[0]
    return dimension_group


def to_single_value(data_array):
    if data_array.size != 1:
        return data_array
    if isinstance(data_array.values, numpy.ndarray):
        return data_array.item()
    return data_array.values


def get_result_stats(storage_units, dimension_ranges):
    strata_storage_units = _stratify_irregular_dimension(storage_units, 'time')
    storage_data = _get_data_from_storage_units(strata_storage_units, dimension_ranges=dimension_ranges)
    example = storage_data['arrays'][list(storage_data['arrays'].keys())[0]]
    result = {
        'result_shape': example.shape,
        'result_min': tuple(to_single_value(example[dim].min()) if example[dim].size > 0 else numpy.NaN
                            for dim in example.dims),
        'result_max': tuple(to_single_value(example[dim].max()) if example[dim].size > 0 else numpy.NaN
                            for dim in example.dims),
    }
    irregular_dim_names = ['time', 't']  # TODO: Use irregular flag from database instead
    result['irregular_indices'] = dict((dim, example[dim].values)
                                       for dim in example.dims if dim in irregular_dim_names)
    return result


def _dimension_crs_to_ranges_query(dimension_ranges_descriptor):
    dimension_ranges = dimension_ranges_descriptor.copy()
    query = {}
    input_coord = {'left': None, 'bottom': None, 'right': None, 'top': None}
    input_crs = None
    mapped_vars = {}
    for dim, data in dimension_ranges.items():
        # Convert any known dimension CRS
        if dim in ['latitude', 'lat', 'y']:
            if 'range' in data:
                input_crs = input_crs or data.get('crs', 'EPSG:4326')
                if isinstance(data['range'], types.StringTypes + (int, float)):
                    input_coord['top'] = float(data['range'])
                    input_coord['bottom'] = float(data['range'])
                else:
                    input_coord['top'] = data['range'][0]
                    input_coord['bottom'] = data['range'][-1]
                mapped_vars['lat'] = dim
        elif dim in ['longitude', 'lon', 'long', 'x']:
            if 'range' in data:
                input_crs = input_crs or data.get('crs', 'EPSG:4326')
                if isinstance(data['range'], types.StringTypes + (int, float)):
                    input_coord['left'] = float(data['range'])
                    input_coord['right'] = float(data['range'])
                else:
                    input_coord['left'] = data['range'][0]
                    input_coord['right'] = data['range'][-1]
                mapped_vars['lon'] = dim
        elif dim in ['time']:
            # TODO: Handle time formatting strings & other CRS's
            # Assume dateime object or seconds since UNIX epoch 1970-01-01 for now...
            if 'range' in data:
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

        if bottom == top:
            query['lat'] = Range(bottom + FLOAT_TOLERANCE, top - FLOAT_TOLERANCE)
            dimension_ranges[mapped_vars['lat']]['range'] = top
        if left == right:
            query['lon'] = Range(left - FLOAT_TOLERANCE, right + FLOAT_TOLERANCE)
            dimension_ranges[mapped_vars['lon']]['range'] = left
    return query, dimension_ranges


class API(object):
    def __init__(self, index=None):
        self.index = index or index_connect()

    def _convert_descriptor_to_query(self, descriptor=None):
        descriptor = descriptor or {}

        known_fields = self.index.datasets.get_fields().keys()
        query = {key: descriptor[key] for key in descriptor.keys() if key in known_fields}

        unknown_fields = [key for key in descriptor.keys()
                          if key not in known_fields and key not in ['variables', 'dimensions']]
        if unknown_fields:
            _LOG.warning("Some of the fields in the query are unknown and will be ignored: %s",
                         ', '.join(unknown_fields))

        variables = descriptor.get('variables', None)
        dimension_ranges_descriptor = descriptor.get('dimensions', {})
        range_query, dimension_ranges = _dimension_crs_to_ranges_query(dimension_ranges_descriptor)
        query.update(range_query)

        return query, variables, dimension_ranges

    def _get_storage_units(self, descriptor_request):
        query, variables, dimension_ranges = self._convert_descriptor_to_query(descriptor_request)

        sus = self.index.storage.search(**query)

        storage_units_by_type = {}
        for su in sus:
            unit = make_storage_unit(su, is_diskless=True)
            storage_units_by_type.setdefault(su.storage_type.name, StorageUnitCollection()).append(unit)
        return storage_units_by_type, query, variables, dimension_ranges

    def get_descriptor(self, descriptor_request=None, include_storage_units=True):
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
        :param include_storage_units: Include the list of storage units
        :return: descriptor = {
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
        storage_units_by_type, query, _, dimension_ranges = self._get_storage_units(descriptor_request)
        descriptor = {}
        for stype, storage_units in storage_units_by_type.items():
            # Group by dimension
            storage_units_by_dimensions = storage_units.group_by_dimensions()
            for dimensions, grouped_storage_units in storage_units_by_dimensions.items():
                # TODO: Either filter our undesired variables or return everything
                if len(dimensions) != 3:
                    continue

                result = descriptor.setdefault(stype, {
                    'dimensions': list(dimensions),
                    'storage_units': {},
                    'variables': {},
                    'result_min': None,
                    'result_max': None,
                    'result_shape': None,
                    'irregular_indices': None,
                })
                for var_name, var in grouped_storage_units.get_variables().items():
                    result['variables'][var_name] = {
                        'datatype_name': var.dtype,
                        'nodata_value': var.nodata,
                    }
                if include_storage_units:
                    result['storage_units'] = grouped_storage_units.get_storage_unit_stats(dimensions)
                result.update(get_result_stats(grouped_storage_units.get_storage_units(), dimension_ranges))
        return descriptor

    def get_data(self, descriptor=None, storage_units=None):
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
        :param storage_units:
        :return: data_response = \
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

        query, variables, dimension_ranges = self._convert_descriptor_to_query(descriptor)

        storage_units_by_type = {}
        if storage_units:
            stype = 'Requested Storage'
            storage_units_by_type[stype] = [NetCDF4StorageUnit.from_file(su['storage_path'])
                                            for su in storage_units.values()]
        else:
            sus = self.index.storage.search(**query)
            for su in sus:
                stype = su.storage_type.name
                storage_units_by_type.setdefault(stype, []).append(make_storage_unit(su))

        if len(storage_units_by_type) > 1:
            raise RuntimeError('Data must come from a single storage')

        data_response = {'arrays': {}}
        for stype, storage_units in storage_units_by_type.items():
            # TODO: check var names are unique accross products
            storage_units = _stratify_irregular_dimension(storage_units, 'time')
            storage_data = _get_data_from_storage_units(storage_units, variables, dimension_ranges)

            data_response.update(storage_data)
        return data_response


def main():
    agdc_api = API()
    desc = agdc_api.get_descriptor()
    print(desc)


if __name__ == '__main__':
    main()
