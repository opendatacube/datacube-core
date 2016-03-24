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
Storage Query and Access API module
"""

from __future__ import absolute_import, division, print_function

import logging
import datetime
import itertools
from collections import defaultdict

import numpy
import xarray
from dateutil import tz

from datacube.index import index_connect
from datacube.compat import string_types

from ._conversion import convert_descriptor_query_to_search_query, convert_descriptor_dims_to_selector_dims
from ._conversion import convert_request_args_to_descriptor_query
from ._conversion import dimension_ranges_to_selector, dimension_ranges_to_iselector, to_datetime
from ._dask import get_dask_array
from ._storage import StorageUnitCollection
from ._storage import make_storage_unit, make_storage_unit_collection_from_descriptor
from ._stratify import _stratify_irregular_dimension

_LOG = logging.getLogger(__name__)


class API(object):
    def __init__(self, index=None):
        """
        Creates the interface for the query and storage access.
        If no index is given, the default configuration is used for database connection, etc.

        :param index: The database index to use.
        :type index: from :py:class:`datacube.index.index_connect` or None
        """
        self.index = index or index_connect()

    def get_descriptor(self, descriptor_request=None, include_storage_units=True):
        """
        Gets the metadata for a `AnalyticsEngine` query.

        :param descriptor_request: The request query, formatted as:
            ::
                descriptor_request = {
                    'platform': 'LANDSAT_8',
                    'product': 'NBAR',
                    'dimensions': {
                        'x': {
                            'range': (140, 142),
                            'crs': 'EPSG:4326'
                        },
                        'y': {
                            'range': (-36, -35),
                            'crs': 'EPSG:4326'
                        },
                        'time': {
                            'range': ((1990, 6, 1), (1992, 7 ,1)),
                        }
                    },
                }

        :type descriptor_request: dict or None
        :param include_storage_units: Include the list of storage units
        :type include_storage_units: bool, optional
        :return: A descriptor dict of the query, containing the metadata of the request
            ::
                descriptor = {
                    'ls5_nbar_albers': { # storage_type identifier
                        'dimensions': ['x', 'y', 'time'],
                        'variables': { # Variables which can be accessed as arrays
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
                            }
                        },
                        'result_min': (140, -36, 1293840000),
                        'result_max': (141, -35, 1325376000),
                        'result_shape': (8000, 8000, 40), # Overall size of result set
                        'irregular_indices': {
                            # Regularly indexed dimensions (e.g. x & y) won't be specified
                            'time': date_array # Array of days since 1/1/1970
                        },
                        'storage_units': {
                            (140, -36, 1990): { # Storage unit indices
                                'storage_min': (140, -36, 1293840000),
                                'storage_max': (141, -35, 1293800400),
                                'storage_shape': (4000, 4000, 24),
                                'storage_path': '/path/to/storage/units/nbar_140_-36_1990.nc',
                            },
                            (140, -36, 1991): { # Storage unit indices
                                'storage_min': (140, -36, 1293800400),
                                'storage_max': (141, -35, 1325376000),
                                'storage_shape': (4000, 4000, 23),
                                'storage_path': '/path/to/storage/units/nbar_140_-36_1991.nc',
                            },
                            # ...
                            # <more storage_unit sub-descriptors>
                            # ...
                        },
                        # ...
                        # <more storage unit type sub-descriptors>
                        # ...
                    }
                }

        :rtype: dict
        """
        descriptor_request = descriptor_request or {}
        storage_units_by_type = _get_storage_units(descriptor_request, self.index, is_diskless=True)
        variables = descriptor_request.get('variables', None)
        descriptor = {}
        for stype, storage_units in storage_units_by_type.items():
            # Group by dimension
            dimension_ranges = convert_descriptor_dims_to_selector_dims(descriptor_request.get('dimensions', {}),
                                                                        storage_units.get_spatial_crs())
            storage_units_by_dimensions = storage_units.group_by_dimensions()
            for dimensions, grouped_storage_units in storage_units_by_dimensions.items():
                result = descriptor.setdefault(stype, {
                    'dimensions': list(dimensions),
                    'variables': {},
                    'result_min': None,
                    'result_max': None,
                    'result_shape': None,
                    'irregular_indices': None,
                })
                for var_name, var in grouped_storage_units.get_variables().items():
                    if variables is None or var_name in variables:
                        result['variables'][var_name] = {
                            'datatype_name': var.dtype,
                            'nodata_value': var.nodata,
                        }
                if include_storage_units:
                    result['storage_units'] = grouped_storage_units.get_storage_unit_stats(dimensions)
                result.update(_get_result_stats(grouped_storage_units.get_storage_units(),
                                                dimensions, dimension_ranges))
        return descriptor

    def get_data(self, descriptor=None, storage_units=None):
        """
        Gets the data for a `ExecutionEngine` query.
        Function to return composite in-memory arrays.

        :param descriptor: A dictionary containing the query parameters
            ::
                descriptor = {
                    'platform': 'LANDSAT_8',
                    'product': 'NBAR',
                    # <search_field>: <search value>,
                    'variables': ('B30', 'B40'),
                    'dimensions': {
                        'x': {
                            'range': (140, 142),
                            'array_range': (0, 127),
                            'crs': 'EPSG:4326'
                        },
                        'y': {
                            'range': (-36, -35),
                            'array_range': (0, 127),
                            'crs': 'EPSG:4326'
                        },
                        'time': {
                            'range': (1293840000, 1325376000),
                            'array_range': (0, 127)
                        }
                    },
                }

        :type descriptor: dict or None

        :param storage_units:
            Limit the query to the given storage unit descriptors, as given by the :py:meth:`.get_descriptor` method.

        :type storage_units: list or None

        :return: A dict containing the arrays, dimensions, indices, element_sizes and coordinate_reference_systems of
            the query.
            ::
                data = {
                    'dimensions': ['x', 'y', 'time'],
                    'arrays': {
                        # All of these will have the same shape
                        'B30': 'xarray.DataArray',
                        'B40': 'xarray.DataArray',
                    },
                    'indices': [
                        # Actual x, y & t (long, lat & time) values for each array index
                        '<numpy array of x indices>',
                        '<numpy array of y indices>',
                        '<numpy array of time indices>'
                    ],
                    'element_sizes': [
                        # Element sizes for each dimension
                        '< x element size>',
                        '< y element size>',
                        '< time element size>'
                    ],
                    'coordinate_reference_systems': [
                        # These will be the coordinate_reference_systems for each dimension
                        '< x CRS>',
                        '< y CRS>',
                        '< time CRS>'
                    ],
                }

        :rtype: dict
        """
        descriptor = descriptor or {}
        variables = descriptor.get('variables', None)

        storage_units_by_type = defaultdict(StorageUnitCollection)
        if storage_units:
            stype = 'Requested Storage'
            storage_units_by_type[stype] = make_storage_unit_collection_from_descriptor(storage_units)
        else:
            storage_units_by_type = _get_storage_units(descriptor, self.index)

        if len(storage_units_by_type) > 1:
            # TODO: Work out if they share the same grid (projection & resolution), and combine into same output?
            # TODO: Reproject all into the requested grid
            raise RuntimeError('Data must come from a single storage')

        data_response = {}
        for stype, storage_unit_collection in storage_units_by_type.items():
            # TODO: check var names are unique accross products
            storage_crs = storage_unit_collection.get_spatial_crs()
            dimension_descriptor = descriptor.get('dimensions', {})
            dimension_ranges = convert_descriptor_dims_to_selector_dims(dimension_descriptor, storage_crs)
            dimension_groups = _get_data_from_storage_units(storage_unit_collection.get_storage_units(),
                                                            variables, dimension_ranges)
            #data_response[stype] = _create_data_response(xarrays)
            # TODO: Return multiple storages, or wait until we suppport reprojection
            # Right now, just return the result with the largest number of dimensions
            dimension_groups.sort(key=lambda x: len(x[1]), reverse=True)
            xarrays, dimensions = dimension_groups[0]
            return _create_data_response(xarrays, dimensions)
        return data_response

    def get_data_array(self, variables=None, var_dim_name=u'variable', set_nan=True, **kwargs):
        """
        Gets a stacked xarray.DataArray for the requested variables.
        This stacks the data similar to `numpy.dstack`.

        :param variables: Variables to be included. Use `None` to include all available variables
        :type variables: list or None
        :param var_dim_name: dimension name that the variables will be stacked
        :param set_nan: Set "no data" values to `numpy.NaN`.

            *Note:* this will cause the data to be converted to float dtype.
        :type set_nan: bool

        :param * * kwargs: search parameters and dimension ranges.
            ::
                product='NBAR', platform='LANDSAT_5', latitude=(-35.5, -34.5)

        :return: Data with all variables stacked along a dimension.
        :rtype: xarray.DataArray
        """
        descriptor_request = convert_request_args_to_descriptor_query(kwargs, self.index)
        descriptor_dimensions = descriptor_request.get('dimensions', {})
        variables = [variables] if isinstance(variables, string_types) else variables

        query = convert_descriptor_query_to_search_query(descriptor_request)
        storage_units_by_type = defaultdict(StorageUnitCollection)
        su_id = set()
        for su in self.index.storage.search_eager(**query):
            if su.id not in su_id:
                su_id.add(su.id)
                storage_units_by_type[su.storage_type.name].append(make_storage_unit(su))

        for stype, storage_units in storage_units_by_type.items():
            dimension_ranges = convert_descriptor_dims_to_selector_dims(descriptor_dimensions,
                                                                        storage_units.get_spatial_crs())
            data_dicts = _get_data_from_storage_units(storage_units.iteritems(), variables, dimension_ranges,
                                                      set_nan=set_nan)
            if len(data_dicts) and len(data_dicts[0]):
                data_dict = data_dicts[0][0]
                return _stack_vars(data_dict, var_dim_name, stack_name=stype)
            # for i, (data_dict, _) in enumerate(data_dicts):
            #     #stype_label = '{}.{}'.format(stype, i) if len(data_dicts) > 1 else stype
            #     return _stack_vars(data_dict, var_dim_name, stack_name=stype)
        return None

    def get_dataset(self, variables=None, set_nan=False, **kwargs):
        """
        Gets an xarray.Dataset for the requested data.

        :param variables: variable or list of variables to be included.
                Use `None` to include all available variables (default)
        :type variables: list(str) or str, optional
        :param set_nan: If any "no data" values should be set to `numpy.NaN`
            *Note:* this will cause the data to be cast to a float dtype.
        :type set_nan: bool, optional
        :param kwargs: search parameters and dimension ranges
            E.g.::
                product='NBAR', platform='LANDSAT_5', latitude=(-35.5, -34.5)
        :return: Data as variables with shared coordinate dimensions.
        :rtype: xarray.Dataset
        """
        descriptor_request = convert_request_args_to_descriptor_query(kwargs, self.index)
        descriptor_dimensions = descriptor_request.get('dimensions', {})
        variables = [variables] if isinstance(variables, string_types) else variables

        query = convert_descriptor_query_to_search_query(descriptor_request)
        storage_units_by_type = defaultdict(StorageUnitCollection)
        storage_unit_types = {}
        for su in self.index.storage.search(**query):
            storage_units_by_type[su.storage_type.name].append(make_storage_unit(su))
            storage_unit_types[su.storage_type.name] = su.storage_type

        #TODO: return multiple storage types if compatible
        # or warp / reproject / resample if required?
        # including realigning timestamps
        # and dealing with each storage unit having an extra_metadata field...
        for stype, storage_units in storage_units_by_type.items():
            storage_unit_type = storage_unit_types[stype]
            dimension_ranges = convert_descriptor_dims_to_selector_dims(descriptor_dimensions,
                                                                        storage_units.get_spatial_crs())
            data_dicts = _get_data_from_storage_units(storage_units.iteritems(), variables,
                                                      dimension_ranges, set_nan=set_nan)
            return _make_xarray_dataset(data_dicts, storage_unit_type)
        return xarray.Dataset()

    def list_storage_units(self, **kwargs):
        """
        List of storage units path that meet the search query.

        :param * * kwargs: search parameters and dimension ranges.
            E.g.::
                product='NBAR', platform='LANDSAT_5', latitude=(-35.5, -34.5)

        :return: list of local paths to the storage units
        """
        descriptor_request = kwargs
        query = convert_descriptor_query_to_search_query(descriptor_request)
        sus = self.index.storage.search(**query)
        output_set = set()
        for su in sus:
            output_set.add(str(su.local_path))
        return list(output_set)

    def list_products(self):
        """
        Lists a dictionary for each stored product

        *Note:* This is exposing an internal structure and subject to change.

        :return: List of dicts describing each product
        """
        return [t.document for t in self.index.storage.types.get_all()]

    def list_fields(self):
        """List of the search fields

        :return: list of field names, e.g.
            ::
                ['product', 'platform']

        """
        return self.index.datasets.get_fields().keys()

    def list_field_values(self, field):
        """
        List the values found for a field

        :param field: Name of the field, as returned by the :meth:`.list_fields` method.
        :type field: str
        :return: List of values for the field in the database, eg
            ::
                ['LANDSAT_5', 'LANDSAT_7']

        """
        return list(set(field_values[field] for field_values in self.index.datasets.search_summaries()
                        if field in field_values))

    def list_all_field_values(self):
        """
        Lists all the search fields with their known values in the database

        :return: Each search field with the list of known values
            ::
                {
                    'platform': ['LANDSAT_5', 'LANDSAT_7'],
                    'product': ['NBAR', 'PQ', 'FC']
                }
        :rtype: dict
        """
        summary = list(self.index.datasets.search_summaries())
        fields = self.index.datasets.get_fields()
        return dict((field, list(set(field_values[field] for field_values in summary))) for field in fields)

    def __repr__(self):
        return "API<index={!r}>".format(self.index)


def _stack_vars(data_dict, var_dim_name, stack_name=None):
    labels = sorted(data_dict.keys())
    stack = xarray.concat(
        [data_dict[var_name] for var_name in labels],
        dim=xarray.DataArray(labels, name=var_dim_name, dims=var_dim_name),
        coords='minimal')
    if stack_name:
        stack.name = stack_name
    return stack


def _get_dimension_properties(storage_units, dimensions, dimension_ranges):
    dim_props = {
        'sus_size': {},
        'coord_labels': {},
        'dim_vals': {},
        'coordinate_reference_systems': {},
        'dimension_ranges': dimension_ranges,
    }
    sample = list(storage_units)[0]
    # Get the start value of the storage unit so we can sort them
    # Some dims are stored upside down (eg Latitude), so sort the tiles consistent with the bounding box order
    dim_props['reverse'] = dict((dim, sample.coordinates[dim].begin > sample.coordinates[dim].end)
                                for dim in dimensions if dim in sample.coordinates)
    for dim in dimensions:
        dim_props['dim_vals'][dim] = sorted(set(su.coordinates[dim].begin
                                                for su in storage_units
                                                if dim in su.coordinates),
                                            reverse=dim_props['reverse'][dim])
        dim_props['coordinate_reference_systems'][dim] = sample.get_crs()[dim]

    coord_lists = {}
    for su in storage_units:
        for dim in dimensions:
            dim_val_len = len(dim_props['dim_vals'][dim])
            ordinal = dim_props['dim_vals'][dim].index(su.coordinates[dim].begin)
            dim_props['sus_size'].setdefault(dim, [None] * dim_val_len)[ordinal] = su.coordinates[dim].length
            coord_list = coord_lists.setdefault(dim, [None] * dim_val_len)
            # We only need the coords once, so don't open up every file if we don't need to - su.get_coord()
            # TODO: if we tried against the diagonal first, we might get all coords in max(shape) rather than sum(shape)
            if coord_list[ordinal] is None:
                coord_list[ordinal], _ = su.get_coord(dim)
    for dim in dimensions:
        dim_props['coord_labels'][dim] = list(itertools.chain(*coord_lists[dim]))
    _fix_custom_dimensions(dimensions, dim_props)
    return dim_props


def _get_storage_units(descriptor_request=None, index=None, is_diskless=False):
    '''
    Given a descriptor query, get the storage units covered
    :param descriptor_request dict of requests
    :param is_diskless: (default False) If True, use a light-weight class that only reads the files for
                        data not stored in the db, such as irregular variables
    :return: StorageUnitCollection
    '''
    index = index or index_connect()
    query = convert_descriptor_query_to_search_query(descriptor_request, index)
    _LOG.debug("Database storage search %s", query)
    sus = index.storage.search(**query)
    storage_units_by_type = defaultdict(StorageUnitCollection)
    for su in sus:
        unit = make_storage_unit(su, is_diskless=is_diskless)
        storage_units_by_type[su.storage_type.name].append(unit)
    return storage_units_by_type


def _create_data_response(xarrays, dimensions):
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
        'coordinate_reference_systems': [dict(v for v in sample_xarray[dim].attrs.items()) for dim in dimensions]

    }
    return response


def _get_array(storage_units, var_name, dimensions, dim_props, fake_array=False, set_nan=False):
    """
    Create an xarray.DataArray
    :return xarray.DataArray
    """
    dask_array = get_dask_array(storage_units, var_name, dimensions, dim_props, fake_array)
    coords = [(dim, dim_props['coord_labels'][dim]) for dim in dimensions]
    xarray_data_array = xarray.DataArray(dask_array, coords=coords)
    for dim in dimensions:
        coord = xarray_data_array.coords[dim]
        if isinstance(dim_props['coordinate_reference_systems'][dim], dict):
            for prop_name, prop_value in dim_props['coordinate_reference_systems'][dim].items():
                coord.attrs[prop_name] = prop_value
        else:
            coord.attrs['coordinate_reference_systems'] = dim_props['coordinate_reference_systems'][dim]
    if set_nan:
        nodata_val = storage_units[0].variables[var_name].nodata
        xarray_data_array = xarray_data_array.where(xarray_data_array != nodata_val)
    return xarray_data_array


def _fix_custom_dimensions(dimensions, dim_props):
    dimension_ranges = dim_props['dimension_ranges']
    coord_labels = dim_props['coord_labels']
    # TODO: Move handling of timestamps down to the storage level
    if 'time' in dimensions:
        coord_labels['time'] = [datetime.datetime.fromtimestamp(c, tz=tz.tzutc()) for c in coord_labels['time']]
        if 'time' in dimension_ranges and 'range' in dimension_ranges['time']:
            dimension_ranges['time']['range'] = tuple(to_datetime(t)
                                                      for t in dimension_ranges['time']['range'])
    return dimension_ranges, coord_labels


def _get_data_array_dict(storage_units_by_variable, dimensions, dimension_ranges, fake_array=False, set_nan=False):
    sus_with_dims = set(itertools.chain(*storage_units_by_variable.values()))
    dim_props = _get_dimension_properties(sus_with_dims, dimensions, dimension_ranges)
    selectors = dimension_ranges_to_selector(dim_props['dimension_ranges'], dim_props['reverse'])
    iselectors = dimension_ranges_to_iselector(dim_props['dimension_ranges'])

    xarrays = {}
    for var_name, storage_units in storage_units_by_variable.items():
        xarray_data_array = _get_array(storage_units, var_name, dimensions, dim_props, fake_array)
        for key, value in selectors.items():
            if key in xarray_data_array.dims:
                if isinstance(value, slice):
                    xarray_data_array = xarray_data_array.sel(**{key: value})
                else:
                    xarray_data_array = xarray_data_array.sel(method='nearest', **{key: value})
        iselectors = dict((k, v) for k, v in iselectors.items() if k in dimensions)
        if iselectors:
            xarray_data_array = xarray_data_array.isel(**iselectors)
        if set_nan:
            nodata = storage_units[0].variables[var_name].nodata
            xarray_data_array = xarray_data_array.where(xarray_data_array != nodata)
        xarrays[var_name] = xarray_data_array
    return xarrays


def _make_xarray_dataset(data_dicts, storage_unit_type):
    combined = {
        'crs': xarray.Variable(dims=(), data=0, attrs={
            'spatial_ref': storage_unit_type.crs
        })
    }
    for data_arrays, _ in data_dicts:
        for variable_name, data_array in data_arrays.items():
            data_array.attrs.update(storage_unit_type.variable_attributes[variable_name])
        combined.update(data_arrays)
    return xarray.Dataset(combined, attrs=storage_unit_type.global_attributes)


def _get_data_from_storage_units(storage_units, variables=None, dimension_ranges=None, fake_array=False, set_nan=False):
    """
    Converts a set of storage units into a dictionary of xrarray.DataArrays
    :param storage_units: list(datacube.storage.access.core.StorageUnitBase)
    :param variables: (optional) list of variables names to retrieve
    :param dimension_ranges: dict of dimensions name -> {'range': (begin, end), 'array_range': (begin, end)}
            Should be converted to storage CRS
    :param fake_array: (default=False) Set to true if only the array shape and properties are required,
            not the actual array.
    :param set_nan: sets the data to numpy.NaN if it matches the storage_unit nodata_value (default=False)
    :return: list of dict(variable name -> xarray.DataArray)
    """
    if not dimension_ranges:
        dimension_ranges = {}

    storage_units = _stratify_irregular_dimension(storage_units, 'time')

    variables_by_dimensions = defaultdict(lambda: defaultdict(list))
    for storage_unit in storage_units:
        for var_name, v in storage_unit.variables.items():
            if variables is None or var_name in variables:
                dims = tuple(v.dimensions)
                variables_by_dimensions[dims][var_name].append(storage_unit)

    if not len(variables_by_dimensions):
        return {}

    dimension_group = []
    for dimensions, sus_by_variable in variables_by_dimensions.items():
        dimension_group.append((_get_data_array_dict(sus_by_variable, dimensions, dimension_ranges,
                                                     fake_array, set_nan),
                                dimensions))
    return dimension_group


def _to_single_value(data_array):
    if data_array.size != 1:
        return data_array
    if isinstance(data_array.values, numpy.ndarray):
        return data_array.item()
    return data_array.values


def _get_result_stats(storage_units, dimensions, dimension_ranges):
    """

    :param storage_units:
    :param dimensions:
    :param dimension_ranges:
    :return:
    """
    storage_data = _get_data_from_storage_units(storage_units,
                                                dimension_ranges=dimension_ranges,
                                                fake_array=True)

    sample = list(storage_data[0][0].values())[0]
    result = _describe_data_array(sample)
    return result


def _describe_data_array(xarray_sample):
    """
    Creates a "descriptor" dictionary for a sample xarray DataArray
    :param storage_units:
    :return:
    """
    example = xarray_sample
    dimensions = xarray_sample.dims
    result = {
        'result_shape': tuple(example[dim].size for dim in dimensions),
        'result_min': tuple(_to_single_value(example[dim].min()) if example[dim].size > 0 else numpy.NaN
                            for dim in dimensions),
        'result_max': tuple(_to_single_value(example[dim].max()) if example[dim].size > 0 else numpy.NaN
                            for dim in dimensions),
    }
    irregular_dim_names = ['time', 't']  # TODO: Use irregular flag from database instead
    result['irregular_indices'] = dict((dim, example[dim].values)
                                       for dim in dimensions if dim in irregular_dim_names)
    return result


def main():
    agdc_api = API()
    desc = agdc_api.get_descriptor()
    print(desc)


if __name__ == '__main__':
    main()
