# coding=utf-8
"""
Functions for creating xarray objects from storage units.
"""

from __future__ import absolute_import, division, print_function

import datetime
import itertools
from collections import defaultdict, OrderedDict
import logging

import numpy
from dateutil import tz
import xarray

from datacube.index import index_connect

from ._conversion import convert_descriptor_query_to_search_query
from ._conversion import dimension_ranges_to_selector, dimension_ranges_to_iselector, to_datetime
from ._dask import get_dask_array
from ._storage import StorageUnitCollection
from ._storage import make_storage_unit
from ._stratify import stratify_irregular_dimension


_LOG = logging.getLogger(__name__)


def get_array(storage_units, var_name, dimensions, dim_props, fake_array=False, set_nan=False):
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


def stack_vars(data_dict, var_dim_name, stack_name=None):
    labels = sorted(data_dict.keys())
    stack = xarray.concat(
        [data_dict[var_name] for var_name in labels],
        dim=xarray.DataArray(labels, name=var_dim_name, dims=var_dim_name),
        coords='minimal')
    if stack_name:
        stack.name = stack_name
    return stack


def su_in_cell(su, x_index, y_index, xy_index=None):
    if not hasattr(su, 'tile_index'):
        return False
    if xy_index is None and x_index is None and y_index is None:
        return True
    if xy_index is not None and su.tile_index in xy_index:
        return True
    if x_index is None and y_index is None:
        return False
    return (x_index is None or su.tile_index[0] in x_index) and (y_index is None or su.tile_index[1] in y_index)


def get_dimension_properties(storage_units, dimensions, dimension_ranges):
    dim_props = {
        'sus_size': {},
        'coord_labels': {},
        'dim_vals': {},
        'coordinate_reference_systems': {},
        'dimension_ranges': dimension_ranges,
    }
    sample = list(storage_units)[0]
    # Get the start value of the storage unit so we can sort them
    # Some dims are stored upside down (e.g. Latitude), so sort the tiles consistent with the bounding box order
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
    fix_custom_dimensions(dimensions, dim_props)
    return dim_props


def get_storage_units(descriptor_request=None, index=None, is_diskless=False):
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


def create_data_response(xarrays, dimensions):
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
                           float(sample_xarray.coords[dim].size - 1)) if sample_xarray.coords[dim].size > 1 else 0
                          for dim in dimensions],
        'size': sample_xarray.shape,
        'coordinate_reference_systems': [dict(v for v in sample_xarray[dim].attrs.items()) for dim in dimensions]

    }
    return response


def fix_custom_dimensions(dimensions, dim_props):
    dimension_ranges = dim_props['dimension_ranges']
    coord_labels = dim_props['coord_labels']
    # TODO: Move handling of timestamps down to the storage level
    if 'time' in dimensions:
        coord_labels['time'] = [datetime.datetime.fromtimestamp(c, tz=tz.tzutc()) for c in coord_labels['time']]
        if 'time' in dimension_ranges and 'range' in dimension_ranges['time']:
            if hasattr(dimension_ranges['time']['range'], '__iter__'):
                dimension_ranges['time']['range'] = tuple(to_datetime(t)
                                                          for t in dimension_ranges['time']['range'])
            else:
                dimension_ranges['time']['range'] = numpy.datetime64(to_datetime(dimension_ranges['time']['range']))
    for dim in dimension_ranges.keys():
        if dim not in dimensions:
            x_dims = ['x', 'lon', 'longitude']
            if dim in x_dims:
                x_match = list(set(x_dims) & set(dimensions))
                if x_match:
                    dimension_ranges[x_match[0]] = dimension_ranges.pop(dim)
            y_dims = ['y', 'lat', 'latitude']
            if dim in y_dims:
                y_match = list(set(y_dims) & set(dimensions))
                if y_match:
                    dimension_ranges[y_match[0]] = dimension_ranges.pop(dim)

    return dimension_ranges, coord_labels


def get_data_array_dict(storage_units_by_variable, dimensions, dimension_ranges, fake_array=False, set_nan=False):
    sus_with_dims = set(itertools.chain(*storage_units_by_variable.values()))
    dim_props = get_dimension_properties(sus_with_dims, dimensions, dimension_ranges)
    selectors = dimension_ranges_to_selector(dim_props['dimension_ranges'], dim_props['reverse'])
    iselectors = dimension_ranges_to_iselector(dim_props['dimension_ranges'])

    xarrays = {}
    for var_name, storage_units in storage_units_by_variable.items():
        xarray_data_array = get_array(storage_units, var_name, dimensions, dim_props, fake_array)
        xarray_data_array = apply_selectors(xarray_data_array, selectors)
        iselectors = dict((k, v) for k, v in iselectors.items() if k in dimensions)
        if iselectors:
            xarray_data_array = xarray_data_array.isel(**iselectors)

        nodata_value = storage_units[0].variables[var_name].nodata
        if nodata_value is not None:
            if set_nan:
                xarray_data_array = xarray_data_array.where(xarray_data_array != nodata_value)
            else:
                xarray_data_array.attrs['_FillValue'] = nodata_value
        xarrays[var_name] = xarray_data_array
    return xarrays


def apply_selectors(xarray_data_array, selectors):
    for key, value in selectors.items():
        if key in xarray_data_array.dims:
            if isinstance(value, slice):
                if xarray_data_array[key].dtype.kind == 'f':
                    tolerance = float(xarray_data_array[key][1] - xarray_data_array[key][0]) / 4.
                    xarray_data_array = xarray_data_array.sel(
                        **{key: slice(value.start - tolerance, value.stop + tolerance, value.step)})
                else:
                    xarray_data_array = xarray_data_array.sel(**{key: value})
            else:
                xarray_data_array = xarray_data_array.sel(method='nearest', **{key: value})
    return xarray_data_array


def make_xarray_dataset(data_dicts, storage_unit_type):
    combined = {
        'crs': xarray.Variable(dims=(), data=0, attrs={
            'spatial_ref': storage_unit_type.crs
        })
    }
    for data_arrays, _ in data_dicts:
        for variable_name, data_array in data_arrays.items():
            data_array.attrs.update(storage_unit_type.variable_attributes[variable_name])
        combined.update(data_arrays)
    attrs = storage_unit_type.global_attributes
    attrs['storage_type'] = storage_unit_type.name
    return xarray.Dataset(combined, attrs=attrs)


def get_metadata_from_storage_units(storage_units, dimension_ranges):
    dimensions = ('time',)
    #TODO: Handle metadata across non-harcoded and multiple dimensions
    dim_props = get_dimension_properties(storage_units, dimensions, dimension_ranges)
    selectors = dimension_ranges_to_selector(dim_props['dimension_ranges'], dim_props['reverse'])
    iselectors = dimension_ranges_to_iselector(dim_props['dimension_ranges'])

    metadata = defaultdict(list)
    for storage_unit in storage_units:
        if 'extra_metadata' in storage_unit.variables:
            su_metadata = storage_unit.get('extra_metadata')
            for arr in su_metadata:
                metadata[arr['time'].item()].append(arr.values)
    multi_yaml = {k: '\n---\n'.join(str(s) for s in v) for k, v in metadata.items()}
    multi_yaml = OrderedDict((to_datetime(k), multi_yaml[k]) for k in sorted(multi_yaml))
    data_array = xarray.DataArray(multi_yaml.values(),
                                  coords={'time': list(multi_yaml.keys())})

    for key, value in selectors.items():
        if key in data_array.dims:
            if isinstance(value, slice):
                data_array = data_array.sel(**{key: value})
            else:
                data_array = data_array.sel(method='nearest', **{key: value})
    iselectors = dict((k, v) for k, v in iselectors.items() if k in dimensions)
    if iselectors:
        data_array = data_array.isel(**iselectors)
    return ({'extra_metadata': data_array}, dimensions)


def get_data_from_storage_units(storage_units, variables=None, dimension_ranges=None, fake_array=False, set_nan=False):
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

    storage_units = stratify_irregular_dimension(storage_units, 'time')

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
        dimension_group.append((get_data_array_dict(sus_by_variable, dimensions, dimension_ranges,
                                                    fake_array, set_nan),
                                dimensions))
    return dimension_group


def to_single_value(data_array):
    if data_array.size != 1:
        return data_array
    if isinstance(data_array.values, numpy.ndarray):
        return data_array.item()
    return data_array.values


def get_result_stats(storage_units, dimensions, dimension_ranges):
    """

    :param storage_units:
    :param dimensions:
    :param dimension_ranges:
    :return:
    """
    storage_data = get_data_from_storage_units(storage_units, dimension_ranges=dimension_ranges, fake_array=True)
    sample = list(storage_data[0][0].values())[0]
    result = describe_data_array(sample)
    return result


def describe_data_array(xarray_sample):
    """
    Creates a "descriptor" dictionary for a sample xarray DataArray
    :param xarray_sample:
    :return:
    """
    example = xarray_sample
    dimensions = xarray_sample.dims
    result = {
        'result_shape': tuple(example[dim].size for dim in dimensions),
        'result_min': tuple(to_single_value(example[dim].min()) if example[dim].size > 0 else numpy.NaN
                            for dim in dimensions),
        'result_max': tuple(to_single_value(example[dim].max()) if example[dim].size > 0 else numpy.NaN
                            for dim in dimensions),
    }
    irregular_dim_names = ['time', 't']  # TODO: Use irregular flag from database instead
    result['irregular_indices'] = dict((dim, example[dim].values)
                                       for dim in dimensions if dim in irregular_dim_names)
    return result
