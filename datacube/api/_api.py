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

from ._conversion import convert_descriptor_query_to_search_query, convert_descriptor_dims_to_selector_dims
from ._conversion import dimension_ranges_to_selector, dimension_ranges_to_iselector, to_datetime
from ._dask import get_dask_array, get_fake_dask_array
from ._storage import StorageUnitCollection, IrregularStorageUnitSlice
from ._storage import make_storage_unit, make_storage_unit_collection_from_descriptor

_LOG = logging.getLogger(__name__)


class API(object):
    def __init__(self, index=None):
        """
        :param index: datacube.index._api.Index
        :return:
        """
        self.index = index or index_connect()

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
        Function to return composite in-memory arrays
        :param descriptor:
        data_request = \
        {
        'platform': 'LANDSAT_8',
        <search_field>: <search value>,
        'product': '',
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

        variables = descriptor.get('variables', None)
        storage_units_by_type = defaultdict(StorageUnitCollection)
        if storage_units:
            stype = 'Requested Storage'
            storage_units_by_type[stype] = make_storage_unit_collection_from_descriptor(storage_units)
        else:
            storage_units_by_type = self._get_storage_units(descriptor, self.index)

        if len(storage_units_by_type) > 1:
            # TODO: Work out if they share the same grid (projection & resolution)
            # TODO: Reproject all into the requested grid
            raise RuntimeError('Data must come from a single storage')

        data_response = {}
        for stype, storage_unit_collection in storage_units_by_type.items():
            # TODO: check var names are unique accross products
            storage_crs = storage_unit_collection.get_spatial_crs()
            dimension_descriptor = descriptor.get('dimensions', {})
            dimension_ranges = convert_descriptor_dims_to_selector_dims(dimension_descriptor, storage_crs)
            storage_units = _stratify_irregular_dimension(storage_unit_collection.iteritems(), 'time')
            storage_data = _get_data_from_storage_units(storage_units, variables, dimension_ranges)

            data_response.update(storage_data)
        return data_response

    def get_fields(self):
        return self.index.datasets.get_fields().keys()


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
    sus = index.storage.search(**query)
    storage_units_by_type = defaultdict(StorageUnitCollection)
    for su in sus:
        unit = make_storage_unit(su, is_diskless=is_diskless)
        storage_units_by_type[su.storage_type.name].append(unit)
    return storage_units_by_type


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


def _get_array(storage_units, var_name, dimensions, dim_props, fake_array=False):
    """
    Create an xarray.DataArray
    :return xarray.DataArray
    """
    if not fake_array:
        dask_array = get_dask_array(storage_units, var_name, dimensions, dim_props)
    else:
        dask_array = get_fake_dask_array(storage_units, var_name, dimensions, dim_props)
    coords = [(dim, dim_props['coord_labels'][dim]) for dim in dimensions]
    xarray_data_array = xarray.DataArray(dask_array, coords=coords)
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


def _get_data_by_variable(storage_units_by_variable, dimensions, dimension_ranges, fake_array=False):
    sus_with_dims = set(itertools.chain(*storage_units_by_variable.values()))
    dim_props = _get_dimension_properties(sus_with_dims, dimensions)
    dim_props['dimension_ranges'] = dimension_ranges

    _fix_custom_dimensions(dimensions, dim_props)
    selectors = dimension_ranges_to_selector(dim_props['dimension_ranges'], dim_props['reverse'])
    iselectors = dimension_ranges_to_iselector(dim_props['dimension_ranges'])

    xarrays = {}
    for var_name, storage_units in storage_units_by_variable.items():
        xarray_data_array = _get_array(storage_units, var_name, dimensions, dim_props, fake_array)
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


def _get_data_from_storage_units(storage_units, variables=None, dimension_ranges=None, fake_array=False):
    if not dimension_ranges:
        dimension_ranges = {}

    variables_by_dimensions = defaultdict(lambda: defaultdict(list))
    for storage_unit in storage_units:
        for var_name, v in storage_unit.variables.items():
            if variables is None or var_name in variables:
                dims = tuple(v.dimensions)
                variables_by_dimensions[dims][var_name].append(storage_unit)

    if not len(variables_by_dimensions):
        return {}

    dimension_group = {}
    for dimensions, sus_by_variable in variables_by_dimensions.items():
        dimension_group[dimensions] = _get_data_by_variable(sus_by_variable, dimensions, dimension_ranges, fake_array)
    if len(dimension_group) == 1:
        return list(dimension_group.values())[0]
    return dimension_group


def _to_single_value(data_array):
    if data_array.size != 1:
        return data_array
    if isinstance(data_array.values, numpy.ndarray):
        return data_array.item()
    return data_array.values


def _get_result_stats(storage_units, dimensions, dimension_ranges):
    strata_storage_units = _stratify_irregular_dimension(storage_units, 'time')
    storage_data = _get_data_from_storage_units(strata_storage_units,
                                                dimension_ranges=dimension_ranges,
                                                fake_array=True)
    example = storage_data['arrays'][list(storage_data['arrays'].keys())[0]]
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
