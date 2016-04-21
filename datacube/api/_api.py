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
from collections import defaultdict
import warnings

import xarray

from datacube.index import index_connect
from datacube.compat import string_types

from ._conversion import convert_descriptor_query_to_search_query, convert_descriptor_dims_to_selector_dims
from ._conversion import convert_request_args_to_descriptor_query
from ._storage import StorageUnitCollection
from ._storage import make_storage_unit, make_storage_unit_collection_from_descriptor, get_tiles_for_su
from ._xarray import get_storage_units, get_result_stats, get_data_from_storage_units, create_data_response, stack_vars
from ._xarray import su_in_cell, get_metadata_from_storage_units, make_xarray_dataset

_LOG = logging.getLogger(__name__)


class API(object):
    """
    The API object is the primary way to query the datacube and extract data, making use of both the database and
    underlying data files.  This API can be used directly, or through higher-level layers such as the Analytics Engine.
    """

    def __init__(self, index=None, application_name=None):
        """
        Creates the interface for the query and storage access.
        If no index is given, the default configuration is used for database connection, etc.

        :param index: The database index to use.
        :param application_name: A short, alphanumeric name to identify this application.
        :type index: from :py:class:`datacube.index.index_connect` or None
        """
        self.index = index or index_connect(application_name=application_name)

    def get_descriptor(self, descriptor_request=None, include_storage_units=True):
        """
        Gets the metadata for a ``AnalyticsEngine`` query.

        All fields are optional.

        Search for any of the fields returned by :meth:`list_fields()`.

        **Dimensions**

        Dimensions can specify a range by label, and optionally a CRS to interpret the label.

        The default CRS interpretation for geospatial dimensions (longitude/latitude or x/y) is WGS84/EPSG:4326,
        even if the resulting dimension is in another projection.

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
        storage_units_by_type = get_storage_units(descriptor_request, self.index, is_diskless=True)
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
                result.update(get_result_stats(grouped_storage_units.get_storage_units(),
                                               dimensions, dimension_ranges))
        return descriptor

    def get_data(self, descriptor=None, storage_units=None):
        """
        Gets the data for a ``ExecutionEngine`` query.
        Function to return composite in-memory arrays.

        :param descriptor: A dictionary containing the query parameters. All fields are optional.

            **Search fields**

            Search for any of the fields returned by :meth:`list_fields()`, using a value from
            :meth:`list_field_values()`.

            **Storage type field**

            The ``storage_type`` can be any of the keys returned by :meth:`get_descriptor()` or
            :meth:`list_storage_type_names()`.

            **Variables field**

            The ``variables`` field is a list of variable names matching those listed by :meth:`get_descriptor()` or
            :meth:`list_variables()`.
            If not specified, all variables are returned.

            **Dimensions field**

            The ``dimensions`` field can specify a range by label and/or index, and optionally a CRS to interpret
            the label range request.

            Times can be specified as :class:`datetime` objects, tuples of (year, month, day) or
            (year, month, day, hour, minute, second), or by seconds since the Unix epoch.
            Strings may also be used, with ISO format preferred.

            The default CRS interpretation for geospatial dimensions (longitude/latitude or x/y) is WGS84/EPSG:4326,
            even if the resulting dimension is in another projection.

            The ``array_range`` field can be used to subset the request.
            ::
                descriptor = {
                    'platform': 'LANDSAT_8',
                    'product': 'NBAR',
                    # <search_field>: <search value>,

                    'storage_type': 'ls8_nbar',

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

        :param storage_units: Limit the query to the given storage unit descriptors,
            as given by the :py:meth:`.get_descriptor()` method.

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
            storage_units_by_type = get_storage_units(descriptor, self.index)

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
            dimension_groups = get_data_from_storage_units(storage_unit_collection.get_storage_units(),
                                                           variables, dimension_ranges)
            #data_response[stype] = _create_data_response(xarrays)
            # TODO: Return multiple storages, or wait until we suppport reprojection
            # Right now, just return the result with the largest number of dimensions
            dimension_groups.sort(key=lambda x: len(x[1]), reverse=True)
            xarrays, dimensions = dimension_groups[0]
            return create_data_response(xarrays, dimensions)
        return data_response

    def get_data_array(self, variables=None, var_dim_name=u'variable', set_nan=True, **kwargs):
        """
        Gets data as a stacked ``xarray.DataArray``.  The data will be in a single array, with each variable
        available for the  variables.
        This stacks the data similar to ``numpy.dstack``.  Use this function instead of :meth:`get_dataset()` if you
        only need stacked data.  All variables must be of the same dimensions, and this function doesn't return
        additional information such as ``crs`` and ``extra_metadata``.

        See http://xarray.pydata.org/en/stable/api.html#dataarray for usage of the ``DataArray`` object.

        :param variables: Variables to be included. Use ``None`` to include all available variables
        :type variables: list or None
        :param var_dim_name: dimension name to use for the stack of variables.
            The default is `"variable"`.

        :type var_dim_name: str
        :param set_nan: Set "no data" values to ``numpy.NaN``.

            .. warning::
                This will cause the data to be converted to float dtype.
        :type set_nan: bool

        :param * * kwargs: search parameters, dimension ranges and storage_type.
            ::
                api.get_data_array(product='NBAR', platform='LANDSAT_5', latitude=(-35.5, -34.5))

                api.get_data_array(storage_type='ls5_nbar', time=((1990, 1, 1), (1991, 1, 1)))

            The default CRS interpretation for geospatial dimensions is WGS84/EPSG:4326,
            even if the resulting dimension is in another projection.

            The dimensions ``longitude``/``latitude`` and ``x``/``y`` can be used interchangeably.

        :return: Data with all variables stacked along a dimension.
        :rtype: xarray.DataArray
        """
        descriptor_request = convert_request_args_to_descriptor_query(kwargs, self.index)
        descriptor_dimensions = descriptor_request.get('dimensions', {})
        variables = [variables] if isinstance(variables, string_types) else variables

        query = convert_descriptor_query_to_search_query(descriptor_request, self.index)
        storage_units_by_type = defaultdict(StorageUnitCollection)

        for su in self.index.storage.search_eager(**query):
            storage_units_by_type[su.storage_type.name].append(make_storage_unit(su))

        return self._storage_units_to_data_array(storage_units_by_type, descriptor_request.get('dimensions', {}),
                                                 variables, set_nan, var_dim_name)

    def get_data_array_by_cell(self, xy_index=None, x_index=None, y_index=None, variables=None,
                               var_dim_name=u'variable', set_nan=True, **kwargs):
        """
        Gets data as a stacked ``xarray.DataArray``.  The data will be in a single array, with each variable
        available for the  variables.
        This stacks the data similar to ``numpy.dstack``.  Use this function instead of :meth:`get_dataset()` if you
        only need stacked data.  All variables must be of the same dimensions, and this function doesn't return
        additional information such as ``crs`` and ``extra_metadata``.

        The cell represents a tiled footprint of the underlying storage footprint,
        and is typically only used in large-scale processing of data.
        Cell indexes can be found using :meth:`list_cells()`.

        See http://xarray.pydata.org/en/stable/api.html#dataarray for usage of the ``DataArray`` object.

        :param xy_index: (x, y) tile index (or list of indices) to return.
            ::
                api.get_data_array_by_cell((11, -20), product='NBAR')

                api.get_data_array_by_cell([(11, -20), (12, -20)], product='NBAR')

        :type xy_index: list or tuple
        :param x_index: x tile index (or list of indicies) to return.
            ::
                api.get_data_array_by_cell(x_index=11, y_index=-20, product='NBAR')

                api.get_data_array_by_cell(x_index=[11, 12], y_index=[-20, -21], product='NBAR')

        :type x_index: list or int
        :param y_index: y tile index (or list of indicies) to return.
        :type y_index: list or int
        :param variables: Variables to be included. Use ``None`` to include all available variables
        :type variables: list or None
        :param var_dim_name: dimension name to use for the stack of variables.
            The default is `"variable"`.

        :type var_dim_name: str
        :param set_nan: Set "no data" values to ``numpy.NaN``.

            .. warning::
                This will cause the data to be converted to float dtype.

        :type set_nan: bool

        :param * * kwargs: search parameters, dimension ranges and storage_type.
            ::
                api.get_data_array(product='NBAR', platform='LANDSAT_5', latitude=(-35.5, -34.5))

                api.get_data_array(storage_type='ls5_nbar', time=((1990, 1, 1), (1991, 1, 1)))

        :return: Data with all variables stacked along a dimension.
        :rtype: xarray.DataArray
        """
        x_index = x_index if x_index is None or hasattr(x_index, '__contains__') else [x_index]
        y_index = y_index if y_index is None or hasattr(y_index, '__contains__') else [y_index]
        xy_index = xy_index if not isinstance(xy_index, tuple) else [xy_index]
        variables = [variables] if isinstance(variables, string_types) else variables

        descriptor_request = convert_request_args_to_descriptor_query(kwargs, self.index)
        query = convert_descriptor_query_to_search_query(descriptor_request, self.index)

        storage_units_by_type = defaultdict(StorageUnitCollection)
        for su in self.index.storage.search_eager(**query):
            if su_in_cell(su, x_index, y_index, xy_index):
                storage_units_by_type[su.storage_type.name].append(make_storage_unit(su))

        return self._storage_units_to_data_array(storage_units_by_type, descriptor_request.get('dimensions', {}),
                                                 variables, set_nan, var_dim_name)

    def _storage_units_to_data_array(self, storage_units_by_type, descriptor_dimensions, variables,
                                     set_nan, var_dim_name):
        for stype, storage_units in storage_units_by_type.items():
            dimension_ranges = convert_descriptor_dims_to_selector_dims(descriptor_dimensions,
                                                                        storage_units.get_spatial_crs())
            data_dicts = get_data_from_storage_units(storage_units.iteritems(), variables, dimension_ranges,
                                                     set_nan=set_nan)
            if len(data_dicts) and len(data_dicts[0]):
                data_dict = data_dicts[0][0]
                return stack_vars(data_dict, var_dim_name, stack_name=stype)
        return None

    def get_dataset(self, variables=None, set_nan=False, include_lineage=False, **kwargs):
        """
        Gets an ``xarray.Dataset`` for the requested data.

        See http://xarray.pydata.org/en/stable/api.html#dataset for usage of the ``Dataset`` object.

        :param variables: variable or list of variables to be included.
                Use ``None`` to include all available variables (default)
        :type variables: list(str) or str, optional
        :param set_nan: If any "no data" values should be set to ``numpy.NaN``

            .. warning::
                This will cause the data to be converted to float dtype.
        :type set_nan: bool, optional
        :param include_lineage: Include an 'extra_metadata' variable containing detailed lineage information.
            Not included by default.

            .. note::
                This can cause the query to be slow for large datasets, as it is not lazy-loaded.
        :type include_lineage: bool, optional
        :param kwargs: Search parameters and dimension ranges.

            See :meth:`get_data()` for a explaination of the possible parameters.
            E.g.::
                product='NBAR', platform='LANDSAT_5', latitude=(-35.5, -34.5)

            The default CRS interpretation for geospatial dimensions is WGS84/EPSG:4326,
            even if the resulting dimension is in another projection.

            The dimensions ``longitude``/``latitude`` and ``x``/``y`` can be used interchangeably.

        :return: Data as variables with shared coordinate dimensions.
        :rtype: xarray.Dataset
        """
        descriptor_request = convert_request_args_to_descriptor_query(kwargs, self.index)
        descriptor_dimensions = descriptor_request.get('dimensions', {})
        variables = [variables] if isinstance(variables, string_types) else variables

        query = convert_descriptor_query_to_search_query(descriptor_request, self.index)

        storage_units_by_type = defaultdict(lambda: list([None, StorageUnitCollection()]))
        for su in self.index.storage.search(**query):
            storage_units_by_type[su.storage_type.id][0] = su.storage_type
            storage_units_by_type[su.storage_type.id][1].append(
                make_storage_unit(su, include_lineage=include_lineage))

        #TODO: return multiple storage types if compatible
        # or warp / reproject / resample if required?
        # including realigning timestamps
        # and dealing with each storage unit having an extra_metadata field...
        if len(storage_units_by_type) > 1:
            warnings.warn('Multiple storage types found. Only 1 will be returned. Make a '
                          'more specific request to access the other data.')

        for storage_unit_type, storage_units in storage_units_by_type.values():
            # storage_unit_type = storage_unit_types[stype]
            dimension_ranges = convert_descriptor_dims_to_selector_dims(descriptor_dimensions,
                                                                        storage_units.get_spatial_crs())
            data_dicts = get_data_from_storage_units(storage_units.iteritems(), variables,
                                                     dimension_ranges, set_nan=set_nan)
            if include_lineage:
                data_dicts.append(get_metadata_from_storage_units(storage_units.items(), dimension_ranges))
            return make_xarray_dataset(data_dicts, storage_unit_type)
        return xarray.Dataset()

    def get_dataset_by_cell(self, xy_index=None, x_index=None, y_index=None, variables=None,
                            set_nan=False, include_lineage=False, **kwargs):
        """
        Gets an ``xarray.Dataset`` for a given cell.

        The cell represents a tiled footprint of the underlying storage footprint,
        and is typically only used in large-scale processing of data.
        Cell indexes can be found using :meth:`list_cells()`.

        See http://xarray.pydata.org/en/stable/api.html#dataset for usage of the ``Dataset`` object.

        :param xy_index: (x, y) tile index (or list of indices) to return.
            ::
                api.get_dataset_by_cell((11, -20), product='NBAR')

                api.get_dataset_by_cell([(11, -20), (12, -20)], product='NBAR')

        :type xy_index: list or tuple
        :param x_index: x tile index (or list of indicies) to return.
            ::
                api.get_dataset_by_cell(x_index=11, y_index=-20, product='NBAR')

                api.get_dataset_by_cell(x_index=[11, 12], y_index=[-20, -21], product='NBAR')

        :type x_index: list or int
        :param y_index: y tile index (or list of indicies) to return.
        :type y_index: list or int
        :param variables: variable or list of variables to be included.
                Use ``None`` to include all available variables (default)
        :type variables: list(str) or str, optional
        :param set_nan: If any "no data" values should be set to ``numpy.NaN``

            .. warning::
                This will cause the data to be converted to float dtype.
        :type set_nan: bool, optional
        :param include_lineage: Include an 'extra_metadata' variable containing detailed lineage information.
            *Note:* This can cause the query to be slow for large datasets, as it is not lazy-loaded.
            Not included by default.
        :type include_lineage: bool, optional
        :param kwargs: Search parameters and dimension ranges.

            See :meth:`get_data()` for a explaination of the possible parameters.
            E.g.::
                product='NBAR', platform='LANDSAT_5',
                time=((1990, 6, 1), (1992, 7 ,1)), latitude=(-35.5, -34.5)

            The default CRS interpretation for geospatial dimensions is WGS84/EPSG:4326,
            even if the resulting dimension is in another projection.

            The dimensions ``longitude``/``latitude`` and ``x``/``y`` can be used interchangeably.

            .. note::
                The dimension range must fall in the cells specified by the tile indices.

        :return: Data as variables with shared coordinate dimensions.
        :rtype: xarray.Dataset
        """
        x_index = x_index if x_index is None or hasattr(x_index, '__contains__') else [x_index]
        y_index = y_index if y_index is None or hasattr(y_index, '__contains__') else [y_index]
        xy_index = xy_index if not isinstance(xy_index, tuple) else [xy_index]

        variables = [variables] if isinstance(variables, string_types) else variables

        descriptor_request = convert_request_args_to_descriptor_query(kwargs, self.index)
        query = convert_descriptor_query_to_search_query(descriptor_request, self.index)

        storage_units_by_type = defaultdict(lambda: list([None, StorageUnitCollection()]))
        for su in self.index.storage.search(**query):
            if su_in_cell(su, x_index, y_index, xy_index):
                storage_units_by_type[su.storage_type.id][0] = su.storage_type
                storage_units_by_type[su.storage_type.id][1].append(
                    make_storage_unit(su, include_lineage=include_lineage))

        #TODO: return multiple storage types if compatible
        # or warp / reproject / resample if required?
        # including realigning timestamps
        # and dealing with each storage unit having an extra_metadata field...
        for storage_unit_type, storage_units in storage_units_by_type.values():
            dimension_ranges = convert_descriptor_dims_to_selector_dims(descriptor_request.get('dimensions', {}),
                                                                        storage_units.get_spatial_crs())
            data_dicts = get_data_from_storage_units(storage_units.iteritems(), variables,
                                                     dimension_ranges, set_nan=set_nan)
            if include_lineage:
                data_dicts.append(get_metadata_from_storage_units(storage_units.items(), dimension_ranges))
            return make_xarray_dataset(data_dicts, storage_unit_type)
        return xarray.Dataset()

    def list_storage_units(self, **kwargs):
        """
        List of storage units path that meet the search query.

        :param * * kwargs: search parameters and dimension ranges.
            E.g.::
                product='NBAR', platform='LANDSAT_5', latitude=(-35.5, -34.5)

        :return: List of local paths to the storage units
        """
        descriptor_request = kwargs
        query = convert_descriptor_query_to_search_query(descriptor_request, self.index)
        sus = self.index.storage.search(**query)
        output_set = set()
        for su in sus:
            output_set.add(str(su.local_path))
        return list(output_set)

    def list_storage_type_names(self):
        """
        List the names of the storage types.

        .. warning::
            This is exposing an internal structure and subject to change.

        :return: List of the storage types
        """
        storage_types = self.index.storage.types.get_all()
        return [st.name for st in storage_types]

    def list_products(self):
        """
        Lists a dictionary for each stored product.

        .. warning::
            This is exposing an internal structure and subject to change.

        :return: List of dicts describing each product
        """
        return [t.document for t in self.index.storage.types.get_all()]

    def list_fields(self):
        """
        List of the search fields.

        :return: list of field names, e.g.
            ::
                ['product', 'platform']

        """
        return self.index.datasets.get_fields().keys()

    def list_field_values(self, field):
        """
        List the values found for a field.

        :param field: Name of the field, as returned by the :meth:()`.list_fields` method.
        :type field: str
        :return: List of values for the field in the database, e.g.
            ::
                ['LANDSAT_5', 'LANDSAT_7']

        """
        return list(set(field_values[field] for field_values in self.index.datasets.search_summaries()
                        if field in field_values))

    def list_all_field_values(self):
        """
        Lists all the search fields with their known values in the database.

        :return: Each search field with the list of known values.
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

    def list_cells(self, x_index=None, y_index=None, **kwargs):
        """
        List the tile index pairs for cells.

        Cells are the spatial footprint, with an ``(x, y)`` index that can be configured to match the projection of the
        stored data.

        E.g. ``(148, -35)`` could represent a 1x1 degree tile containing data between
        longitudes 148.0 up to but not including 149.0 and
        latitudes of -35.0 up to but not including -36.0 for in geographically projected data.

        For projected data (such as Australian Albers equal-area projection - ESPG:3577), the tile
        ``(15, -40)`` could represent a 100x100km tile containing data from
        eastings 1,500,000m up to but not including 1,600,000, and
        northings -4,000,000m up to but not including -4,100,000m.

        **Note:** This is typically only used for bulk data processing.

        :param x_index: Limit the response to those cells with an x tile index in this list.
            The default of ``None`` does not filter the list.
        :type x_index: int, list of ints, or None
        :param y_index: Limit the response to those cells with an y tile index in this list.
            The default of ``None`` does not filter the list.
        :type y_index: int, list of ints, or None
        :param kwargs: Filter the cells by search parameters, dimension ranges and storage_type.
        :return: List of tuples of the (x, y) tile indicies.
        """
        x_index = x_index if x_index is None or hasattr(x_index, '__contains__') else [x_index]
        y_index = y_index if y_index is None or hasattr(y_index, '__contains__') else [y_index]

        descriptor_request = convert_request_args_to_descriptor_query(kwargs, self.index)
        query = convert_descriptor_query_to_search_query(descriptor_request, self.index)
        return sorted({su.tile_index for su in self.index.storage.search(**query) if su_in_cell(su, x_index, y_index)})

    def list_tiles(self, xy_index=None, x_index=None, y_index=None, **kwargs):
        """List the tiles for a given cell.

        The cell represents a tiled footprint of the underlying storage footprint,
        and is typically only used in large-scale processing of data.

        A first element of a returned item (the `tile_query` part) can be used as part of a
        :meth:`get_dataset_by_cell()` or :meth:`get_data_array_by_cell()`, using the ``**`` unpack operator::
                tiles = api.list_tiles((11, -20), product='NBAR')
                for (tile_query, tile_attrs) in tiles.items():
                    dataset = api.get_dataset_by_cell(**tile_query)
                    ...

        :param xy_index: (x, y) tile index (or list of indices) to return.
            ::
                api.list_tiles((11, -20), product='NBAR')

                api.list_tiles([(11, -20), (12, -20)], product='NBAR')

        :type xy_index: list or tuple
        :param x_index: x tile index (or list of indicies) to return.
            ::
                api.list_tiles(x_index=11, y_index=-20, product='NBAR')

                api.list_tiles(x_index=[11, 12], y_index=[-20, -21], product='NBAR')

        :type x_index: list or int
        :param y_index: y tile index (or list of indicies) to return.
        :type y_index: list or int
        :param kwargs: Search parameters and dimension ranges.

            See :meth:`get_data()` for a explaination of the possible parameters.
            E.g.::
                product='NBAR', platform='LANDSAT_5',
                time=((1990, 6, 1), (1992, 7 ,1)), latitude=(-35.5, -34.5)

            The default CRS interpretation for geospatial dimensions is WGS84/EPSG:4326,
            even if the resulting dimension is in another projection.

            The dimensions ``longitude``/``latitude`` and ``x``/``y`` can be used interchangeably.

            .. note::
                The dimension range must fall in the cells specified by the tile indices.

        :return: List of tuples containing (``tile_query``, ``tile_attributes``).
            ::
                [
                    ({
                        'xy_index': (-15, -40),
                        'time': numpy.datetime64(),
                        'storage_type': 'ls5_nbar',
                    }, {
                        'path': '...',
                        'description': '...',
                    }),
                    ...
                ]

        """
        x_index = x_index if x_index is None or hasattr(x_index, '__contains__') else [x_index]
        y_index = y_index if y_index is None or hasattr(y_index, '__contains__') else [y_index]
        xy_index = xy_index if not isinstance(xy_index, tuple) else [xy_index]
        descriptor_request = convert_request_args_to_descriptor_query(kwargs, self.index)
        query = convert_descriptor_query_to_search_query(descriptor_request, self.index)

        tiles = []  # (tile query, attributes)
        for su in self.index.storage.search(**query):
            if su_in_cell(su, x_index, y_index, xy_index):
                slices = get_tiles_for_su(su)
                for data_slice in slices:
                    tile_query = {
                        'xy_index': su.tile_index,
                        'storage_type': su.storage_type.name,
                    }
                    for (dim, val) in data_slice:
                        tile_query[dim] = val
                    tile_attributes = {
                        'path': str(su.local_path),
                        'description': su.storage_type.description,
                        'metadata': su.storage_type.document[u'match'][u'metadata']
                    }
                    tiles.append(tuple([tile_query, tile_attributes]))
        return tiles

    def list_variables(self, storage_type):
        """Lists the variables for a given ``storage_type`` name, from :meth:`list_storage_type_names()`.

            .. warning::
                This function is under development, and is subject to change.

        :param storage_type: Name of the the storage type
        :return: dict
        """
        storage_type = self.index.storage.types.get_by_name(storage_type)
        return {k: {} for k in storage_type.measurements.keys()}

    def __repr__(self):
        return "API<index={!r}>".format(self.index)


def main():
    agdc_api = API()
    desc = agdc_api.get_descriptor()
    print(desc)


if __name__ == '__main__':
    main()
