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
from itertools import chain, groupby

import numpy

from ..model import GeoBox
from .core import Datacube, Group, get_bounds, datatset_type_to_row
from .query import DescriptorQuery

_LOG = logging.getLogger(__name__)


class API(object):
    """
    Interface for use by the ``AnalyticsEngine`` and ``ExecutionEngine`` modules.
    """
    def __init__(self, index=None, app=None, datacube=None):
        """
        Creates the interface for query and storage access.

        If no datacube or index is given, the default configuration is used for database connection, etc.

        :param index: The database index to use, from :py:class:`datacube.index.index_connect`
        :type index: :py:class:`datacube.index._api.Index` or None
        :param app: A short, alphanumeric name to identify this application.
            The application name is used to track down problems with database queries, so it is strongly
            advised that be used.  If an index is supplied, application name is ignored.
        :type app: string, required if no index is given
        :param datacube:
        :type datacube: :class:`datacube.Datacube`
        """
        if datacube is not None:
            self.datacube = datacube
        elif index is not None:
            self.datacube = Datacube(index)
        else:
            app = app or 'Datacube-API'
            self.datacube = Datacube(app=app)

    def _get_descriptor_for_dataset(self, dataset_type, datasets, group_by, geopolygon=None,
                                    include_storage_units=True):
        dataset_descriptor = {}

        if not (dataset_type.grid_spec and dataset_type.grid_spec.dimensions):
            return None

        if not geopolygon:
            geopolygon = get_bounds(datasets, dataset_type.grid_spec.crs)

        datasets.sort(key=group_by.group_by_func)
        groups = [Group(key, list(group)) for key, group in groupby(datasets, group_by.group_by_func)]

        dataset_descriptor['result_min'] = tuple()
        dataset_descriptor['result_max'] = tuple()
        dataset_descriptor['result_shape'] = tuple()
        dataset_descriptor['irregular_indices'] = {}

        geobox = GeoBox.from_geopolygon(geopolygon.to_crs(dataset_type.grid_spec.crs),
                                        dataset_type.grid_spec.resolution)
        dims = dataset_type.dimensions
        spatial_dims = dataset_type.grid_spec.dimensions
        dataset_descriptor['dimensions'] = list(dims)
        for dim in dims:
            if dim in spatial_dims:
                coords = geobox.coordinates[dim].values
            elif dim == group_by.dimension:
                coords = [group.key for group in groups]
                dataset_descriptor['irregular_indices'][dim] = coords
            else:
                # not supported yet...
                continue
            # this is here due to the fact that not every dataset we have ingested will have
            # data for every area... This was broken on commit 5b83ea6ec2f7fab5ecd2fe40fba02db8786ec711, Aug 4.
            if len(coords) > 0:
                dataset_descriptor['result_min'] += (min(coords),)
                dataset_descriptor['result_max'] += (max(coords),)
                dataset_descriptor['result_shape'] += (len(coords),)

        if dataset_type.measurements:
            dataset_descriptor['variables'] = self._get_descriptor_for_measurements(dataset_type)

        dataset_descriptor['groups'] = (dataset_type, groups)

        if include_storage_units:
            dataset_descriptor['storage_units'] = self._compute_storage_units(dataset_type, datasets)

        return dataset_descriptor

    @staticmethod
    def _compute_storage_units(dataset_type, datasets):
        storage_units = {}

        def dataset_path(ds):
            return str(ds.local_path)

        datasets.sort(key=dataset_path)
        for path, datasets in groupby(datasets, key=dataset_path):
            datasets = list(datasets)
            su = {}
            times = [dataset.center_time for dataset in datasets]
            xs = [x for dataset in datasets for x in (dataset.bounds.left, dataset.bounds.right)]
            ys = [y for dataset in datasets for y in (dataset.bounds.top, dataset.bounds.bottom)]
            su['storage_shape'] = (len(times),) + dataset_type.grid_spec.tile_resolution
            su['storage_min'] = min(times), min(ys), min(xs)
            su['storage_max'] = max(times), max(ys), max(xs)
            su['storage_path'] = path
            su['irregular_indices'] = {'time': times}

            storage_units[(min(times), max(ys), min(xs))] = su
        return storage_units

    @staticmethod
    def _get_descriptor_for_measurements(dataset_type):
        data_vars = {}
        for k, v in dataset_type.measurements.items():
            var_desc = {}
            if 'dtype' in v:
                var_desc['datatype_name'] = v['dtype']
            if 'nodata' in v:
                var_desc['nodata_value'] = v['nodata']
            data_vars[k] = var_desc
        return data_vars

    def get_descriptor(self, descriptor_request=None, include_storage_units=True):
        """
        Gets the metadata for a ``AnalyticsEngine`` query.
        All fields are optional.

        **Dimensions**

            Dimensions can specify a range by label, and optionally a CRS to interpret the label.
            The default CRS interpretation for geospatial dimensions (longitude/latitude or x/y) is WGS84/EPSG:4326,
            even if the resulting dimension is in another projection.

        :param descriptor_request: The request query, formatted as:
            ::

                descriptor_request = {
                    'platform': 'LANDSAT_8',
                    'product_type': 'nbar',
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
                            'group_by': 'solar_day'
                        }
                    },
                }

        :type descriptor_request: dict or None
        :param include_storage_units: Include the list of storage units
        :type include_storage_units: bool, optional
        :return: A descriptor dict of the query, containing the metadata of the request
            ::

                descriptor = {
                    'ls5_nbar_albers': { # product identifier
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

        .. seealso:: :meth:`get_descriptor`
        """
        query = DescriptorQuery(descriptor_request)
        descriptor = {}

        for dataset_type, datasets in self.datacube.index.datasets.search_by_product(**query.search_terms):
            dataset_descriptor = self._get_descriptor_for_dataset(dataset_type, list(datasets),
                                                                  query.group_by,
                                                                  query.geopolygon,
                                                                  include_storage_units)
            if dataset_descriptor:
                descriptor[dataset_type.name] = dataset_descriptor
        return descriptor

    def _search_datasets_by_type(self, **query):
        return dict(self.datacube.index.datasets.search_by_product(**query))

    def _get_dataset_groups(self, query):
        dataset_groups = {}
        group_by = query.group_by

        for dataset_type, datasets in self.datacube.index.datasets.search_by_product(**query.search_terms):
            if dataset_type.grid_spec:
                dataset_groups[dataset_type] = self.datacube.group_datasets(list(datasets),
                                                                            group_by)
        return dataset_groups

    def get_data(self, data_request, dataset_groups=None, return_all=False):
        """
        Gets the data for a ``ExecutionEngine`` query.
        Function to return composite in-memory arrays.

        :param data_request: A dictionary containing the query parameters. All fields are optional.

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

        :type data_request: dict or None
        :param dataset_groups: dict mapping dataset_type to sequence of Group pairs.
            If not provided, the index is queried.

        :param return_all: If True, data from all requested products is returned,
            otherwise only the first result is returned.

        :type dataset_groups: dict{dataset_type: list(Group(key, list(datasets)))}
        :return: A mapping product

        .. seealso:: :meth:`get_descriptor`
        """
        query = DescriptorQuery(data_request)

        # If the user has not provided `groups` from get_descriptor call, retrieve them from the index
        if dataset_groups is None:
            dataset_groups = self._get_dataset_groups(query)

        all_datasets = {dt.name: self._get_data_for_type(dt, sources, query.measurements,
                                                         query.geopolygon, query.slices)
                        for dt, sources in dataset_groups.items()}
        if all_datasets and not return_all:
            type_name, data_descriptor = all_datasets.popitem()
            return data_descriptor
        return all_datasets

    def _get_data_for_type(self, dataset_type, sources, measurements, geopolygon, slices=None, chunks=None):
        dt_data = {}
        datasets = list(chain.from_iterable(g for _, g in numpy.ndenumerate(sources)))
        if not geopolygon:
            geopolygon = get_bounds(datasets, dataset_type.grid_spec.crs)
        geobox = GeoBox.from_geopolygon(geopolygon.to_crs(dataset_type.grid_spec.crs),
                                        dataset_type.grid_spec.resolution)
        if slices:
            _rename_spatial_keys(slices, geobox.dimensions)
            geo_slices = [slices.get(dim, slice(None)) for dim in geobox.dimensions]
            geobox = geobox[geo_slices]
            for dim, dim_slice in slices.items():
                if dim in sources.dims:
                    sources = sources.isel(dim=dim_slice)
        dt_data.update(self._get_data_for_dims(dataset_type, sources, geobox))
        dt_data.update(self._get_data_for_measurement(dataset_type, sources, measurements, geobox, dask_chunks=chunks))
        return dt_data

    @staticmethod
    def _get_data_for_dims(dataset_type, sources, geobox):
        dims = dataset_type.dimensions
        dt_data = {
            'dimensions': list(dims),
            'indices': {},
            'element_sizes': [],
            'coordinate_reference_systems': [],
            'size': tuple()
        }
        for dim in dims:
            if dim in dataset_type.grid_spec.dimensions:
                dt_data['indices'][dim] = geobox.coordinates[dim].values
                dim_i = dataset_type.grid_spec.dimensions.index(dim)
                dt_data['element_sizes'].append(abs(dataset_type.grid_spec.resolution[dim_i]))
                dt_data['coordinate_reference_systems'].append({
                    'reference_system_definition': str(geobox.crs),
                    'reference_system_unit': geobox.coordinates[dim].units
                })
                dt_data['size'] += (geobox.coordinates[dim].values.size, )
            elif dim in sources.dims:
                coords = sources.coords[dim].values
                dt_data['indices'][dim] = coords
                dt_data['size'] += (coords.size, )
                dt_data['coordinate_reference_systems'].append({
                    'reference_system_definition': 'UTC',
                    'reference_system_unit': 'seconds since 1970-01-01 00:00:00'
                })
                if len(coords) < 2:
                    dt_data['element_sizes'].append(numpy.NaN)
                else:
                    dt_data['element_sizes'].append(abs(coords[0] - coords[1]))
            else:
                raise NotImplementedError('Unsupported dimension type: ', dim)
        return dt_data

    def _get_data_for_measurement(self, dataset_type, sources, measurements, geobox, dask_chunks=None):
        dt_data = {
            'arrays': {}
        }
        for measurement_name, measurement in dataset_type.measurements.items():
            if measurements is None or measurement_name in measurements:
                dt_data['arrays'][measurement_name] = self.datacube.measurement_data(sources, geobox, measurement,
                                                                                     dask_chunks=dask_chunks)
        return dt_data

    def list_products(self):
        """
        Lists the products in the datacube.

        :return: list of dictionaries describing the products
        """
        return [datatset_type_to_row(dataset_type) for dataset_type in self.datacube.index.products.get_all()]

    def list_variables(self):
        """
        Lists the variables of products in the datacube.

        Variables are also referred to as measurements or bands.
        :return: list of dictionaries describing the variables
        """
        return self.datacube.list_measurements(with_pandas=False)

    def __repr__(self):
        return "API<datacube={!r}>".format(self.datacube.index)


SPATIAL_KEYS = [('latitude', 'lat', 'y'), ('longitude', 'lon', 'long', 'x')]


def _rename_spatial_keys(dictionary, dimensions):
    for alt_keys in SPATIAL_KEYS:
        match = [dim_key for dim_key in dimensions if dim_key in alt_keys]
        for dim_key in match:
            for old_key in alt_keys:
                if old_key in dictionary:
                    dictionary[dim_key] = dictionary.pop(old_key)


def main():
    agdc_api = API()
    desc = agdc_api.get_descriptor()
    print(desc)


if __name__ == '__main__':
    main()
