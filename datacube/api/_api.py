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
from .query import Query

_LOG = logging.getLogger(__name__)


class API(object):
    def __init__(self, index=None, app=None, datacube=None):
        if datacube is not None:
            self.datacube = datacube
        elif index is not None:
            self.datacube = Datacube(index)
        else:
            app = app or 'Datacube-API'
            self.datacube = Datacube(app=app)

    def _get_descriptor_for_dataset(self, dataset_type, datasets, group_func, geopolygon=None,
                                    include_storage_units=True):
        dataset_descriptor = {}
        irregular_dims = ['time', 't', 'T']  # TODO: get irregular dims from dataset_type

        if not (dataset_type.grid_spec and dataset_type.grid_spec.dimensions):
            return None

        if not geopolygon:
            geopolygon = get_bounds(datasets, dataset_type.grid_spec.crs)

        datasets.sort(key=group_func)
        groups = [Group(key, list(group)) for key, group in groupby(datasets, group_func)]

        dataset_descriptor['result_min'] = []
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
                coords = geobox.coordinates[dim].labels
            elif dim in irregular_dims:
                # groups will define irregular_dims
                coords = [group.key for group in groups]
                dataset_descriptor['irregular_indices'][dim] = coords
            else:
                # not supported yet...
                continue
            dataset_descriptor['result_min'].append(min(coords))
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
            su['storage_shape'] = tuple([len(times)] + dataset_type.grid_spec.tile_resolution)
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
        query = Query.from_descriptor_request(descriptor_request)
        descriptor = {}
        datasets_by_type = self._search_datasets_by_type(**query.search_terms)
        for dataset_type, datasets in datasets_by_type.items():
            dataset_descriptor = self._get_descriptor_for_dataset(dataset_type, datasets,
                                                                  query.group_by_func,
                                                                  query.geopolygon,
                                                                  include_storage_units)
            if dataset_descriptor:
                descriptor[dataset_type.name] = dataset_descriptor
        return descriptor

    def _search_datasets_by_type(self, **query):
        datasets = self.datacube.index.datasets.search(**query)
        datasets_by_type = defaultdict(list)
        for dataset in datasets:
            datasets_by_type[dataset.type].append(dataset)
        return datasets_by_type

    def _get_dataset_groups(self, query):
        dataset_groups = {}
        group_func = query.group_by_func

        datasets_by_type = self._search_datasets_by_type(**query.search_terms)
        for dataset_type, datasets in datasets_by_type.items():
            if dataset_type.grid_spec:
                dataset_groups[dataset_type] = self.datacube.product_sources(datasets, group_func, 'time',
                                                                             'seconds since 1970-01-01 00:00:00')
        return dataset_groups

    def get_data(self, data_request, dataset_groups=None, return_all=False):
        """

        :param data_request:
        :param dataset_groups: dict mapping dataset_type to sequence of Group pairs.
            If not provided, the index is queried.
        :param return_all: If True, data from all requested datatsets is returned,
            otherwise only the first result is returned.
        :type dataset_groups: dict{dataset_type: list(Group(key, list(datasets)))}
        :return:
        """
        query = Query.from_descriptor_request(data_request)

        # If the user has not provided `groups` from get_descriptor call, retrieve them from the index
        if dataset_groups is None:
            dataset_groups = self._get_dataset_groups(query)

        all_datasets = {dt.name: self._get_data_for_type(dt, sources, query.variables, query.geopolygon, query.slices)
                        for dt, sources in dataset_groups.items()}
        if all_datasets and not return_all:
            type_name, data_descriptor = all_datasets.popitem()
            return data_descriptor
        return all_datasets

    def _get_data_for_type(self, dataset_type, sources, variables, geopolygon, slices=None):
        dt_data = {}
        datasets = list(chain.from_iterable(g for _, g in numpy.ndenumerate(sources)))
        if not geopolygon:
            geopolygon = get_bounds(datasets, dataset_type.grid_spec.crs)
        geobox = GeoBox.from_geopolygon(geopolygon.to_crs(dataset_type.grid_spec.crs),
                                        dataset_type.grid_spec.resolution)
        if slices:
            geo_slices = [slices.get(dim, slice(None)) for dim in geobox.dimensions]
            geobox = geobox[geo_slices]
            for dim, dim_slice in slices.items():
                if dim in sources.dims:
                    sources = sources.isel(dim=dim_slice)
        dt_data.update(self._get_data_for_dims(dataset_type, sources, geobox))
        dt_data.update(self._get_data_for_measurement(dataset_type, sources, variables, geobox))
        return dt_data

    @staticmethod
    def _get_data_for_dims(dataset_type, sources, geobox):
        dims = dataset_type.dimensions
        dt_data = {
            'dimensions': dims,
            'indices': {},
            'element_sizes': [],
            'coordinate_reference_systems': [],
            'size': tuple()
        }
        for dim in dims:
            if dim in dataset_type.grid_spec.dimensions:
                dt_data['indices'][dim] = geobox.coordinates[dim].labels
                dim_i = dataset_type.grid_spec.dimensions.index(dim)
                dt_data['element_sizes'].append(abs(dataset_type.grid_spec.resolution[dim_i]))
                dt_data['coordinate_reference_systems'].append({
                    'reference_system_definition': str(geobox.crs),
                    'reference_system_unit': geobox.coordinates[dim].units
                })
                dt_data['size'] += (geobox.coordinates[dim].labels.size, )
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

    def _get_data_for_measurement(self, dataset_type, sources, variables, geobox):
        dt_data = {
            'arrays': {}
        }
        for measurement_name, measurement in dataset_type.measurements.items():
            if variables is None or measurement_name in variables:
                dt_data['arrays'][measurement_name] = self.datacube.variable_data_lazy(sources, geobox, measurement)
        return dt_data

    def list_products(self):
        return [datatset_type_to_row(dataset_type) for dataset_type in self.datacube.index.datasets.types.get_all()]

    def list_variables(self):
        return self.datacube.list_variables()

    def __repr__(self):
        return "API<datacube={!r}>".format(self.datacube.index)


def main():
    agdc_api = API()
    desc = agdc_api.get_descriptor()
    print(desc)


if __name__ == '__main__':
    main()
