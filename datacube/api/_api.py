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
from ..index.postgres._fields import RangeDocField  # pylint: disable=protected-access
from .core import Datacube, Group, _get_bounds
from .query import Query

_LOG = logging.getLogger(__name__)


class API(object):
    def __init__(self, index=None, datacube=None):
        if datacube is not None:
            self.datacube = datacube
        elif index is not None:
            self.datacube = Datacube(index, app='Datacube-API')
        else:
            self.datacube = Datacube(app='Datacube-API')

    def get_descriptor_for_dataset(self, dataset_type, datasets, group_func, geopolygon=None,
                                   include_storage_units=True):
        dataset_descriptor = {}
        irregular_dims = ['time', 't', 'T']  # TODO: get irregular dims from dataset_type

        if not (dataset_type.grid_spec and dataset_type.grid_spec.dimensions):
            return None

        if not geopolygon:
            geopolygon = _get_bounds(datasets, dataset_type)

        datasets.sort(key=group_func)
        groups = [Group(key, list(group)) for key, group in groupby(datasets, group_func)]

        dataset_descriptor['result_min'] = []
        dataset_descriptor['result_max'] = []
        dataset_descriptor['result_shape'] = []
        dataset_descriptor['irregular_indices'] = {}

        geobox = GeoBox.from_geopolygon(geopolygon.to_crs(dataset_type.grid_spec.crs),
                                        dataset_type.grid_spec.resolution)
        dims = dataset_type.dimensions
        spatial_dims = dataset_type.grid_spec.dimensions
        dataset_descriptor['dims'] = dims
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
            dataset_descriptor['result_max'].append(max(coords))
            dataset_descriptor['result_shape'].append(len(coords))
        if dataset_type.measurements:
            dataset_descriptor['variables'] = self.get_descriptor_for_measurements(dataset_type)

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
            times = [dataset.time for dataset in datasets]
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
    def get_descriptor_for_measurements(dataset_type):
        data_vars = {}
        for k, v in dataset_type.measurements.items():
            var_desc = {}
            if 'dtype' in v:
                var_desc['datatype'] = v['dtype']
            if 'nodata' in v:
                var_desc['nodata_value'] = v['nodata']
            data_vars[k] = var_desc
        return data_vars

    def get_descriptor(self, descriptor_request=None, include_storage_units=True):
        query = Query.from_descriptor_request(descriptor_request)
        descriptor = {}
        datasets_by_type = self.search_datasets_by_type(**query.search_terms)
        for dataset_type, datasets in datasets_by_type.items():
            dataset_descriptor = self.get_descriptor_for_dataset(dataset_type, datasets,
                                                                 query.group_by_func,
                                                                 query.geopolygon)
            if dataset_descriptor:
                descriptor[dataset_type.name] = dataset_descriptor
        return descriptor

    def search_datasets_by_type(self, **query):
        datasets = self.datacube.index.datasets.search(**query)
        datasets_by_type = defaultdict(list)
        for dataset in datasets:
            datasets_by_type[dataset.type].append(dataset)
        return datasets_by_type

    def get_dataset_groups(self, query):
        dataset_groups = {}
        group_func = query.group_by_func

        datasets_by_type = self.search_datasets_by_type(**query.search_terms)
        for dataset_type, datasets in datasets_by_type.items():
            if dataset_type.grid_spec:
                datasets.sort(key=group_func)
                dataset_groups[dataset_type] = [Group(key, list(group))
                                                for key, group in groupby(datasets, group_func)]
        return dataset_groups

    def get_data(self, data_request, dataset_groups=None):
        """

        :param data_request:
        :param dataset_groups: dict mapping dataset_type to sequence of Group pairs.
            If not provided, the index is queried.
        :type dataset_groups: dict{dataset_type: list(Group(key, list(datasets)))}
        :return:
        """
        query = Query.from_descriptor_request(data_request)

        # If the user has not provided `groups` from get_descriptor call, retrieve them from the index
        if dataset_groups is None:
            dataset_groups = self.get_dataset_groups(query)

        return {dt.name: self.get_data_for_type(dt, groups, query.variables, query.geopolygon, query.slices)
                for dt, groups in dataset_groups.items()}

    def get_data_for_type(self, dataset_type, groups, variables, geopolygon, slices=None):
        irregular_dims = ['time', 't']  # TODO: get irregular dims from dataset_type
        dt_data = {}
        datasets = list(chain.from_iterable(g.datasets for g in groups))
        if not geopolygon:
            geopolygon = _get_bounds(datasets, dataset_type)
        geobox = GeoBox.from_geopolygon(geopolygon.to_crs(dataset_type.grid_spec.crs),
                                        dataset_type.grid_spec.resolution)
        if slices:
            geo_slices = [slices.get(dim, slice(None)) for dim in geobox.dimensions]
            geobox = geobox[geo_slices]
            for dim, dim_slice in slices.items():
                if dim.lower() in irregular_dims:
                    groups = groups[dim_slice]
        dt_data.update(self.get_data_for_dims(dataset_type, groups, geobox))
        dt_data.update(self.get_data_for_measurement(dataset_type, groups, variables, geobox))
        return dt_data

    @staticmethod
    def get_data_for_dims(dataset_type, groups, geobox):
        irregular_dims = ['time', 't']  # TODO: get irregular dims from dataset_type
        dims = dataset_type.dimensions
        dt_data = {
            'dimemsions': dims,
            'indicies': [],
            'element_sizes': [],
            'coordinate_reference_systems': [],
        }
        for dim in dims:
            if dim in dataset_type.spatial_dimensions:
                dt_data['indicies'].append(geobox.coordinates[dim].labels)
                dim_i = dataset_type.spatial_dimensions.index(dim)
                dt_data['element_sizes'].append(dataset_type.grid_spec.resolution[dim_i])
                dt_data['coordinate_reference_systems'].append(geobox.crs_str)
            elif dim.lower() in irregular_dims:
                # groups define irregular_dims
                coords = [group.key for group in groups]
                dt_data['indicies'].append(coords)
                if len(coords) < 2:
                    dt_data['element_sizes'].append(numpy.NaN)
                    dt_data['coordinate_reference_systems'].append('')
                else:
                    dt_data['element_sizes'].append(abs(coords[0] - coords[1]))
                    dt_data['coordinate_reference_systems'].append('')
            else:
                dt_data['indicies'].append([])
                dt_data['element_sizes'][dim] = numpy.NaN
                dt_data['coordinate_reference_systems'].append('')
        return dt_data

    def get_data_for_measurement(self, dataset_type, groups, variables, geobox):
        dt_data = {
            'arrays': {}
        }
        for measurement_name, measurement in dataset_type.measurements.items():
            if variables is None or measurement_name in variables:
                dt_data['arrays'][measurement_name] = self.datacube.variable_data(groups, geobox,
                                                                                  measurement_name, measurement)
        return dt_data

    def get_query(self, descriptor=None):
        """
        Parses the descriptor query into the following parts:
         query = {
             'type': 'ls5_nbar_albers',
             'variables': ['red', 'blue', 'green'],
             'search': {
                 'platform': 'LANDSAT_5',
                 'product': 'nbar',
                 'time': Range(datetime.datetime(2001, 1, 1), datetime.datetime(2006, 12, 31))
             },
             'geopolygon': GeoPolygon([], 'crs'),
             'group_by': {'time': 'solar_day'}
             'slices': {
                 'time': slice(0, 250),
                 'x': slice(0, 250),  # Need to convert to match spatial dims of output
                 'y': slice(0, 250),  #
             }
          }
        """
        if descriptor is None:
            descriptor = {}
        if not hasattr(descriptor, '__getitem__'):
            raise ValueError('Could not understand descriptor {}'.format(descriptor))
        remaining_keys = set(descriptor.keys())
        query = {
            'search': {},
        }

        type_keys = [key for key in remaining_keys if key in ('storage_type', 'type', 'dataset_type')]
        for key in type_keys:
            remaining_keys.remove(key)
            query['type'] = descriptor[key]

        if 'variables' in remaining_keys:
            remaining_keys.remove('variables')
            query['variables'] = descriptor['variables']

        mt = self.datacube.index.metadata_types.get_by_name('eo')  # TODO: ???
        known_fields = [field_name for field_name, field in mt.dataset_fields.items()
                        if not isinstance(field, RangeDocField)]
        found_fields = [key for key in remaining_keys if key in known_fields]
        for key in found_fields:
            remaining_keys.remove(key)
            query['search'][key] = descriptor[key]

        # for key in remaining_keys:
        #     if key.lower() in ['x', 'lon', 'long', 'longitude', 'projection_x_coordinate']:

        return query

    def __repr__(self):
        return "API<index={!r}>".format(self.index)


def main():
    agdc_api = API()
    desc = agdc_api.get_descriptor()
    print(desc)


if __name__ == '__main__':
    main()
