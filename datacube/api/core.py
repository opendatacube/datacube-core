
from __future__ import absolute_import, division, print_function

import logging
from itertools import groupby
from collections import defaultdict, OrderedDict

import pandas
import numpy
import xarray
import rasterio
import rasterio.crs
from rasterio.coords import BoundingBox
from osgeo import ogr

from ..compat import string_types, integer_types
from ..index import index_connect
from ..model import StorageUnit, GeoPolygon, GeoBox, Range, Coordinate, Variable
from ..model import _DocReader as DocReader
from ..storage.storage import DatasetSource, fuse_sources, RESAMPLING
from ..storage import netcdf_writer

_LOG = logging.getLogger(__name__)


class Datacube(object):
    """
    Interface to search, read and write a datacube

    Functions in current API:

    AA/EE functions
    ===============
    get_descriptor
    get_data

    List search fields
    ==================
    list_fields
    list_field_values
    list_all_field_values

    List collections (all questionable...)
    ================
    list_storage_units
    list_storage_type_names
    list_products
    list_variables

    Data Access
    ===========
    get_dataset
    get_data_array  (Just get_dataset with a hat)

    Legacy tile-based workflow
    ==========================
    list_cells
    list_tiles
    get_dataset_by_cell
    get_data_array_by_cell

    """
    def __init__(self, index=None, config=None, app=None):
        """
        Defines a connection to a datacube index and file storage
        :return: Datacube object
        """
        if index is None:
            if config is not None:
                self.index = index_connect(config, application_name=app)
            else:
                self.index = index_connect(application_name=app)
        else:
            self.index = index


    def datasets(self):
        """
        List of products as a Pandas DataTable
        :return:
        """
        def to_row(dt):
            row = {
                'id': dt.id,
                'dataset': dt.name,
                'description': dt.definition['description'],
            }
            good_fields = {}
            # TODO: Move to DatasetType
            offsets = {name: field.offset if hasattr(field, 'offset') else [name]
                       for name, field in dt.metadata_type.dataset_fields.items()}
            dr = DocReader(offsets, dt.metadata)
            for k, v in dr._field_offsets.items():  # pylint: disable=protected-access
                try:
                    good_fields[k] = dr.__getattr__(k)
                except KeyError:
                    pass
            row.update(good_fields)
            if dt.gridspec is not None:
                row.update({
                    'crs': dt.crs,
                    'spatial_dimensions': dt.spatial_dimensions,
                    'tile_size': dt.tile_size,
                    'resolution': dt.resolution,
                })
            return row
        return pandas.DataFrame([to_row(dt) for dt in self.index.datasets.types.get_all()])

    def variables(self):
        variables = []
        dts = self.index.datasets.types.get_all()
        for dt in dts:
            if dt.measurements:
                for name, measurement in dt.measurements.items():
                    row = {
                        'dataset': dt.name,
                        'variable': name,
                    }
                    if 'attrs' in measurement:
                        row.update(measurement['attrs'])
                    # row.update({k: v for k, v in measurement.items() if k != 'attrs'})
                    variables.append(row)
        return pandas.DataFrame.from_dict(variables).set_index(['dataset', 'variable'])

    # def get_dataset(self, variables=None, group_by=None, set_nan=False, include_lineage=False, **kwargs):
    #     # Split kwargs into dataset_type search fields and dimension search fields
    #
    #         # Convert spatial dimension search fields into geobox
    #
    #         # Convert kwargs to index search query
    #
    #     # Search for datasets
    #     datasets = self.index.datasets.search(**kwargs)
    #
    #     # Group by dataset type
    #     datasets_by_type = defaultdict(list)
    #     for dataset in datasets:
    #         datasets_by_type[dataset.type.name].append(dataset)
    #
    #     # Get output geobox from query
    #
    #     # Or work out geobox from extents of requested datasets
    #
    #     response = {}
    #
    #     # Get dataset data
    #     for type_name, datasets in datasets_by_type.items():
    #         dataset_type = self.index.datasets.types.get_by_name(type_name)
    #         crs = dataset_type.crs
    #         polygon = polygon.to_crs(crs)
    #         geobox = GeoBox.from_geopolygon(polygon, dataset_type.resolution)
    #         data_vars = OrderedDict()
    #         group_func = _get_group_by_func()
    #         datasets.sort(key=group_func)
    #         groups = [(key, list(group)) for key, group in groupby(datasets, group_func)]
    #         for m_name, m_props in dataset_type.measurements.items():
    #             if variables is None or m_name in variables:
    #                 data_vars[m_name] = self.product_data_measurement(groups, m_name, m_props, geobox)
    #         attrs = {
    #             'extent': geobox.extent,
    #             'affine': geobox.affine,
    #             'crs': geobox.crs
    #         }
    #         if 'global_attributes' in dataset_type.definition:
    #             attrs.update(dataset_type.definition['global_attributes'])
    #         response[type_name] = xarray.Dataset(data_vars, attrs=attrs)
    #     return response

    def product_observations(self, type_name, geopolygon=None, group_func=None, **kwargs):
        if geopolygon:
            geo_bb = geopolygon.to_crs('EPSG:4326').boundingbox
            kwargs['lat'] = Range(geo_bb.bottom, geo_bb.top)
            kwargs['lon'] = Range(geo_bb.left, geo_bb.right)
        # TODO: pull out full datasets lineage?
        datasets = self.index.datasets.search_eager(type=type_name, **kwargs)

        if geopolygon:
            datasets = [dataset for dataset in datasets
                        if _check_intersect(geopolygon, dataset.extent.to_crs(geopolygon.crs_str))]
        group_func = _get_group_by_func(group_func)
        datasets.sort(key=group_func)
        groups = [(key, list(group)) for key, group in groupby(datasets, group_func)]

        return groups

    @staticmethod
    def product_data(groups, geobox, measurements, fuse_func=None):
        assert groups

        result = xarray.Dataset(attrs={'extent': geobox.extent, 'affine': geobox.affine, 'crs': geobox.crs})
        result['time'] = ('time', numpy.array([v[0] for v in groups]), {'units': 'seconds since 1970-01-01 00:00:00'})
        for name, v in geobox.coordinate_labels.items():
            result[name] = (name, v, {'units': geobox.coordinates[name].units})

        for name, stuffs in measurements.items():
            data = numpy.empty((len(groups),) + geobox.shape, dtype=stuffs['dtype'])
            for index, (_, sources) in enumerate(groups):
                fuse_sources([DatasetSource(dataset, name) for dataset in sources],
                             data[index],
                             geobox.affine,
                             geobox.crs_str,
                             stuffs.get('nodata'),
                             resampling=RESAMPLING.nearest,
                             fuse_func=fuse_func)
            result[name] = (('time',) + geobox.dimensions, data, {
                'nodata': stuffs.get('nodata'),
                'units': stuffs.get('units', '1')
            })

        extra_md = numpy.empty(len(groups), dtype=object)
        for index, (_, sources) in enumerate(groups):
            extra_md[index] = sources
        result['sources'] = (['time'], extra_md)

        return result

    # def describe(self, type_name, variables=None, group_by=None, **kwargs):
    #     polygon = _query_to_geopolygon(**kwargs)
    #
    #     group_by_func = _get_group_by_func(group_by)
    #
    #     groups = self.product_observations(type_name, polygon, group_by_func)
    #
    #     times = sorted(numpy.array([group[0] for group in groups], dtype='datetime64[ns]'))
    #     dataset_count = sum([len(group[1]) for group in groups])
    #
    #     # Get dataset data
    #     dataset_type = self.index.datasets.types.get_by_name(type_name)
    #     crs = dataset_type.crs
    #     dims = dataset_type.gridspec['dimension_order']
    #     polygon = polygon.to_crs(crs)
    #     geobox = GeoBox.from_geopolygon(polygon, dataset_type.resolution)
    #     shape = dict(zip(geobox.dimensions, geobox.shape))
    #
    #     shape['time'] = len(groups)
    #     shape_str = '({})'.format(', '.join('{}: {}'.format(dim, shape[dim]) for dim in dims))
    #
    #     ranges = {dim: (geobox.coordinate_labels[dim][0], geobox.coordinate_labels[dim][-1])
    #               for dim in geobox.dimensions}
    #     ranges['time'] = (str(times[0]), (str(times[-1])))
    #
    #     indent = 4 * ' '
    #     print('Dimensions:\t', shape_str)
    #
    #     print('Coordinates:')
    #     width = max(len(dim) for dim in dims) + 2
    #     for dim in dims:
    #         print(indent, '{}:'.format(dim).ljust(width), '{}, {}'.format(*(ranges[dim])))
    #
    #     print('Data variables:')
    #
    #     if isinstance(variables, string_types):
    #         variables = [variables]
    #     if variables is None and dataset_type.measurements:
    #         variables = dataset_type.measurements.keys()
    #     for variable in variables:
    #         measurement_props = dataset_type.measurements[variable]
    #         print(indent, variable, ':\t',
    #               'dtype:\t', measurement_props['dtype'], '\t',
    #               'nodata:\t', measurement_props['nodata'],
    #               sep='')
    #
    #     print('Datasets:\t{}'.format(dataset_count))
    #
    #     print('Attributes:')
    #     attrs = {
    #         'extent': geobox.extent,
    #         'affine': geobox.affine,
    #         'crs': geobox.crs
    #     }
    #     if 'global_attributes' in dataset_type.definition:
    #         attrs.update(dataset_type.definition['global_attributes'])
    #     for k, v in attrs.items():
    #         print(indent, k, ': ', v, sep='')


class API(object):
    def __init__(self, index):
        self.datacube = Datacube(index)

    def get_descriptor_for_dataset(self, dataset_type, datasets, group_func, geopolygon=None):
        dataset_descriptor = {}
        irregular_dims = ['time', 't', 'T']  # TODO: get irregular dims from dataset_type

        if not dataset_type.gridspec:
            return None

        if not geopolygon:
            geopolygon = _get_bounds(datasets, dataset_type)

        datasets.sort(key=group_func)
        groups = [(key, list(group)) for key, group in groupby(datasets, group_func)]

        dataset_descriptor['result_min'] = []
        dataset_descriptor['result_max'] = []
        dataset_descriptor['result_shape'] = []
        dataset_descriptor['irregular_indices'] = {}

        resolution = [dataset_type.gridspec['resolution'][dim] for dim in dataset_type.spatial_dimensions]
        geobox = GeoBox.from_geopolygon(geopolygon, resolution)
        dims = dataset_type.gridspec['dimension_order']
        spatial_dims = dataset_type.spatial_dimensions
        dataset_descriptor['dims'] = dims
        for dim in dims:
            if dim in spatial_dims:
                coords = geobox.coordinate_labels[dim]
            elif dim in irregular_dims:
                # groups will define irregular_dims
                coords = [group[0] for group in groups]
                dataset_descriptor['irregular_indices'][dim] = coords
            else:
                # not supported yet...
                continue
            dataset_descriptor['result_min'].append(min(coords))
            dataset_descriptor['result_max'].append(max(coords))
            dataset_descriptor['result_shape'].append(len(coords))

        if dataset_type.measurements:
            dataset_descriptor['variables'] = self.get_descriptor_for_measurements(dataset_type)
        return dataset_descriptor

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

    def get_descriptor(self, descriptor_request=None):
        search_terms = _convert_descriptor_request_to_search_query(descriptor_request)
        geopolygon = _descriptor_request_to_geopolygon(descriptor_request)
        if geopolygon:
            geo_bb = geopolygon.to_crs('EPSG:4326').boundingbox
            search_terms['lat'] = Range(geo_bb.bottom, geo_bb.top)
            search_terms['lon'] = Range(geo_bb.left, geo_bb.right)

        datasets = self.datacube.index.datasets.search(**search_terms)
        datasets_by_type = defaultdict(list)
        for dataset in datasets:
            datasets_by_type[dataset.type.name].append(dataset)

        group_func = _get_group_by_func()  # TODO: Get the group func out of the dims request

        descriptor = {}
        for type_name, datasets in datasets_by_type.items():
            dataset_type = self.datacube.index.datasets.types.get_by_name(type_name)
            dataset_descriptor = self.get_descriptor_for_dataset(dataset_type, datasets, group_func, geopolygon)
            if dataset_descriptor:
                descriptor[type_name] = dataset_descriptor
        return descriptor

    def get_data(self, descriptor_request):
        return {}


def _check_intersect(a, b):
    def ogr_poly(poly):
        ring = ogr.Geometry(ogr.wkbLinearRing)
        for point in poly.points:
            ring.AddPoint_2D(*point)
        ring.AddPoint_2D(*poly.points[0])
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        return poly
    a = ogr_poly(a)
    b = ogr_poly(b)
    return a.Intersects(b) and not a.Touches(b)


def _value_to_range(value):
    if isinstance(value, string_types + integer_types + (float,)):
        value = float(value)
        return value, value
    else:
        return float(value[0]), float(value[-1])


def _descriptor_request_to_geopolygon(descriptor_request):
    if 'dimensions' in descriptor_request:
        dims = descriptor_request['dimensions']
        geo_params = {dim: v['range'] for dim, v in dims if 'range' in v}
        crs = [v['coordinate_reference_system'] for dim, v in dims if 'coordinate_reference_system' in v]
        if crs:
            geo_params['crs'] = crs
        return _query_to_geopolygon(**geo_params)
    return None


def _query_to_geopolygon(**kwargs):
    input_crs = None
    input_coords = {'left': None, 'bottom': None, 'right': None, 'top': None}
    for key, value in kwargs.items():
        key = key.lower()
        if key in ['latitude', 'lat', 'y']:
            input_coords['top'], input_coords['bottom'] = _value_to_range(value)
        if key in ['longitude', 'lon', 'x']:
            input_coords['left'], input_coords['right'] = _value_to_range(value)
        if key in ['crs', 'coordinate_reference_system']:
            input_crs = value
    input_crs = input_crs or 'EPSG:4326'
    if any(v is not None for v in input_coords.values()):
        points = [(input_coords['left'], input_coords['top']),
                  (input_coords['right'], input_coords['top']),
                  (input_coords['right'], input_coords['bottom']),
                  (input_coords['left'], input_coords['bottom']),
                 ]
        return GeoPolygon(points, input_crs)
    return None


def _get_group_by_func(group_by=None):
    if hasattr(group_by, '__call__'):
        return group_by
    if group_by is None or group_by == 'time':
        def just_time(ds):
            try:
                return ds.time
            except KeyError:
                # TODO: Remove this mess when issue #119 is resolved
                return ds.metadata_doc['acquisition']['aos']
        return just_time
    elif group_by == 'day':
        return lambda ds: ds.time.date()
    elif group_by == 'solar_day':
        raise NotImplementedError('The group by `solar_day` feature is coming soon.')
    else:
        raise LookupError('No group_by function found called {}'.format(group_by))


def _get_bounds(datasets, dataset_type):
    left = min([d.bounds.left for d in datasets])
    right = max([d.bounds.right for d in datasets])
    top = max([d.bounds.top for d in datasets])
    bottom = min([d.bounds.bottom for d in datasets])
    return GeoPolygon.from_boundingbox(BoundingBox(left, bottom, right, top), dataset_type.crs)


def _convert_descriptor_request_to_search_query(descriptor_request):
    return {}
