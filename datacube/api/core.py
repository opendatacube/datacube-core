from __future__ import absolute_import, division, print_function

import logging
from itertools import groupby, chain
from collections import defaultdict, namedtuple

import pandas
import numpy
import xarray
from rasterio.coords import BoundingBox
from osgeo import ogr

from ..index import index_connect
from ..model import GeoPolygon, GeoBox, Range, CRS
from ..model import _DocReader as DocReader
from ..storage.storage import DatasetSource, fuse_sources, RESAMPLING

_LOG = logging.getLogger(__name__)


Group = namedtuple('Group', ['key', 'datasets'])


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
            if dt.grid_spec is not None:
                row.update({
                    'crs': dt.grid_spec.crs,
                    'spatial_dimensions': dt.grid_spec.dimensions,
                    'tile_size': dt.grid_spec.tile_size,
                    'resolution': dt.grid_spec.resolution,
                })
            return row
        return pandas.DataFrame([to_row(dataset_type) for dataset_type in self.index.datasets.types.get_all()])

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

    def product_observations(self, type_name, geopolygon=None, **kwargs):
        if geopolygon:
            geo_bb = geopolygon.to_crs(CRS('EPSG:4326')).boundingbox
            kwargs['lat'] = Range(geo_bb.bottom, geo_bb.top)
            kwargs['lon'] = Range(geo_bb.left, geo_bb.right)
        # TODO: pull out full datasets lineage?
        datasets = self.index.datasets.search_eager(type=type_name, **kwargs)
        # All datasets will be same type, can make assumptions
        if geopolygon:
            datasets = [dataset for dataset in datasets
                        if _check_intersect(geopolygon, dataset.extent.to_crs(geopolygon.crs))]
            # Check against the bounding box of the original scene, can throw away some portions

        return datasets

    @staticmethod
    def product_sources(datasets, group_func, dimension, units):
        datasets.sort(key=group_func)
        groups = [Group(key, tuple(group)) for key, group in groupby(datasets, group_func)]

        data = numpy.empty(len(groups), dtype=object)
        for index, (_, sources) in enumerate(groups):
            data[index] = sources
        coord = numpy.array([v.key for v in groups])
        sources = xarray.DataArray(data, dims=[dimension], coords=[coord])
        sources[dimension].attrs['units'] = units
        return sources

    @staticmethod
    def product_data(sources, geobox, measurements, fuse_func=None):
        # GeoPolygon defines a boundingbox with a CRS
        # Geobox is a GeoPolygon with a resolution
        # Geobox has named dimensions, eg lat/lon, x/y

        result = xarray.Dataset(attrs={'extent': geobox.extent, 'crs': geobox.crs})
        for name, coord in sources.coords.items():
            result[name] = coord
        for name, coord in geobox.coordinates.items():
            result[name] = (name, coord.labels, {'units': coord.units})

        for name, measurement in measurements.items():
            data = numpy.empty(sources.shape + geobox.shape, dtype=measurement['dtype'])

            for index, datasets in numpy.ndenumerate(sources.values):
                fuse_sources([DatasetSource(dataset, name) for dataset in datasets],
                             data[index],  # Output goes here
                             geobox.affine,
                             geobox.crs,
                             measurement.get('nodata'),
                             resampling=RESAMPLING.nearest,
                             fuse_func=fuse_func)
            result[name] = (sources.dims + geobox.dimensions, data, {
                'nodata': measurement.get('nodata'),
                'units': measurement.get('units', '1')
            })
        return result

    @staticmethod
    def variable_data(groups, geobox, name, measurement, fuse_func=None):
        assert groups

        time_coord = xarray.Coordinate('time', numpy.array([v.key for v in groups]),
                                       attrs={'units': 'seconds since 1970-01-01 00:00:00'})
        coords = [time_coord]
        for dim, coord in geobox.coordinate_labels.items():
            coords.append(xarray.Coordinate(dim, coord.labels, attrs={'units': coord.units}))

        data = numpy.empty((len(groups),) + geobox.shape, dtype=measurement['dtype'])
        for index, (_, sources) in enumerate(groups):
            fuse_sources([DatasetSource(dataset, name) for dataset in sources],
                         data[index],
                         geobox.affine,
                         geobox.crs_str,
                         measurement.get('nodata'),
                         resampling=RESAMPLING.nearest,
                         fuse_func=fuse_func)

        result = xarray.DataArray(data,
                                  coords=coords,
                                  dims=[coord.name for coord in coords],
                                  name=name,
                                  attrs={
                                      'extent': geobox.extent,
                                      'affine': geobox.affine,
                                      'crs': geobox.crs_str,
                                      'nodata': measurement.get('nodata'),
                                      'units': measurement.get('units', '1')
                                  })

        # TODO: Include source metadata
        # extra_md = numpy.empty(len(groups), dtype=object)
        # for index, (_, sources) in enumerate(groups):
        #     extra_md[index] = sources
        # result['sources'] = (['time'], extra_md)

        return result

    # def describe(self, type_name, variables=None, group_by=None, **kwargs):
    #     polygon = _query_to_geopolygon(**kwargs)
    #
    #     group_by_func = _get_group_by_func(group_by)
    #
    #     groups = self.product_observations(type_name, polygon, group_by_func)
    #
    #     times = sorted(numpy.array([group.key for group in groups], dtype='datetime64[ns]'))
    #     dataset_count = sum([len(group.datasets) for group in groups])
    #
    #     # Get dataset data
    #     dataset_type = self.index.datasets.types.get_by_name(type_name)
    #     crs = dataset_type.crs
    #     dims = dataset_type.grid_spec['dimension_order']
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

    def __repr__(self):
        return "Datacube<index={!r}>".format(self.index)


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


def _get_bounds(datasets, dataset_type):
    left = min([d.bounds.left for d in datasets])
    right = max([d.bounds.right for d in datasets])
    top = max([d.bounds.top for d in datasets])
    bottom = min([d.bounds.bottom for d in datasets])
    return GeoPolygon.from_boundingbox(BoundingBox(left, bottom, right, top), dataset_type.grid_spec.crs)
