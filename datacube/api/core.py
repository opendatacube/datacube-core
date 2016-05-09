
from __future__ import absolute_import, division, print_function

import logging
from itertools import groupby
from collections import defaultdict

import pandas
import numpy
import xarray
import rasterio
import rasterio.crs
from rasterio.coords import BoundingBox
from osgeo import ogr

from ..index import index_connect
from ..model import StorageUnit, GeoPolygon, GeoBox, Range, Coordinate, Variable
from ..storage.storage import DatasetSource, fuse_sources, RESAMPLING
from ..storage import netcdf_writer

_LOG = logging.getLogger(__name__)


class Datacube(object):
    """
    Interface to search, read and write a datacube

    Current functions:

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

    def products(self):
        """
        List of products as a Pandas DataTable
        :return:
        """
        def to_row(dt):
            row = {
                'id': dt.id,
                'name': dt.name,
                'description': dt.definition['description'],
                'data_vars': len(dt.definition['measurements']),
                'resolution': dt.resolution,
            }
            if dt.gridspec is not None:
                row.update({
                    'crs': dt.crs,
                    'spatial_dimensions': dt.spatial_dimensions,
                    'tile_size': dt.tile_size,
                })
            else:
                row.update({
                    'crs': '<?>',
                    'spatial_dimensions': '<?>',
                    'tile_size': '<?>',
                })
            return row
        return pandas.from_dict([to_row(dt) for dt in self.index.datasets.types.get_all()])

    # def get_dataset_types(self, **kwargs):
    #     ds_types = self.index.datasets.types.get_all()
    #     for ds_type in ds_types:
    #         if ds_type.metadata_type.dataset_fields:

    def get_dataset(self, variables=None, group_by=None, set_nan=False, include_lineage=False, **kwargs):
        # Split kwargs into dataset_type search fields and dimension search fields

            # Convert spatial dimension search fields into geobox

            # Convert kwargs to index search query

        # Search for datasets
        datasets = self.index.datasets.search(**kwargs)

        # Group by dataset type
        datasets_by_type = defaultdict(list)
        for dataset in datasets:
            datasets_by_type[dataset.type_.name].append(dataset)

        # Get output geobox from query

        # Or work out geobox from extents of requested datasets

        # Get dataset data

        return xarray.Dataset()

    def product_observations(self, type_name, geobox, group_func, index):
        geo_bb = geobox.geographic_extent.boundingbox
        # TODO: pull out full datasets lineage?
        datasets = self.index.datasets.search_eager(lat=Range(geo_bb.bottom, geo_bb.top),
                                                    lon=Range(geo_bb.left, geo_bb.right),
                                                    type=type_name)
        datasets = [dataset for dataset in datasets
                    if _check_intersect(geobox.extent, dataset_poly(dataset).to_crs(geobox.crs_str))]

        datasets.sort(key=group_func)
        groups = [(key, list(group)) for key, group in groupby(datasets, group_func)]

        return groups

    def product_data(self, groups, geobox, fuse_func=None):
        assert groups

        measurements = groups[0][1][0].type_.definition['measurements']

        shape = (len(groups), ) + geobox.shape
        dims = ('time',) + geobox.dimensions

        variables = {}

        for name, stuffs in measurements.items():
            data = numpy.empty(shape, dtype=stuffs['dtype'])
            for index, (key, sources) in enumerate(groups):
                fuse_sources([DatasetSource(dataset, name) for dataset in sources],
                             data[index],
                             geobox.affine,
                             geobox.crs_str,
                             stuffs.get('nodata'),
                             resampling=RESAMPLING.nearest,
                             fuse_func=fuse_func)
            variables[name] = (dims, data, {
                'nodata': stuffs.get('nodata'),
                'units': '1'
                })

        extra_md = numpy.empty(len(groups), dtype=object)
        for index, (key, sources) in enumerate(groups):
            extra_md[index] = sources
        variables['sources'] = (['time'], extra_md)

        coords = {'time': ('time', numpy.array([v[0] for v in groups]), {'units': 'seconds since 1970-01-01 00:00:00'})}
        coords.update({k:(k, v, {'units': '1'}) for k, v in geobox.coordinate_labels.items()})

        attrs = {
            'extent': geobox.extent,
            'affine': geobox.affine,
            'crs': geobox.crs
        }

        return xarray.Dataset(variables, coords=coords, attrs=attrs)


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


def dataset_poly(dataset):
    left, bottom, right, top = dataset.bounds
    return GeoPolygon([(left, bottom), (left, top), (right, top), (right, bottom)], crs_str=dataset.crs)

