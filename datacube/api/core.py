from __future__ import absolute_import, division, print_function

import logging
from itertools import groupby, chain
from collections import defaultdict, namedtuple
from math import ceil

import pandas
import numpy
import xarray
from dask import array as da
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

    @property
    def datasets(self):
        """
        List of products as a Pandas DataTable
        :return:
        """
        return pandas.DataFrame([datatset_type_to_row(dataset_type)
                                 for dataset_type in self.index.datasets.types.get_all()])

    @property
    def variables(self):
        return pandas.DataFrame.from_dict(self.list_variables()).set_index(['dataset', 'variable'])

    def list_variables(self):
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
                    row.update({k: v for k, v in measurement.items() if k != 'attrs'})
                    variables.append(row)
        return variables

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
    def variable_data(sources, geobox, measurement, name=None, fuse_func=None):
        print('!variable_data:')
        print('!sources=', sources)
        print('!geobox=', geobox)
        print('!measurement=', measurement)
        print('!name=', name)
        name = measurement.get('name', name)
        coords = {dim: coord for dim, coord in sources.coords.items()}
        for dim, coord in geobox.coordinates.items():
            coords[dim] = xarray.Coordinate(dim, coord.labels, attrs={'units': coord.units})
        dims = sources.dims + geobox.dimensions

        data = numpy.empty(sources.shape + geobox.shape, dtype=measurement['dtype'])
        for index, datasets in numpy.ndenumerate(sources.values):
            fuse_sources([DatasetSource(dataset, name) for dataset in datasets],
                         data[index],
                         geobox.affine,
                         geobox.crs,
                         measurement.get('nodata'),
                         resampling=RESAMPLING.nearest,
                         fuse_func=fuse_func)

        result = xarray.DataArray(data,
                                  coords=coords,
                                  dims=dims,
                                  name=name,
                                  attrs={
                                      'extent': geobox.extent,
                                      'affine': geobox.affine,
                                      'crs': geobox.crs,
                                      'nodata': measurement.get('nodata'),
                                      'units': measurement.get('units', '1')
                                  })

        # TODO: Include source metadata
        # extra_md = numpy.empty(len(groups), dtype=object)
        # for index, (_, sources) in enumerate(groups):
        #     extra_md[index] = sources
        # result['sources'] = (['time'], extra_md)

        return result

    # @staticmethod
    # def variable_data_lazy(sources, geobox, measurement, name=None, fuse_func=None, grid_chunks=None):
    #     coords = {dim: coord for dim, coord in sources.coords.items()}
    #     for dim, coord in geobox.coordinates.items():
    #         coords[dim] = xarray.Coordinate(dim, coord.labels, attrs={'units': coord.units})
    #     dims = sources.dims + geobox.dimensions
    #     name = measurement.get('name', name)
    #     dsk_name = 'datacube_' + name
    #     irr_chunks = (1,) * sources.ndim
    #     grid_chunks = grid_chunks or (1000, 1000)
    #     chunks = irr_chunks + grid_chunks
    #     shape = sources.shape + geobox.shape
    #     dtype = measurement['dtype']
    #     dsk = {}
    #
    #     geobox_subsets = {}
    #     num_grid_chunks = [int(ceil(s/float(c))) for s, c in zip(geobox.shape, grid_chunks)]
    #     for grid_index in numpy.ndindex(*num_grid_chunks):
    #         slices = [slice(min(d*c, stop), min((d+1)*c, stop))
    #                   for d, c, stop in zip(grid_index, grid_chunks, geobox.shape)]
    #         geobox_subsets[grid_index] = geobox[slices]
    #
    #     for irr_index, datasets in numpy.ndenumerate(sources.values):
    #         for grid_index, subset_geobox in geobox_subsets.items():
    #             index = (dsk_name,) + irr_index + grid_index
    #             dsk[index] = (fuse_lazy, datasets, subset_geobox, measurement, name, fuse_func, sources.ndim)
    #
    #     data = da.Array(dsk, dsk_name, chunks=chunks, dtype=dtype, shape=shape)
    #     result = xarray.DataArray(data,
    #                               coords=coords,
    #                               dims=dims,
    #                               name=name,
    #                               attrs={
    #                                   'extent': geobox.extent,
    #                                   'affine': geobox.affine,
    #                                   'crs': geobox.crs,
    #                                   'nodata': measurement.get('nodata'),
    #                                   'units': measurement.get('units', '1')
    #                               })
    #     return result

    def __repr__(self):
        return "Datacube<index={!r}>".format(self.index)


def fuse_lazy(datasets, geobox, measurement, name, fuse_func=None, prepend_dims=0):
    name = measurement.get('name', name)
    prepend_shape = (1,) * prepend_dims
    prepend_index = (0,) * prepend_dims
    data = numpy.empty(prepend_shape + geobox.shape, dtype=measurement['dtype'])
    fuse_sources([DatasetSource(dataset, name) for dataset in datasets],
                 data[prepend_index],
                 geobox.affine,
                 geobox.crs,
                 measurement.get('nodata'),
                 resampling=RESAMPLING.nearest,
                 fuse_func=fuse_func)
    return data


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


def get_bounds(datasets, dataset_type):
    left = min([d.bounds.left for d in datasets])
    right = max([d.bounds.right for d in datasets])
    top = max([d.bounds.top for d in datasets])
    bottom = min([d.bounds.bottom for d in datasets])
    return GeoPolygon.from_boundingbox(BoundingBox(left, bottom, right, top), dataset_type.grid_spec.crs)


def datatset_type_to_row(dt):
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
