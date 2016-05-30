from __future__ import absolute_import, division, print_function

import logging
from itertools import groupby
from collections import namedtuple, OrderedDict
from math import ceil

import pandas
import numpy
import xarray
from dask import array as da
from rasterio.coords import BoundingBox

from ..config import LocalConfig
from ..compat import string_types
from ..index import index_connect
from ..model import GeoPolygon, GeoBox, Range, CRS
from ..model import _DocReader as DocReader
from ..storage.storage import DatasetSource, fuse_sources, RESAMPLING
from ..utils import check_intersect
from .query import Query

_LOG = logging.getLogger(__name__)


Group = namedtuple('Group', ['key', 'datasets'])
Grouper = namedtuple('Grouper', ['dimension', 'group_by', 'units'])


class Datacube(object):
    """
    Interface to search, read and write a datacube

    Functions in current API:

    AA/EE functions
    ===============
    get_descriptor              -> Remain in API.get_descriptor
    get_data                    -> Remain in API.det_data

    List search fields
    ==================
    list_fields                 -> Use Datacube.datasets
    list_field_values           -> Use Datacube.datasets
    list_all_field_values       -> Use Datacube.datasets

    List collections
    ================
    list_storage_units          -> *REMOVED*
    list_storage_type_names     -> Use Datacube.datasets
    list_products               -> Use Datacube.datasets
    list_variables              -> Use Datacube.variables

    Data Access
    ===========
    get_dataset
    get_data_array

    Legacy tile-based workflow
    ==========================
    list_cells                  -> Get dt, Get geobox for cell
    list_tiles                  -> Get dt
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
                if isinstance(config, string_types):
                    config = LocalConfig.find([config])
                self.index = index_connect(config, application_name=app)
            else:
                self.index = index_connect(application_name=app)
        else:
            self.index = index

    @property
    def products(self):
        """
        List of products as a Pandas DataFrame.
        :rtype: pandas.DataFrame
        """
        return pandas.DataFrame([datatset_type_to_row(dataset_type)
                                 for dataset_type in self.index.datasets.types.get_all()],
                                index='id')

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

    def get_dataset(self, **indexers):
        query = Query.from_kwargs(self.index, **indexers)
        type_name = query.type  # TODO: Can type_name be optional?
        query.type = None
        observations = self.product_observations(type_name=type_name,
                                                 geopolygon=query.geopolygon,
                                                 **query.search_terms)
        geopolygon = query.geopolygon
        if not geopolygon:
            crs = query.output_crs or get_crs(observations)
            geopolygon = get_bounds(observations, crs)

        resolution = query.resolution or get_resolution(observations)
        geobox = GeoBox.from_geopolygon(geopolygon, resolution)

        #TDOD: Make Grouper in Query
        grouper = Grouper(dimension='time',
                          group_by=lambda ds: ds.center_time,
                          units='seconds since 1970-01-01 00:00:00')
        sources = self.product_sources(observations, grouper.group_by, grouper.dimension, grouper.units)

        all_measurements = get_measurements(observations)
        if query.variables:
            measurements = OrderedDict((variable, all_measurements[variable]) for variable in query.variables
                                       if variable in all_measurements)
        else:
            measurements = all_measurements

        dataset = self.product_data(sources, geobox, measurements.values())
        return dataset

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
                        if check_intersect(geopolygon, dataset.extent.to_crs(geopolygon.crs))]
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
        """
        :type sources: xarray.DataArray
        :type geobox: datacube.model.GeoBox
        :type measurements: list
        :rtype: xarray.Dataset
        """
        result = xarray.Dataset(attrs={'extent': geobox.extent, 'crs': geobox.crs})
        for name, coord in sources.coords.items():
            result[name] = coord
        for name, coord in geobox.coordinates.items():
            result[name] = (name, coord.labels, {'units': coord.units})

        for measurement in measurements:
            name = measurement['name']
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
    def variable_data(sources, geobox, measurement, fuse_func=None):
        coords = {dim: coord for dim, coord in sources.coords.items()}
        for dim, coord in geobox.coordinates.items():
            coords[dim] = xarray.Coordinate(dim, coord.labels, attrs={'units': coord.units})
        dims = sources.dims + geobox.dimensions

        data = numpy.empty(sources.shape + geobox.shape, dtype=measurement['dtype'])
        for index, datasets in numpy.ndenumerate(sources.values):
            fuse_sources([DatasetSource(dataset, measurement['name']) for dataset in datasets],
                         data[index],
                         geobox.affine,
                         geobox.crs,
                         measurement.get('nodata'),
                         resampling=RESAMPLING.nearest,
                         fuse_func=fuse_func)

        result = xarray.DataArray(data,
                                  coords=coords,
                                  dims=dims,
                                  name=measurement['name'],
                                  attrs={
                                      'extent': geobox.extent,
                                      'affine': geobox.affine,
                                      'crs': geobox.crs,
                                      'nodata': measurement.get('nodata'),
                                      'units': measurement.get('units', '1')
                                  })
        return result

    @staticmethod
    def variable_data_lazy(sources, geobox, measurement, fuse_func=None, grid_chunks=None):
        coords = {dim: coord for dim, coord in sources.coords.items()}
        for dim, coord in geobox.coordinates.items():
            coords[dim] = xarray.Coordinate(dim, coord.labels, attrs={'units': coord.units})
        dims = sources.dims + geobox.dimensions

        data = _make_dask_array(sources, geobox, measurement, fuse_func, grid_chunks)

        result = xarray.DataArray(data,
                                  coords=coords,
                                  dims=dims,
                                  name=measurement['name'],
                                  attrs={
                                      'extent': geobox.extent,
                                      'affine': geobox.affine,
                                      'crs': geobox.crs,
                                      'nodata': measurement.get('nodata'),
                                      'units': measurement.get('units', '1')
                                  })
        return result

    def __repr__(self):
        return "Datacube<index={!r}>".format(self.index)


def fuse_lazy(datasets, geobox, measurement, fuse_func=None, prepend_dims=0):
    name = measurement['name']
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


def get_crs(datasets):
    """
    Returns a single CRS from a collection of datasets
    Raises an error if no or multiple CRSs are found
    :param datasets: [`model.Dataset`]
    :return: `model.CRS`
    """
    crs_set = {d.type.grid_spec.crs for d in datasets if d.type.grid_spec}
    if not crs_set:
        raise ValueError('No valid CRS found.')
    first = crs_set.pop()
    if crs_set and any(first != another for another in crs_set):
        raise ValueError('Could not determine a unique output CRS from: ', [first] + list(crs_set))
    return first


def get_resolution(datasets):
    resolution_set = {tuple(d.type.grid_spec.resolution) for d in datasets if d.type.grid_spec}
    if len(resolution_set) != 1:
        raise ValueError('Could not determine an output resolution')
    return resolution_set.pop()


def get_bounds(datasets, crs):
    left = min([d.extent.to_crs(crs).boundingbox.left for d in datasets])
    right = max([d.extent.to_crs(crs).boundingbox.right for d in datasets])
    top = max([d.extent.to_crs(crs).boundingbox.top for d in datasets])
    bottom = min([d.extent.to_crs(crs).boundingbox.bottom for d in datasets])
    return GeoPolygon.from_boundingbox(BoundingBox(left, bottom, right, top), crs)


def get_measurements(datasets):
    dataset_types = {d.type for d in datasets}
    all_measurements = OrderedDict()
    for dataset_type in dataset_types:
        for name, measurement in dataset_type.measurements.items():
            if name in all_measurements:
                raise LookupError('Multiple values found for variable: ', name)
            all_measurements[name] = measurement
    return all_measurements


def datatset_type_to_row(dt):
    row = {
        'id': dt.id,
        'name': dt.name,
        'description': dt.definition['description'],
    }
    row.update(dt.fields)
    if dt.grid_spec is not None:
        row.update({
            'crs': dt.grid_spec.crs,
            'spatial_dimensions': dt.grid_spec.dimensions,
            'tile_size': dt.grid_spec.tile_size,
            'resolution': dt.grid_spec.resolution,
        })
    return row


def _chunk_geobox(geobox, chunk_size):
    num_grid_chunks = [int(ceil(s/float(c))) for s, c in zip(geobox.shape, chunk_size)]
    geobox_subsets = {}
    for grid_index in numpy.ndindex(*num_grid_chunks):
        slices = [slice(min(d*c, stop), min((d+1)*c, stop))
                  for d, c, stop in zip(grid_index, chunk_size, geobox.shape)]
        geobox_subsets[grid_index] = geobox[slices]
    return geobox_subsets


def _make_dask_array(sources, geobox, measurement, fuse_func=None, grid_chunks=None):
    dsk_name = 'datacube_' + measurement['name']
    irr_chunks = (1,) * sources.ndim
    grid_chunks = grid_chunks or (1000, 1000)
    dsk = {}
    geobox_subsets = _chunk_geobox(geobox, grid_chunks)

    for irr_index, datasets in numpy.ndenumerate(sources.values):
        for grid_index, subset_geobox in geobox_subsets.items():
            index = (dsk_name,) + irr_index + grid_index
            dsk[index] = (fuse_lazy, datasets, subset_geobox, measurement, fuse_func, sources.ndim)

    data = da.Array(dsk, dsk_name,
                    chunks=(irr_chunks + grid_chunks),
                    dtype=measurement['dtype'],
                    shape=(sources.shape + geobox.shape))
    return data
