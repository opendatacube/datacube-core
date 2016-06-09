from __future__ import absolute_import, division, print_function

import logging
from itertools import groupby
from collections import namedtuple, OrderedDict
from math import ceil

import pandas
import numpy
import xarray
from affine import Affine
from dask import array as da
from rasterio.coords import BoundingBox

from ..config import LocalConfig
from ..compat import string_types
from ..index import index_connect
from ..model import GeoPolygon, GeoBox, Range, CRS
from ..storage.storage import DatasetSource, fuse_sources, RESAMPLING
from ..utils import check_intersect, data_resolution_and_offset
from .query import Query

_LOG = logging.getLogger(__name__)


Group = namedtuple('Group', ['key', 'datasets'])


def _xarray_affine(obj):
    dims = obj.crs.dimensions
    xres, xoff = data_resolution_and_offset(obj[dims[1]].values)
    yres, yoff = data_resolution_and_offset(obj[dims[0]].values)
    return Affine.translation(xoff, yoff) * Affine.scale(xres, yres)


def _get_min_max(data):
    res, off = data_resolution_and_offset(data)
    left, right = numpy.asscalar(data[0]-0.5*res), numpy.asscalar(data[-1]+0.5*res)
    return (right, left) if res < 0 else (left, right)


def _xarray_extent(obj):
    dims = obj.crs.dimensions
    left, right = _get_min_max(obj[dims[1]].values)
    bottom, top = _get_min_max(obj[dims[0]].values)
    points = [[left, bottom], [left, top], [right, top], [right, bottom]]
    return GeoPolygon(points, obj.crs)


xarray.Dataset.affine = property(_xarray_affine)
xarray.Dataset.extent = property(_xarray_extent)


class Datacube(object):
    """
    Interface to search, read and write a datacube.
    """
    def __init__(self, index=None, config=None, app=None):
        """
        Creates the interface for the query and storage access.

        If no index is given, the default configuration is used for database connection, etc.

        :param index: The database index to use.
        :type index: from :py:class:`datacube.index.index_connect` or None
        :param config: A config object or a path to a config file that defines the connection.
            If an index is supplied, config is ignored.
        :type config: str or :class:`datacube.config.LocalConfig`
        :param app: A short, alphanumeric name to identify this application.

            The application name is used to track down problems with database queries, so it is strongly
            advised that be used.  If an index is supplied, application name is ignored.
        :type app: string, required if no index is given
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

    def list_products(self, show_archived=False, with_pandas=True):
        """
        List of products in the datacube.

        :param show_archived: include products that have been archived.
        :param with_pandas: return the list as a Pandas DataFrame, otherwise as a list of dict.
        :rtype: pandas.DataFrame or list(dict)
        """
        rows = [datatset_type_to_row(dataset_type) for dataset_type in self.index.datasets.types.get_all()]
        if not with_pandas:
            return rows

        keys = set(k for r in rows for k in r)
        main_cols = ['id', 'name', 'description']
        grid_cols = ['crs', 'resolution', 'tile_size', 'spatial_dimensions']
        other_cols = list(keys - set(main_cols) - set(grid_cols))
        cols = main_cols + other_cols + grid_cols
        return pandas.DataFrame(rows, columns=cols).set_index('id')

    def list_measurements(self, show_archived=False, with_pandas=True):
        """
        List of measurements for each product.

        :param show_archived: include products that have been archived.
        :param with_pandas: return the list as a Pandas DataFrame, otherwise as a list of dict.
        :rtype: pandas.DataFrame or list(dict)
        """
        measurements = self._list_measurements()
        if not with_pandas:
            return measurements
        return pandas.DataFrame.from_dict(measurements).set_index(['product', 'measurement'])

    def _list_measurements(self):
        measurements = []
        dts = self.index.datasets.types.get_all()
        for dt in dts:
            if dt.measurements:
                for name, measurement in dt.measurements.items():
                    row = {
                        'product': dt.name,
                        'measurement': name,
                    }
                    if 'attrs' in measurement:
                        row.update(measurement['attrs'])
                    row.update({k: v for k, v in measurement.items() if k != 'attrs'})
                    measurements.append(row)
        return measurements

    def load(self, product=None, measurements=None, output_crs=None, resolution=None, stack=False,
             **indexers):
        """
        Loads data as an ``xarray`` object.

        See http://xarray.pydata.org/en/stable/api.html for usage of the ``Dataset`` and ``DataArray`` objects.

        **Search fields**
            Search product fields. E.g.
            ::

                platform=['LANDSAT_5', 'LANDSAT_7'],
                product_type='nbar'

            See :meth:`list_products` for more information on the fields that can be searched.

        **Measurements**
            The ``measurements`` argument is a list of measurement names, as listed in :meth:`list_measurements`.

        **Dimensions**
            Spatial dimensions can specified using the ``longitude``/``latitude`` and ``x``/``y`` fields.
            The CRS of this query is assumed to be **WGS84/EPSG:4326** unless the ``crs`` field is supplied.

        **Output**
            If the `stack` argument is supplied, the returned data is stacked in a single ``DataArray``.
            A new dimension is created with the name supplied.
            This requires all of the data to be of the same datatype.

            To reproject or resample the data, supply the ``output_crs`` and ``resolution`` fileds.

        :param product: the product to be included.
                By default all available measurements are included.
        :type product: str
        :type measurements: list(str) or str, optional
        :param measurements: measurements name or list of names to be included, as listed in :meth:`list_measurements`.
                If a list is specified, the measurements will be returned in the order requested.
                By default all available measurements are included.
        :type measurements: list(str) or str, optional
        :param indexers: Search parameters and dimension ranges as described above. E.g.::

                product='NBAR', platform='LANDSAT_5', latitude=(-35.5, -34.5)

            The default CRS interpretation for geospatial dimensions is WGS84/EPSG:4326,
            even if the resulting dimension is in another projection.
            The dimensions ``longitude``/``latitude`` and ``x``/``y`` can be used interchangeably.
        :param output_crs: The CRS of the returned data.  If no CRS is supplied, the CRS of the stored data is used.
            E.g.::

                output_crs='EPSG:3577'

        :type output_crs: str
        :param resolution: A tuple of the spatial resolution of the returned data.
            E.g. 25m resolution for **EPSG:3577**::

                resolution=(-25, 25)

            This includes the direction (as indicated by a positive or negative number).
            Typically when using most CRSs, the first number would be negative.
        :type resolution: tuple(float, float)

        :return: Requested data.  As a ``DataArray`` if the ``stack`` variable is supplied.
        :rtype: :class:`xarray.Dataset` or :class:`xarray.DataArray`
        """
        if product is not None:
            indexers['product'] = product
        if measurements is not None:
            indexers['measurements'] = measurements
        if output_crs is not None:
            indexers['output_crs'] = output_crs
        if resolution is not None:
            indexers['resolution'] = resolution

        if stack:
            return self._get_data_array(var_dim_name=stack, **indexers)
        else:
            return self._get_dataset(**indexers)

    def _get_dataset(self, **indexers):
        query = Query.from_kwargs(self.index, **indexers)
        observations = self.product_observations(geopolygon=query.geopolygon, **query.search_terms)
        if not observations:
            return xarray.Dataset()

        crs = query.output_crs or get_crs(observations)
        geopolygon = query.geopolygon or get_bounds(observations, crs)
        resolution = query.resolution or get_resolution(observations)
        geobox = GeoBox.from_geopolygon(geopolygon, resolution, crs)

        group_by = query.group_by
        sources = self.product_sources(observations, group_by.group_by_func, group_by.dimension, group_by.units)

        all_measurements = get_measurements(observations)
        if query.measurements:
            measurements = OrderedDict((measurement, all_measurements[measurement])
                                       for measurement in query.measurements if measurement in all_measurements)
        else:
            measurements = all_measurements

        dataset = self.product_data(sources, geobox, measurements.values())
        return dataset

    def _get_data_array(self, var_dim_name='measurement', **indexers):
        query = Query.from_kwargs(self.index, **indexers)
        observations = self.product_observations(geopolygon=query.geopolygon, **query.search_terms)
        if not observations:
            return None

        crs = query.output_crs or get_crs(observations)
        geopolygon = query.geopolygon or get_bounds(observations, crs)
        resolution = query.resolution or get_resolution(observations)
        geobox = GeoBox.from_geopolygon(geopolygon, resolution, crs)

        group_by = query.group_by
        sources = self.product_sources(observations, group_by.group_by_func, group_by.dimension, group_by.units)

        all_measurements = get_measurements(observations)
        if query.measurements:
            measurements = OrderedDict((measurement, all_measurements[measurement])
                                       for measurement in query.measurements if measurement in all_measurements)
        else:
            measurements = all_measurements

        data_dict = OrderedDict()
        for name, measurement in measurements.items():
            data_dict[name] = self.measurement_data(sources, geobox, measurement)

        return _stack_vars(data_dict, var_dim_name)

    def product_observations(self, product=None, geopolygon=None, **kwargs):
        """
        Finds datasets for a product.

        Lower level function than most people will use.

        :param product: Name of the product
        :param geopolygon: Spatial area to search for datasets
        :type geopolygon: :class:`datacube.model.GeoPolygon`
        :param kwargs: mapping of additional search fields used.
        :return: list of datasets
        :rtype: list( :class:`datacube.model.Dataset` )

        .. seealso:: :meth:`product_sources` :meth:`product_data`
        """
        if geopolygon:
            geo_bb = geopolygon.to_crs(CRS('EPSG:4326')).boundingbox
            kwargs['lat'] = Range(geo_bb.bottom, geo_bb.top)
            kwargs['lon'] = Range(geo_bb.left, geo_bb.right)
        # TODO: pull out full datasets lineage?
        if product is not None:
            kwargs['product'] = product
        datasets = self.index.datasets.search_eager(**kwargs)
        if geopolygon:
            datasets = [dataset for dataset in datasets
                        if check_intersect(geopolygon, dataset.extent.to_crs(geopolygon.crs))]
            # Check against the bounding box of the original scene, can throw away some portions

        return datasets

    @staticmethod
    def product_sources(datasets, group_func, dimension, units):
        """
        Groups the datasets along defined non-spatial dimensions.

        :param datasets: a list of datasets, typically from :meth:`product_observations`
        :param group_func: a function that returns a label for a dataset
        :param dimension: name of the new dimension
        :param units: unit for the new dimension
        :return: :class:`xarray.Array`

        .. seealso:: :meth:`product_observations` :meth:`product_data`
        """
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
    def create_storage(coords, geobox, measurements, data_func=None):
        """
        Create empty xarry.Dataset to hold data from :meth:`product_sources`.

        :param sources: DataArray holding a list of :py:class:`datacube.model.Dataset` objects
        :type sources: :py:class:`xarray.DataArray`
        :param geobox: A GeoBox defining the output spatial projection and resolution
        :type geobox: :class:`datacube.model.GeoBox`
        :param measurements: list of measurement dicts with keys: {'name', 'dtype', 'nodata', 'units'}
        :param fill_func: function to fill the data
        :rtype: :py:class:`xarray.Dataset`

        .. seealso:: :meth:`product_observations` :meth:`product_sources`
        """
        def empty_func(measurement):
            coord_shape = tuple(coord.size for coord in coords.values())
            return numpy.full(coord_shape + geobox.shape, measurement['nodata'], dtype=measurement['dtype'])
        data_func = data_func or empty_func

        result = xarray.Dataset(attrs={'crs': geobox.crs})
        for name, coord in coords.items():
            result[name] = coord
        for name, coord in geobox.coordinates.items():
            result[name] = (name, coord.labels, {'units': coord.units})

        for measurement in measurements:
            data = data_func(measurement)

            attrs = {
                'nodata': measurement.get('nodata'),
                'units': measurement.get('units', '1')
            }
            if 'flags_definition' in measurement:
                attrs['flags_definition'] = measurement['flags_definition']
            if 'spectral_definition' in measurement:
                attrs['spectral_definition'] = measurement['spectral_definition']

            result[measurement['name']] = (coords.dims + geobox.dimensions, data, attrs)

        return result

    @staticmethod
    def product_data(sources, geobox, measurements, fuse_func=None):
        """
        Loads data from :meth:`product_sources` into a Dataset object.

        :param sources: DataArray holding a list of :py:class:`datacube.model.Dataset` objects
        :type sources: :py:class:`xarray.DataArray`
        :param geobox: A GeoBox defining the output spatial projection and resolution
        :type geobox: :class:`datacube.model.GeoBox`
        :param measurements: list of measurement dicts with keys: {'name', 'dtype', 'nodata', 'units'}
        :param fuse_func: function to merge successive arrays as an output
        :rtype: :py:class:`xarray.Dataset`

        .. seealso:: :meth:`product_observations` :meth:`product_sources`
        """
        def data_func(measurement):
            data = numpy.full(sources.shape + geobox.shape, measurement['nodata'], dtype=measurement['dtype'])
            for index, datasets in numpy.ndenumerate(sources.values):
                fuse_sources([DatasetSource(dataset, measurement['name']) for dataset in datasets],
                             data[index],  # Output goes here
                             geobox.affine,
                             geobox.crs,
                             measurement['nodata'],
                             resampling=RESAMPLING.nearest,
                             fuse_func=fuse_func)
            return data
        return Datacube.create_storage(sources.coords, geobox, measurements, data_func)

    @staticmethod
    def product_data_lazy(sources, geobox, measurements, fuse_func=None, grid_chunks=None):
        """
        Creates a lazy-loaded :py:class:`xarray.Dataset` for measurements.

        The data will be loaded from disk as needed, or when the `load` method is called.

        :param sources: DataArray holding a list of :py:class:`datacube.model.Dataset` objects
        :type sources: :py:class:`xarray.DataArray`
        :param geobox: A GeoBox defining the output spatial projection and resolution
        :type geobox: :class:`datacube.model.GeoBox`
        :param measurement: measurement definition with keys: {'name', 'dtype', 'nodata', 'units'}
        :param fuse_func: function to merge successive arrays as an output
        :rtype: :py:class:`xarray.DataArray`

        ..seealso:: :meth:`product_data`
        """
        def data_func(measurement):
            return _make_dask_array(sources, geobox, measurement, fuse_func, grid_chunks)
        return Datacube.create_storage(sources.coords, geobox, measurements, data_func)

    @staticmethod
    def measurement_data(sources, geobox, measurement, fuse_func=None):
        """
        Retrieves a single measurement variable as a :py:class:`xarray.DataArray`.

        :param sources: DataArray holding a list of :py:class:`datacube.model.Dataset` objects
        :type sources: :py:class:`xarray.DataArray`
        :param geobox: A GeoBox defining the output spatial projection and resolution
        :type geobox: :class:`datacube.model.GeoBox`
        :param measurement: measurement definition with keys: {'name', 'dtype', 'nodata', 'units'}
        :param fuse_func: function to merge successive arrays as an output
        :rtype: :py:class:`xarray.DataArray`

        ..seealso:: :meth:`product_data`
        """
        dataset = Datacube.product_data(sources, geobox, [measurement], fuse_func=fuse_func)
        dataarray = dataset[measurement['name']]
        dataarray.attrs['crs'] = dataset.crs
        return dataarray

    @staticmethod
    def measurement_data_lazy(sources, geobox, measurement, fuse_func=None, grid_chunks=None):
        """
        Creates a lazy-loaded :py:class:`xarray.DataArray` for a single measurement variable.

        The data will be loaded from disk as needed, or when the `load` method is called.

        :param sources: DataArray holding a list of :py:class:`datacube.model.Dataset` objects
        :type sources: :py:class:`xarray.DataArray`
        :param geobox: A GeoBox defining the output spatial projection and resolution
        :type geobox: :class:`datacube.model.GeoBox`
        :param measurements: list of measurement definitions with keys: {'name', 'dtype', 'nodata', 'units'}
        :param fuse_func: function to merge successive arrays as an output
        :rtype: :py:class:`xarray.DataArray`

        ..seealso:: :meth:`product_data`
        """
        dataset = Datacube.product_data_lazy(sources, geobox, [measurement], fuse_func, grid_chunks)
        dataarray = dataset[measurement['name']]
        dataarray.attrs['crs'] = dataset.crs
        return dataarray

    def grid_spec_for_product(self, product):
        """
        Gets the grid specification for a product.

         The GridSpec specifies the projection, resolution and tile size of the product.

        :param product: Name of the product. E.g. `'ls5_nbar_albers'`
        :return: :class:`datacube.model.GridSpec`

        .. seealso :class:`datacube.api.GridWorkflow`
        """
        dataset_type = self.index.datasets.types.get_by_name(product)
        return dataset_type.grid_spec if dataset_type else None

    def __str__(self):
        return "Datacube<index={!r}>".format(self.index)

    def __repr__(self):
        return self.__str__()


def fuse_lazy(datasets, geobox, measurement, fuse_func=None, prepend_dims=0):
    prepend_shape = (1,) * prepend_dims
    prepend_index = (0,) * prepend_dims
    data = numpy.full(prepend_shape + geobox.shape, measurement['nodata'], dtype=measurement['dtype'])
    fuse_sources([DatasetSource(dataset, measurement['name']) for dataset in datasets],
                 data[prepend_index],
                 geobox.affine,
                 geobox.crs,
                 measurement['nodata'],
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
    type_set = {d.type for d in datasets if d.type.grid_spec}
    if not type_set:
        raise ValueError('No valid CRS found.')
    first_type = type_set.pop()
    first_crs = first_type.grid_spec.crs
    if first_type and any(first_crs != another.grid_spec.crs for another in type_set):
        raise ValueError('Could not determine a unique output CRS from: ',
                         [first_crs] + [another.grid_spec.crs for another in type_set])
    return first_crs


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
                raise LookupError('Multiple values found for measurement: ', name)
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


def _stack_vars(data_dict, var_dim_name, stack_name=None):
    if len(data_dict) == 1:
        key, value = data_dict.popitem()
        return value
    labels = list(data_dict.keys())
    stack = xarray.concat(
        [data_dict[var_name] for var_name in labels],
        dim=xarray.DataArray(labels, name=var_dim_name, dims=var_dim_name),
        coords='minimal')
    if stack_name:
        stack.name = stack_name
    return stack
