from __future__ import absolute_import, division, print_function

import logging
import os
import sys
import uuid
import warnings
from collections import namedtuple, OrderedDict
from itertools import groupby, repeat
from math import ceil

import numpy
import pandas
import xarray

try:
    import SharedArray as sa
except ImportError:
    pass
from affine import Affine
from dask import array as da

try:
    from pathos.threading import ThreadPool
except ImportError:
    pass
from six.moves import zip

from datacube.config import LocalConfig
from datacube.compat import string_types
from datacube.storage.storage import reproject_and_fuse
from datacube.utils import geometry, intersects, data_resolution_and_offset
from .query import Query, query_group_by, query_geopolygon
from ..index import index_connect
from ..drivers import new_datasource

_LOG = logging.getLogger(__name__)
THREADING_REQS_AVAILABLE = ('SharedArray' in sys.modules and 'pathos.threading' in sys.modules)

Group = namedtuple('Group', ['key', 'datasets'])


class Datacube(object):
    """
    Interface to search, read and write a datacube.

    :type index: datacube.index.index.Index
    """

    def __init__(self,
                 index=None,
                 config=None,
                 app=None,
                 env=None,
                 validate_connection=True):
        """
        Create the interface for the query and storage access.

        If no index or config is given, the default configuration is used for database connection.

        :param Index index: The database index to use.
        :type index: :py:class:`datacube.index.Index` or None.

        :param Union[LocalConfig|str] config: A config object or a path to a config file that defines the connection.

            If an index is supplied, config is ignored.
        :param str app: A short, alphanumeric name to identify this application.

            The application name is used to track down problems with database queries, so it is strongly
            advised that be used.  Required if an index is not supplied, otherwise ignored.

        :param str env: Name of the datacube environment to use.
            ie. the section name in any config files. Defaults to 'datacube' for backwards
            compatibility with old config files.

            Allows you to have multiple datacube instances in one configuration, specified on load,
            eg. 'dev', 'test' or 'landsat', 'modis' etc.

        :param bool validate_connection: Should we check that the database connection is available and valid

        :return: Datacube object

        """
        def normalise_config(config):
            if config is None:
                return LocalConfig.find(env=env)
            if isinstance(config, string_types):
                return LocalConfig.find([config], env=env)
            return config

        if index is None:
            index = index_connect(normalise_config(config),
                                  application_name=app,
                                  validate_connection=validate_connection)

        self.index = index

    def list_products(self, show_archived=False, with_pandas=True):
        """
        List products in the datacube

        :param show_archived: include products that have been archived.
        :param with_pandas: return the list as a Pandas DataFrame, otherwise as a list of dict.
        :rtype: pandas.DataFrame or list(dict)
        """
        rows = [dataset_type_to_row(dataset_type) for dataset_type in self.index.products.get_all()]
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
        List measurements for each product

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
        dts = self.index.products.get_all()
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

    #: pylint: disable=too-many-arguments, too-many-locals
    def load(self, product=None, measurements=None, output_crs=None, resolution=None, resampling=None,
             dask_chunks=None, like=None, fuse_func=None, align=None, datasets=None, use_threads=False, **query):
        """
        Load data as an ``xarray`` object.  Each measurement will be a data variable in the :class:`xarray.Dataset`.

        See the `xarray documentation <http://xarray.pydata.org/en/stable/data-structures.html>`_ for usage of the
        :class:`xarray.Dataset` and :class:`xarray.DataArray` objects.

        **Product and Measurements**
            A product can be specified using the product name, or by search fields that uniquely describe a single
            product.
            ::

                product='ls5_ndvi_albers'

            See :meth:`list_products` for the list of products with their names and properties.

            A product can also be selected by searching using fields, but must only match one product.
            For example::

                platform='LANDSAT_5',
                product_type='ndvi'

            The ``measurements`` argument is a list of measurement names, as listed in :meth:`list_measurements`.
            If not provided, all measurements for the product will be returned.
            ::

                measurements=['red', 'nir', 'swir2']

        **Dimensions**
            Spatial dimensions can specified using the ``longitude``/``latitude`` and ``x``/``y`` fields.

            The CRS of this query is assumed to be WGS84/EPSG:4326 unless the ``crs`` field is supplied,
            even if the stored data is in another projection or the `output_crs` is specified.
            The dimensions ``longitude``/``latitude`` and ``x``/``y`` can be used interchangeably.
            ::

                latitude=(-34.5, -35.2), longitude=(148.3, 148.7)

            or ::

                x=(1516200, 1541300), y=(-3867375, -3867350), crs='EPSG:3577'

            The ``time`` dimension can be specified using a tuple of datetime objects or strings with
            `YYYY-MM-DD hh:mm:ss` format. E.g::

                time=('2001-04', '2001-07')

            For EO-specific datasets that are based around scenes, the time dimension can be reduced to the day level,
            using solar day to keep scenes together.
            ::

                group_by='solar_day'

            For data that has different values for the scene overlap the requires more complex rules for combining data,
            such as GA's Pixel Quality dataset, a function can be provided to the merging into a single time slice.

            See :func:`datacube.helpers.ga_pq_fuser` for an example implementation.


        **Output**
            To reproject or resample the data, supply the ``output_crs``, ``resolution``, ``resampling`` and ``align``
            fields.

            To reproject data to 25m resolution for EPSG:3577::

                dc.load(product='ls5_nbar_albers', x=(148.15, 148.2), y=(-35.15, -35.2), time=('1990', '1991'),
                        output_crs='EPSG:3577`, resolution=(-25, 25), resampling='cubic')

        :param str product: the product to be included.

        :param measurements:
            Measurements name or list of names to be included, as listed in :meth:`list_measurements`.

            If a list is specified, the measurements will be returned in the order requested.
            By default all available measurements are included.

        :type measurements: list(str), optional

        :param query:
            Search parameters for products and dimension ranges as described above.

        :param str output_crs:
            The CRS of the returned data.  If no CRS is supplied, the CRS of the stored data is used.

        :param (float,float) resolution:
            A tuple of the spatial resolution of the returned data.
            This includes the direction (as indicated by a positive or negative number).

            Typically when using most CRSs, the first number would be negative.

        :param str resampling:
            The resampling method to use if re-projection is required.

            Valid values are: ``'nearest', 'cubic', 'bilinear', 'cubic_spline', 'lanczos', 'average'``

            Defaults to ``'nearest'``.

        :param (float,float) align:
            Load data such that point 'align' lies on the pixel boundary.
            Units are in the co-ordinate space of the output CRS.

            Default is (0,0)

        :param dict dask_chunks:
            If the data should be lazily loaded using :class:`dask.array.Array`,
            specify the chunking size in each output dimension.

            See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
            for more information.

        :param xarray.Dataset like:
            Uses the output of a previous ``load()`` to form the basis of a request for another product.
            E.g.::

                pq = dc.load(product='ls5_pq_albers', like=nbar_dataset)

        :param str group_by:
            When specified, perform basic combining/reducing of the data.

        :param fuse_func:
            Function used to fuse/combine/reduce data with the ``group_by`` parameter. By default,
            data is simply copied over the top of each other, in a relatively undefined manner. This function can
            perform a specific combining step, eg. for combining GA PQ data.

        :param datasets:
            Optional. If this is a non-empty list of :class:`datacube.model.Dataset` objects, these will be loaded
            instead of performing a database lookup.

        :param bool use_threads:
            Optional. If this is set to True, IO will be multi-thread.
            May not work for all drivers due to locking/GIL.

            Default is False.

        :param int limit:
            Optional. If provided, limit the maximum number of datasets
            returned. Useful for testing and debugging.

        :return: Requested data in a :class:`xarray.Dataset`
        :rtype: :class:`xarray.Dataset`
        """
        if 'stack' in query:
            raise DeprecationWarning("the `stack` keyword argument is not supported anymore, "
                                     "please apply `xarray.Dataset.to_array()` to the result instead")

        observations = datasets or self.find_datasets(product=product, like=like, **query)
        if not observations:
            return xarray.Dataset()

        geobox = output_geobox(like=like, output_crs=output_crs, resolution=resolution, align=align,
                               grid_spec=self.index.products.get_by_name(product).grid_spec,
                               datasets=observations, **query)

        group_by = query_group_by(**query)
        grouped = self.group_datasets(observations, group_by)

        datacube_product = self.index.products.get_by_name(product)
        measurement_dicts = set_resampling_method(datacube_product.lookup_measurements(measurements),
                                                  resampling)

        result = self.load_data(grouped, geobox, list(measurement_dicts.values()),
                                fuse_func=fuse_func,
                                dask_chunks=dask_chunks,
                                use_threads=use_threads)

        return apply_aliases(result, datacube_product, measurements)

    def product_observations(self, **kwargs):
        warnings.warn("product_observations() has been renamed to find_datasets() and will eventually be removed",
                      DeprecationWarning)
        return self.find_datasets(**kwargs)

    def find_datasets(self, **search_terms):
        """
        Search the index and return all datasets for a product matching the search terms.

        :param search_terms: see :class:`datacube.api.query.Query`
        :return: list of datasets
        :rtype: list[:class:`datacube.model.Dataset`]

        .. seealso:: :meth:`group_datasets` :meth:`load_data` :meth:`find_datasets_lazy`
        """
        return list(self.find_datasets_lazy(**search_terms))

    def find_datasets_lazy(self, limit=None, **kwargs):
        """
        Find datasets matching query.

        :param kwargs: see :class:`datacube.api.query.Query`
        :param limit: if provided, limit the maximum number of datasets returned
        :return: iterator of datasets
        :rtype: __generator[:class:`datacube.model.Dataset`]

        .. seealso:: :meth:`group_datasets` :meth:`load_data` :meth:`find_datasets`
        """
        query = Query(self.index, **kwargs)
        if not query.product:
            raise ValueError("must specify a product")

        datasets = self.index.datasets.search(limit=limit,
                                              **query.search_terms)

        for dataset in select_datasets_inside_polygon(datasets, query.geopolygon):
            yield dataset

    @staticmethod
    def product_sources(datasets, group_by):
        warnings.warn("product_sources() has been renamed to group_datasets() and will eventually be removed",
                      DeprecationWarning)
        return Datacube.group_datasets(datasets, group_by)

    @staticmethod
    def group_datasets(datasets, group_by):
        """
        Group datasets along defined non-spatial dimensions (ie. time).

        :param datasets: a list of datasets, typically from :meth:`find_datasets`
        :param GroupBy group_by: Contains:
            - a function that returns a label for a dataset
            - name of the new dimension
            - unit for the new dimension
            - function to sort by before grouping
        :rtype: xarray.DataArray

        .. seealso:: :meth:`find_datasets`, :meth:`load_data`, :meth:`query_group_by`
        """
        dimension, group_func, units, sort_key = group_by
        datasets.sort(key=sort_key)
        groups = [Group(key, tuple(group)) for key, group in groupby(datasets, group_func)]

        data = numpy.empty(len(groups), dtype=object)
        for index, group in enumerate(groups):
            data[index] = group.datasets
        coords = [sort_key(v.datasets[0]) for v in groups]
        sources = xarray.DataArray(data, dims=[dimension], coords=[coords])
        sources[dimension].attrs['units'] = units
        return sources

    @staticmethod
    def create_storage(coords, geobox, measurements, data_func=None, use_threads=False):
        """
        Create a :class:`xarray.Dataset` and (optionally) fill it with data.

        This function makes the in memory storage structure to hold datacube data, loading data from datasets that have
         been grouped appropriately by :meth:`group_datasets`.

        :param dict coords:
            OrderedDict holding `DataArray` objects defining the dimensions not specified by `geobox`

        :param GeoBox geobox:
            A GeoBox defining the output spatial projection and resolution

        :param measurements:
            list of measurement dicts with keys: {'name', 'dtype', 'nodata', 'units'}

        :param data_func:
            function to fill the storage with data. It is called once for each measurement, with the measurement
            as an argument. It should return an appropriately shaped numpy array. If not provided, an empty
            :class:`xarray.Dataset` is returned.

        :param bool use_threads:
            Optional. If this is set to True, IO will be multi-thread.
            May not work for all drivers due to locking/GIL.

            Default is False.

        :rtype: :class:`xarray.Dataset`

        .. seealso:: :meth:`find_datasets` :meth:`group_datasets`
        """

        def empty_func(measurement_):
            coord_shape = tuple(coord_.size for coord_ in coords.values())
            return numpy.full(coord_shape + geobox.shape, measurement_['nodata'], dtype=measurement_['dtype'])

        data_func = data_func or empty_func

        result = xarray.Dataset(attrs={'crs': geobox.crs})
        for name, coord in coords.items():
            result[name] = coord
        for name, coord in geobox.coordinates.items():
            result[name] = (name, coord.values, {'units': coord.units})

        def work_measurements(measurement, data_func):
            return data_func(measurement)

        use_threads = use_threads and THREADING_REQS_AVAILABLE

        if use_threads:
            pool = ThreadPool(32)
            results = pool.map(work_measurements, measurements, repeat(data_func))
        else:
            results = [data_func(a) for a in measurements]

        for measurement in measurements:
            data = results.pop(0)
            attrs = measurement.dataarray_attrs()
            attrs['crs'] = geobox.crs
            dims = tuple(coords.keys()) + tuple(geobox.dimensions)
            result[measurement['name']] = (dims, data, attrs)

        return result

    @staticmethod
    def product_data(*args, **kwargs):
        warnings.warn("product_data() has been renamed to load_data() and will eventually be removed",
                      DeprecationWarning)
        return Datacube.load_data(*args, **kwargs)

    @staticmethod
    def load_data(sources, geobox, measurements, fuse_func=None, dask_chunks=None, skip_broken_datasets=False,
                  use_threads=False):
        """
        Load data from :meth:`group_datasets` into an :class:`xarray.Dataset`.

        :param xarray.DataArray sources:
            DataArray holding a list of :class:`datacube.model.Dataset`, grouped along the time dimension

        :param GeoBox geobox:
            A GeoBox defining the output spatial projection and resolution

        :param measurements:
            list of measurement dicts with keys: {'name', 'dtype', 'nodata', 'units'}

        :param fuse_func:
            function to merge successive arrays as an output

        :param dict dask_chunks:
            If provided, the data will be loaded on demand using using :class:`dask.array.Array`.
            Should be a dictionary specifying the chunking size for each output dimension.

            See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
            for more information.

        :param bool use_threads:
            Optional. If this is set to True, IO will be multi-thread.
            May not work for all drivers due to locking/GIL.

            Default is False.

        :rtype: xarray.Dataset

        .. seealso:: :meth:`find_datasets` :meth:`group_datasets`
        """
        if use_threads and ('SharedArray' not in sys.modules or 'pathos.threading' not in sys.modules):
            use_threads = False

        if dask_chunks is None:
            def data_func(measurement):
                if not use_threads:
                    data = numpy.full(sources.shape + geobox.shape, measurement['nodata'], dtype=measurement['dtype'])
                    for index, datasets in numpy.ndenumerate(sources.values):
                        _fuse_measurement(data[index], datasets, geobox, measurement, fuse_func=fuse_func,
                                          skip_broken_datasets=skip_broken_datasets)
                else:
                    def work_load_data(array_name, index, datasets):
                        data = sa.attach(array_name)
                        _fuse_measurement(data[index], datasets, geobox, measurement, fuse_func=fuse_func,
                                          skip_broken_datasets=skip_broken_datasets)

                    array_name = '_'.join(['DCCORE', str(uuid.uuid4()), str(os.getpid())])
                    sa.create(array_name, shape=sources.shape + geobox.shape, dtype=measurement['dtype'])
                    data = sa.attach(array_name)
                    data[:] = measurement['nodata']

                    pool = ThreadPool(32)
                    pool.map(work_load_data, repeat(array_name), *zip(*numpy.ndenumerate(sources.values)))
                    sa.delete(array_name)
                return data
        else:
            def data_func(measurement):
                return _make_dask_array(sources, geobox, measurement,
                                        skip_broken_datasets=skip_broken_datasets,
                                        fuse_func=fuse_func,
                                        dask_chunks=dask_chunks)

        return Datacube.create_storage(OrderedDict((dim, sources.coords[dim]) for dim in sources.dims),
                                       geobox, measurements, data_func, use_threads)

    @staticmethod
    def measurement_data(sources, geobox, measurement, fuse_func=None, dask_chunks=None):
        """
        Retrieve a single measurement variable as a :class:`xarray.DataArray`.

        .. note:

             This method appears to only be used by the deprecated `get_data()/get_descriptor()`
              :class:`~datacube.api.API`, so is a prime candidate for future removal.

        .. seealso:: :meth:`load_data`


        :param xarray.DataArray sources: DataArray holding a list of :class:`datacube.model.Dataset` objects
        :param GeoBox geobox: A GeoBox defining the output spatial projection and resolution
        :param measurement: measurement definition with keys: {'name', 'dtype', 'nodata', 'units'}
        :param fuse_func: function to merge successive arrays as an output
        :param dict dask_chunks: If the data should be loaded as needed using :class:`dask.array.Array`,
            specify the chunk size in each output direction.
            See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
            for more information.
        :rtype: :class:`xarray.DataArray`
        """
        dataset = Datacube.load_data(sources, geobox, [measurement], fuse_func=fuse_func, dask_chunks=dask_chunks)
        dataarray = dataset[measurement['name']]
        dataarray.attrs['crs'] = dataset.crs
        return dataarray

    def __str__(self):
        return "Datacube<index={!r}>".format(self.index)

    def __repr__(self):
        return self.__str__()

    def close(self):
        """
        Close any open connections
        """
        self.index.close()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()


def apply_aliases(data, product, measurements):
    """
    If measurements are referred to by their aliases,
    rename data arrays to reflect that.
    """
    if measurements is None:
        return data

    return data.rename({product.canonical_measurement(provided_name): provided_name
                        for provided_name in measurements})

def output_geobox(like=None, output_crs=None, resolution=None, align=None,
                  grid_spec=None, datasets=None, **query):
    """ Configure output geobox from user provided output specs. """

    if like is not None:
        assert output_crs is None, "'like' and 'output_crs' are not supported together"
        assert resolution is None, "'like' and 'resolution' are not supported together"
        assert align is None, "'like' and 'align' are not supported together"
        return like.geobox

    if output_crs is not None:
        # user provided specifications
        if resolution is None:
            raise ValueError("Must specify 'resolution' when specifying 'output_crs'")
        crs = geometry.CRS(output_crs)
    else:
        # specification from grid_spec
        if grid_spec is None or grid_spec.crs is None:
            raise ValueError("Product has no default CRS. Must specify 'output_crs' and 'resolution'")
        crs = grid_spec.crs
        if resolution is None:
            if grid_spec.resolution is None:
                raise ValueError("Product has no default resolution. Must specify 'resolution'")
            resolution = grid_spec.resolution
        align = align or grid_spec.alignment

    return geometry.GeoBox.from_geopolygon(query_geopolygon(**query) or get_bounds(datasets, crs),
                                           resolution, crs, align)


def select_datasets_inside_polygon(datasets, polygon):
    # Check against the bounding box of the original scene, can throw away some portions
    for dataset in datasets:
        if polygon is None or intersects(polygon.to_crs(dataset.crs), dataset.extent):
            yield dataset


def fuse_lazy(datasets, geobox, measurement, skip_broken_datasets=False, fuse_func=None, prepend_dims=0):
    prepend_shape = (1,) * prepend_dims
    data = numpy.full(geobox.shape, measurement['nodata'], dtype=measurement['dtype'])
    _fuse_measurement(data, datasets, geobox, measurement,
                      skip_broken_datasets=skip_broken_datasets,
                      fuse_func=fuse_func)
    return data.reshape(prepend_shape + geobox.shape)


def _fuse_measurement(dest, datasets, geobox, measurement,
                      skip_broken_datasets=False,
                      fuse_func=None):
    reproject_and_fuse([new_datasource(dataset, measurement['name']) for dataset in datasets],
                       dest,
                       geobox.affine,
                       geobox.crs,
                       dest.dtype.type(measurement['nodata']),
                       resampling=measurement.get('resampling_method', 'nearest'),
                       fuse_func=fuse_func,
                       skip_broken_datasets=skip_broken_datasets)


def get_bounds(datasets, crs):
    left = min([d.extent.to_crs(crs).boundingbox.left for d in datasets])
    right = max([d.extent.to_crs(crs).boundingbox.right for d in datasets])
    top = max([d.extent.to_crs(crs).boundingbox.top for d in datasets])
    bottom = min([d.extent.to_crs(crs).boundingbox.bottom for d in datasets])
    return geometry.box(left, bottom, right, top, crs=crs)


def set_resampling_method(measurements, resampling=None):
    if resampling is None:
        return measurements

    def make_resampled_measurement(measurement):
        measurement = measurement.copy()
        measurement['resampling_method'] = resampling
        return measurement

    measurements = OrderedDict((name, make_resampled_measurement(measurement))
                               for name, measurement in measurements.items())
    return measurements


def dataset_type_to_row(dt):
    row = {
        'id': dt.id,
        'name': dt.name,
        'description': dt.definition['description'],
    }
    row.update(dt.fields)
    if dt.grid_spec is not None:
        row.update({
            'crs': str(dt.grid_spec.crs),
            'spatial_dimensions': dt.grid_spec.dimensions,
            'tile_size': dt.grid_spec.tile_size,
            'resolution': dt.grid_spec.resolution,
        })
    return row


def _chunk_geobox(geobox, chunk_size):
    num_grid_chunks = [int(ceil(s / float(c))) for s, c in zip(geobox.shape, chunk_size)]
    geobox_subsets = {}
    for grid_index in numpy.ndindex(*num_grid_chunks):
        slices = [slice(min(d * c, stop), min((d + 1) * c, stop))
                  for d, c, stop in zip(grid_index, chunk_size, geobox.shape)]
        geobox_subsets[grid_index] = geobox[slices]
    return geobox_subsets


def _calculate_chunk_sizes(sources, geobox, dask_chunks):
    valid_keys = sources.dims + geobox.dimensions
    bad_keys = set(dask_chunks) - set(valid_keys)
    if bad_keys:
        raise KeyError('Unknown dask_chunk dimension {}. Valid dimensions are: {}'.format(bad_keys, valid_keys))

    # If chunk size is not specified, the entire dimension length is used, as in xarray
    chunks = {dim: size for dim, size in zip(sources.dims, sources.shape)}
    chunks.update({dim: size for dim, size in zip(geobox.dimensions, geobox.shape)})

    chunks.update(dask_chunks)

    irr_chunks = tuple(chunks[dim] for dim in sources.dims)
    grid_chunks = tuple(chunks[dim] for dim in geobox.dimensions)

    return irr_chunks, grid_chunks


# pylint: disable=too-many-locals
def _make_dask_array(sources, geobox, measurement,
                     skip_broken_datasets=False,
                     fuse_func=None,
                     dask_chunks=None):
    dsk_name = 'datacube_' + measurement['name']

    irr_chunks, grid_chunks = _calculate_chunk_sizes(sources, geobox, dask_chunks)
    sliced_irr_chunks = (1,) * sources.ndim

    dsk = {}
    geobox_subsets = _chunk_geobox(geobox, grid_chunks)

    for irr_index, datasets in numpy.ndenumerate(sources.values):
        for grid_index, subset_geobox in geobox_subsets.items():
            dsk[(dsk_name,) + irr_index + grid_index] = (fuse_lazy,
                                                         datasets, subset_geobox, measurement,
                                                         skip_broken_datasets, fuse_func,
                                                         sources.ndim)

    data = da.Array(dsk, dsk_name,
                    chunks=(sliced_irr_chunks + grid_chunks),
                    dtype=measurement['dtype'],
                    shape=(sources.shape + geobox.shape))

    if irr_chunks != sliced_irr_chunks:
        data = data.rechunk(chunks=(irr_chunks + grid_chunks))
    return data
