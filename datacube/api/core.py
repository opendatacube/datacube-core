import uuid
from itertools import groupby
from typing import Union, Optional, Dict, Tuple
import datetime

import numpy
import xarray
from dask import array as da

from datacube.config import LocalConfig
from datacube.storage import reproject_and_fuse, BandInfo
from datacube.utils import geometry
from datacube.utils.geometry import intersects, GeoBox
from datacube.utils.geometry.gbox import GeoboxTiles
from datacube.model.utils import xr_apply

from .query import Query, query_group_by, query_geopolygon
from ..index import index_connect
from ..drivers import new_datasource


class TerminateCurrentLoad(Exception):
    """ This exception is raised by user code from `progress_cbk`
        to terminate currently running `.load`
    """
    pass


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
            if isinstance(config, str):
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
        rows = [product.to_dict() for product in self.index.products.get_all()]
        if not with_pandas:
            return rows

        import pandas
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

        import pandas
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
             skip_broken_datasets=False,
             dask_chunks=None, like=None, fuse_func=None, align=None, datasets=None, progress_cbk=None,
             **query):
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

        :param str|dict resampling:
            The resampling method to use if re-projection is required. This could be a string or
            a dictionary mapping band name to resampling mode. When using a dict use ``'*'`` to
            indicate "apply to all other bands", for example ``{'*': 'cubic', 'fmask': 'nearest'}`` would
            use `cubic` for all bands except ``fmask`` for which `nearest` will be used.

            Valid values are: ``'nearest', 'cubic', 'bilinear', 'cubic_spline', 'lanczos', 'average',
            'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'``

            Default is to use ``nearest`` for all bands.
            .. seealso:: :meth:`load_data`

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
            perform a specific combining step, eg. for combining GA PQ data. This can be a dictionary if different
            fusers are needed per band.

        :param datasets:
            Optional. If this is a non-empty list of :class:`datacube.model.Dataset` objects, these will be loaded
            instead of performing a database lookup.

        :param int limit:
            Optional. If provided, limit the maximum number of datasets
            returned. Useful for testing and debugging.

        :param progress_cbk: Int, Int -> None
            if supplied will be called for every file read with `files_processed_so_far, total_files`. This is
            only applicable to non-lazy loads, ignored when using dask.

        :return: Requested data in a :class:`xarray.Dataset`
        :rtype: :class:`xarray.Dataset`
        """
        if 'stack' in query:
            raise DeprecationWarning("the `stack` keyword argument is not supported anymore, "
                                     "please apply `xarray.Dataset.to_array()` to the result instead")

        # TODO: get rid of this block when removing legacy load support
        legacy_args = {}
        use_threads = query.pop('use_threads', None)
        if use_threads is not None:
            legacy_args['use_threads'] = use_threads

        observations = datasets or self.find_datasets(product=product, like=like, ensure_location=True, **query)
        if not observations:
            return xarray.Dataset()

        geobox = output_geobox(like=like, output_crs=output_crs, resolution=resolution, align=align,
                               grid_spec=self.index.products.get_by_name(product).grid_spec,
                               datasets=observations, **query)

        group_by = query_group_by(**query)
        grouped = self.group_datasets(observations, group_by)

        datacube_product = self.index.products.get_by_name(product)
        measurement_dicts = datacube_product.lookup_measurements(measurements)

        result = self.load_data(grouped, geobox,
                                measurement_dicts,
                                resampling=resampling,
                                fuse_func=fuse_func,
                                dask_chunks=dask_chunks,
                                skip_broken_datasets=skip_broken_datasets,
                                progress_cbk=progress_cbk,
                                **legacy_args)

        return apply_aliases(result, datacube_product, measurements)

    def find_datasets(self, **search_terms):
        """
        Search the index and return all datasets for a product matching the search terms.

        :param search_terms: see :class:`datacube.api.query.Query`
        :return: list of datasets
        :rtype: list[:class:`datacube.model.Dataset`]

        .. seealso:: :meth:`group_datasets` :meth:`load_data` :meth:`find_datasets_lazy`
        """
        return list(self.find_datasets_lazy(**search_terms))

    def find_datasets_lazy(self, limit=None, ensure_location=False, **kwargs):
        """
        Find datasets matching query.

        :param kwargs: see :class:`datacube.api.query.Query`
        :param ensure_location: only return datasets that have locations
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

        if query.geopolygon is not None:
            datasets = select_datasets_inside_polygon(datasets, query.geopolygon)

        if ensure_location:
            datasets = (dataset for dataset in datasets if dataset.uris)

        return datasets

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
        if isinstance(group_by, str):
            group_by = query_group_by(group_by=group_by)

        dimension, group_func, units, sort_key = group_by

        def ds_sorter(ds):
            return sort_key(ds), getattr(ds, 'id', 0)

        def norm_axis_value(x):
            if isinstance(x, datetime.datetime):
                # For datetime we convert to UTC, then strip timezone info
                # to avoid numpy/pandas warning about timezones
                if x.tzinfo is not None:
                    x = x.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                return numpy.datetime64(x, 'ns')
            return x

        def mk_group(group):
            dss = tuple(sorted(group, key=ds_sorter))
            # TODO: decouple axis_value from group sorted order
            axis_value = sort_key(dss[0])
            return (norm_axis_value(axis_value), dss)

        datasets = sorted(datasets, key=group_func)

        groups = [mk_group(group)
                  for _, group in groupby(datasets, group_func)]

        groups.sort(key=lambda x: x[0])

        coords = [coord for coord, _ in groups]
        data = numpy.empty(len(coords), dtype=object)
        for i, (_, dss) in enumerate(groups):
            data[i] = dss

        sources = xarray.DataArray(data, dims=[dimension], coords=[coords])
        sources[dimension].attrs['units'] = units
        return sources

    @staticmethod
    def create_storage(coords, geobox, measurements, data_func=None):
        """
        Create a :class:`xarray.Dataset` and (optionally) fill it with data.

        This function makes the in memory storage structure to hold datacube data, loading data from datasets that have
         been grouped appropriately by :meth:`group_datasets`.

        :param dict coords:
            OrderedDict holding `DataArray` objects defining the dimensions not specified by `geobox`

        :param GeoBox geobox:
            A GeoBox defining the output spatial projection and resolution

        :param measurements:
            list of :class:`datacube.model.Measurement`

        :param data_func:
            function to fill the storage with data. It is called once for each measurement, with the measurement
            as an argument. It should return an appropriately shaped numpy array. If not provided, an empty
            :class:`xarray.Dataset` is returned.

        :rtype: :class:`xarray.Dataset`

        .. seealso:: :meth:`find_datasets` :meth:`group_datasets`
        """

        def empty_func(measurement_):
            coord_shape = tuple(coord_.size for coord_ in coords.values())
            return numpy.full(coord_shape + geobox.shape, measurement_.nodata, dtype=measurement_.dtype)

        data_func = data_func or empty_func

        result = xarray.Dataset(attrs={'crs': geobox.crs})
        for name, coord in coords.items():
            result[name] = coord
        for name, coord in geobox.coordinates.items():
            result[name] = (name, coord.values, {'units': coord.units})

        for measurement in measurements:
            data = data_func(measurement)
            attrs = measurement.dataarray_attrs()
            attrs['crs'] = geobox.crs
            dims = tuple(coords.keys()) + tuple(geobox.dimensions)
            result[measurement.name] = (dims, data, attrs)

        return result

    @staticmethod
    def _dask_load(sources, geobox, measurements, dask_chunks,
                   skip_broken_datasets=False):
        needed_irr_chunks, grid_chunks = _calculate_chunk_sizes(sources, geobox, dask_chunks)
        gbt = GeoboxTiles(geobox, grid_chunks)
        dsk = {}

        def chunk_datasets(dss, gbt):
            out = {}
            for ds in dss:
                dsk[_tokenize_dataset(ds)] = ds
                for idx in gbt.tiles(ds.extent):
                    out.setdefault(idx, []).append(ds)
            return out

        chunked_srcs = xr_apply(sources,
                                lambda _, dss: chunk_datasets(dss, gbt),
                                dtype=object)

        def data_func(measurement):
            return _make_dask_array(chunked_srcs, dsk, gbt,
                                    measurement,
                                    chunks=needed_irr_chunks+grid_chunks,
                                    skip_broken_datasets=skip_broken_datasets)

        return Datacube.create_storage(sources.coords, geobox, measurements, data_func)

    @staticmethod
    def _xr_load(sources, geobox, measurements,
                 skip_broken_datasets=False,
                 progress_cbk=None):

        def mk_cbk(cbk):
            if cbk is None:
                return None
            n = 0
            n_total = sum(len(x) for x in sources.values.ravel())*len(measurements)

            def _cbk(*ignored):
                nonlocal n
                n += 1
                return cbk(n, n_total)
            return _cbk

        data = Datacube.create_storage(sources.coords, geobox, measurements)
        _cbk = mk_cbk(progress_cbk)

        for index, datasets in numpy.ndenumerate(sources.values):
            for m in measurements:
                t_slice = data[m.name].values[index]

                try:
                    _fuse_measurement(t_slice, datasets, geobox, m,
                                      skip_broken_datasets=skip_broken_datasets,
                                      progress_cbk=_cbk)
                except (TerminateCurrentLoad, KeyboardInterrupt):
                    data.attrs['dc_partial_load'] = True
                    return data

        return data

    @staticmethod
    def load_data(sources, geobox, measurements, resampling=None,
                  fuse_func=None, dask_chunks=None, skip_broken_datasets=False,
                  progress_cbk=None,
                  **extra):
        """
        Load data from :meth:`group_datasets` into an :class:`xarray.Dataset`.

        :param xarray.DataArray sources:
            DataArray holding a list of :class:`datacube.model.Dataset`, grouped along the time dimension

        :param GeoBox geobox:
            A GeoBox defining the output spatial projection and resolution

        :param measurements:
            list of `Measurement` objects

        :param str|dict resampling:
            The resampling method to use if re-projection is required. This could be a string or
            a dictionary mapping band name to resampling mode. When using a dict use ``'*'`` to
            indicate "apply to all other bands", for example ``{'*': 'cubic', 'fmask': 'nearest'}`` would
            use `cubic` for all bands except ``fmask`` for which `nearest` will be used.

            Valid values are: ``'nearest', 'cubic', 'bilinear', 'cubic_spline', 'lanczos', 'average',
            'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'``

            Default is to use ``nearest`` for all bands.

        :param fuse_func:
            function to merge successive arrays as an output. Can be a dictionary just like resampling.

        :param dict dask_chunks:
            If provided, the data will be loaded on demand using using :class:`dask.array.Array`.
            Should be a dictionary specifying the chunking size for each output dimension.
            Unspecified dimensions will be auto-guessed, currently this means use chunk size of 1 for non-spatial
            dimensions and use whole dimension (no chunking unless specified) for spatial dimensions.

            See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
            for more information.

        :param progress_cbk: Int, Int -> None
            if supplied will be called for every file read with `files_processed_so_far, total_files`. This is
            only applicable to non-lazy loads, ignored when using dask.

        :rtype: xarray.Dataset

        .. seealso:: :meth:`find_datasets` :meth:`group_datasets`
        """
        def with_resampling(m, resampling, default=None):
            m = m.copy()
            m['resampling_method'] = resampling.get(m.name, default)
            return m

        def with_fuser(m, fuser, default=None):
            m = m.copy()
            m['fuser'] = fuser.get(m.name, default)
            return m

        if isinstance(resampling, str):
            resampling = {'*': resampling}

        if not isinstance(fuse_func, dict):
            fuse_func = {'*': fuse_func}

        if isinstance(measurements, dict):
            measurements = list(measurements.values())

        if resampling is not None:
            measurements = [with_resampling(m, resampling, default=resampling.get('*'))
                            for m in measurements]

        if fuse_func is not None:
            measurements = [with_fuser(m, fuse_func, default=fuse_func.get('*'))
                            for m in measurements]

        if _needs_legacy_fallback(sources):
            from . import _legacy
            return _legacy.load_data(sources, geobox, measurements,
                                     dask_chunks=dask_chunks,
                                     skip_broken_datasets=skip_broken_datasets,
                                     **extra)

        if dask_chunks is not None:
            return Datacube._dask_load(sources, geobox, measurements, dask_chunks,
                                       skip_broken_datasets=skip_broken_datasets)
        else:
            return Datacube._xr_load(sources, geobox, measurements,
                                     skip_broken_datasets=skip_broken_datasets,
                                     progress_cbk=progress_cbk)

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
        :param measurement: `Measurement` object
        :param fuse_func: function to merge successive arrays as an output
        :param dict dask_chunks: If the data should be loaded as needed using :class:`dask.array.Array`,
            specify the chunk size in each output direction.
            See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
            for more information.
        :rtype: :class:`xarray.DataArray`
        """
        dataset = Datacube.load_data(sources, geobox, [measurement], fuse_func=fuse_func, dask_chunks=dask_chunks)
        dataarray = dataset[measurement.name]
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
                  grid_spec=None, datasets=None, geopolygon=None, **query):
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

    if geopolygon is None:
        geopolygon = query_geopolygon(**query)

        if geopolygon is None:
            geopolygon = get_bounds(datasets, crs)

    return geometry.GeoBox.from_geopolygon(geopolygon, resolution, crs, align)


def select_datasets_inside_polygon(datasets, polygon):
    # Check against the bounding box of the original scene, can throw away some portions
    assert polygon is not None
    query_crs = polygon.crs
    for dataset in datasets:
        if intersects(polygon, dataset.extent.to_crs(query_crs)):
            yield dataset


def fuse_lazy(datasets, geobox, measurement, skip_broken_datasets=False, prepend_dims=0):
    prepend_shape = (1,) * prepend_dims
    data = numpy.full(geobox.shape, measurement.nodata, dtype=measurement.dtype)
    _fuse_measurement(data, datasets, geobox, measurement,
                      skip_broken_datasets=skip_broken_datasets)
    return data.reshape(prepend_shape + geobox.shape)


def _fuse_measurement(dest, datasets, geobox, measurement,
                      skip_broken_datasets=False,
                      progress_cbk=None):
    reproject_and_fuse([new_datasource(BandInfo(dataset, measurement.name)) for dataset in datasets],
                       dest,
                       geobox,
                       dest.dtype.type(measurement.nodata),
                       resampling=measurement.get('resampling_method', 'nearest'),
                       fuse_func=measurement.get('fuser', None),
                       skip_broken_datasets=skip_broken_datasets,
                       progress_cbk=progress_cbk)


def get_bounds(datasets, crs):
    bbox = geometry.bbox_union(ds.extent.to_crs(crs).boundingbox for ds in datasets)
    return geometry.box(*bbox, crs=crs)


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


def _calculate_chunk_sizes(sources: xarray.DataArray,
                           geobox: GeoBox,
                           dask_chunks: Dict[str, Union[str, int]]):
    valid_keys = sources.dims + geobox.dimensions
    bad_keys = set(dask_chunks) - set(valid_keys)
    if bad_keys:
        raise KeyError('Unknown dask_chunk dimension {}. Valid dimensions are: {}'.format(bad_keys, valid_keys))

    chunk_maxsz = {dim: sz
                   for dim, sz in zip(sources.dims + geobox.dimensions,
                                      sources.shape + geobox.shape)}  # type: Dict[str, int]

    # defaults: 1 for non-spatial, whole dimension for Y/X
    chunk_defaults = dict(**{dim: 1 for dim in sources.dims},
                          **{dim: -1 for dim in geobox.dimensions})   # type: Dict[str, int]

    def _resolve(k, v: Optional[Union[str, int]]) -> int:
        if v is None or v == "auto":
            v = _resolve(k, chunk_defaults[k])

        if isinstance(v, int):
            if v < 0:
                return chunk_maxsz[k]
            return v
        raise ValueError("Chunk should be one of int|'auto'")

    irr_chunks = tuple(_resolve(dim, dask_chunks.get(dim)) for dim in sources.dims)
    grid_chunks = tuple(_resolve(dim, dask_chunks.get(dim)) for dim in geobox.dimensions)

    return irr_chunks, grid_chunks


def _tokenize_dataset(dataset):
    return 'dataset-{}'.format(dataset.id.hex)


# pylint: disable=too-many-locals
def _make_dask_array(chunked_srcs,
                     dsk,
                     gbt,
                     measurement,
                     chunks,
                     skip_broken_datasets=False):
    dsk = dsk.copy()  # this contains mapping from dataset id to dataset object

    token = uuid.uuid4().hex
    dsk_name = 'dc_load_{name}-{token}'.format(name=measurement.name, token=token)

    needed_irr_chunks, grid_chunks = chunks[:-2], chunks[-2:]
    actual_irr_chunks = (1,) * len(needed_irr_chunks)

    # we can have up to 4 empty chunk shapes: whole, right edge, bottom edge and
    # bottom right corner
    #  W R
    #  B BR
    empties = {}  # type Dict[Tuple[int,int], str]

    def _mk_empty(shape: Tuple[int, int]) -> str:
        name = empties.get(shape, None)
        if name is not None:
            return name

        name = 'empty_{}x{}-{token}'.format(*shape, token=token)
        dsk[name] = (numpy.full, actual_irr_chunks + shape, measurement.nodata, measurement.dtype)
        empties[shape] = name

        return name

    for irr_index, tiled_dss in numpy.ndenumerate(chunked_srcs.values):
        key_prefix = (dsk_name, *irr_index)

        # all spatial chunks
        for idx in numpy.ndindex(gbt.shape):
            dss = tiled_dss.get(idx, None)

            if dss is None:
                val = _mk_empty(gbt.chunk_shape(idx))
            else:
                val = (fuse_lazy,
                       [_tokenize_dataset(ds) for ds in dss],
                       gbt[idx],
                       measurement,
                       skip_broken_datasets,
                       chunked_srcs.ndim)

            dsk[key_prefix + idx] = val

    y_shapes = [grid_chunks[0]]*gbt.shape[0]
    x_shapes = [grid_chunks[1]]*gbt.shape[1]

    y_shapes[-1], x_shapes[-1] = gbt.chunk_shape(tuple(n-1 for n in gbt.shape))

    data = da.Array(dsk, dsk_name,
                    chunks=actual_irr_chunks + (tuple(y_shapes), tuple(x_shapes)),
                    dtype=measurement.dtype,
                    shape=(chunked_srcs.shape + gbt.base.shape))

    if needed_irr_chunks != actual_irr_chunks:
        data = data.rechunk(chunks=chunks)
    return data


def _needs_legacy_fallback(sources):
    if sources.shape[0] == 0:
        return False

    ds = sources.values[0][0]
    is_s3aio_ds = ds.format == 'aio'
    return True if is_s3aio_ds else False
