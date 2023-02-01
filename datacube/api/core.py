# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2021 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import uuid
import collections.abc
from itertools import groupby
from typing import Set, Union, Optional, Dict, Tuple, cast
import datetime

import numpy
import xarray
from dask import array as da

from datacube.config import LocalConfig
from datacube.storage import reproject_and_fuse, BandInfo
from datacube.utils import ignore_exceptions_if
from datacube.utils import geometry
from datacube.utils.dates import normalise_dt
from datacube.utils.geometry import intersects, GeoBox
from datacube.utils.geometry.gbox import GeoboxTiles
from datacube.model import ExtraDimensions
from datacube.model.utils import xr_apply

from .query import Query, query_group_by, query_geopolygon
from ..index import index_connect
from ..drivers import new_datasource


class TerminateCurrentLoad(Exception):  # noqa: N818
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

    def list_products(self, with_pandas=True, dataset_count=False):
        """
        List all products in the datacube. This will produce a ``pandas.DataFrame``
        or list of dicts containing useful information about each product, including:

            'name'
            'description'
            'license'
            'default_crs' or 'grid_spec.crs'
            'default_resolution' or 'grid_spec.crs'
            'dataset_count' (optional)

        :param bool with_pandas:
            Return the list as a Pandas DataFrame. If False, return a list of dicts.

        :param bool dataset_count:
            Return a "dataset_count" column containing the number of datasets
            for each product. This can take several minutes on large datacubes.
            Defaults to False.

        :return: A table or list of every product in the datacube.
        :rtype: pandas.DataFrame or list(dict)
        """
        # Read properties from each datacube product
        cols = [
            'name',
            'description',
            'license',
            'default_crs',
            'default_resolution',
        ]
        rows = [[
            getattr(pr, col, None)
            # if 'default_crs' and 'default_resolution' are not None
            # return 'default_crs' and 'default_resolution'
            if getattr(pr, col, None) and 'default' not in col
            # else try 'grid_spec.crs' and 'grid_spec.resolution'
            # as per output_geobox() handling logic
            else getattr(pr.grid_spec, col.replace('default_', ''), None)
            for col in cols]
            for pr in self.index.products.get_all()]

        # Optionally compute dataset count for each product and add to row/cols
        # Product lists are sorted by product name to ensure 1:1 match
        if dataset_count:

            # Load counts
            counts = [(p.name, c) for p, c in self.index.datasets.count_by_product()]

            # Sort both rows and counts by product name
            from operator import itemgetter
            rows = sorted(rows, key=itemgetter(0))
            counts = sorted(counts, key=itemgetter(0))

            # Add sorted count to each existing row
            rows = [row + [count[1]] for row, count in zip(rows, counts)]
            cols = cols + ['dataset_count']

        # If pandas not requested, return list of dicts
        if not with_pandas:
            return [dict(zip(cols, row)) for row in rows]

        # Return pandas dataframe with each product as a row
        import pandas
        return pandas.DataFrame(rows, columns=cols).set_index('name', drop=False)

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
             skip_broken_datasets=False, dask_chunks=None, like=None, fuse_func=None, align=None,
             datasets=None, dataset_predicate=None, progress_cbk=None, patch_url=None, **query):
        """
        Load data as an ``xarray.Dataset`` object.
        Each measurement will be a data variable in the :class:`xarray.Dataset`.

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
            If not provided, all measurements for the product will be returned. ::

                measurements=['red', 'nir', 'swir2']

        **Dimensions**
            Spatial dimensions can specified using the ``longitude``/``latitude`` and ``x``/``y`` fields.

            The CRS of this query is assumed to be WGS84/EPSG:4326 unless the ``crs`` field is supplied,
            even if the stored data is in another projection or the ``output_crs`` is specified.
            The dimensions ``longitude``/``latitude`` and ``x``/``y`` can be used interchangeably.
            ::

                latitude=(-34.5, -35.2), longitude=(148.3, 148.7)

            or ::

                x=(1516200, 1541300), y=(-3867375, -3867350), crs='EPSG:3577'

            The ``time`` dimension can be specified using a tuple of datetime objects or strings with
            ``YYYY-MM-DD hh:mm:ss`` format. Data will be loaded inclusive of the start and finish times. E.g::

                time=('2000-01-01', '2001-12-31')
                time=('2000-01', '2001-12')
                time=('2000', '2001')

            For 3D datasets, where the product definition contains an ``extra_dimension`` specification,
            these dimensions can be queried using that dimension's name. E.g.::

                z=(10, 30)

            or ::

                z=5

            or ::

                wvl=(560.3, 820.5)

            For EO-specific datasets that are based around scenes, the time dimension can be reduced to the day level,
            using solar day to keep scenes together.
            ::

                group_by='solar_day'

            For data that has different values for the scene overlap the requires more complex rules for combining data,
            a function can be provided to the merging into a single time slice.

            See :func:`datacube.helpers.ga_pq_fuser` for an example implementation.
            see :func:`datacube.api.query.query_group_by` for `group_by` built-in functions.


        **Output**
            To reproject or resample data, supply the ``output_crs``, ``resolution``, ``resampling`` and ``align``
            fields.

            By default, the resampling method is 'nearest'. However, any stored overview layers may be used
            when down-sampling, which may override (or hybridise) the choice of resampling method.

            To reproject data to 30 m resolution for EPSG:3577::

                dc.load(product='ls5_nbar_albers',
                        x=(148.15, 148.2),
                        y=(-35.15, -35.2),
                        time=('1990', '1991'),
                        output_crs='EPSG:3577`,
                        resolution=(-30, 30),
                        resampling='cubic'
                )


        :param str product:
            The product to be loaded.

        :param list(str) measurements:
            Measurements name or list of names to be included, as listed in :meth:`list_measurements`.
            These will be loaded as individual ``xr.DataArray`` variables in
            the output ``xarray.Dataset`` object.

            If a list is specified, the measurements will be returned in the order requested.
            By default all available measurements are included.

        :param \*\*query:
            Search parameters for products and dimension ranges as described above.
            For example: ``'x', 'y', 'time', 'crs'``.

        :param str output_crs:
            The CRS of the returned data, for example ``EPSG:3577``.
            If no CRS is supplied, the CRS of the stored data is used if available.

            This differs from the ``crs`` parameter desribed above, which is used to define the CRS
            of the coordinates in the query itself.

        :param (float,float) resolution:
            A tuple of the spatial resolution of the returned data. Units are in the coordinate
            space of ``output_crs``.

            This includes the direction (as indicated by a positive or negative number).
            For most CRSs, the first number will be negative, e.g. ``(-30, 30)``.

        :param str|dict resampling:
            The resampling method to use if re-projection is required. This could be a string or
            a dictionary mapping band name to resampling mode. When using a dict use ``'*'`` to
            indicate "apply to all other bands", for example ``{'*': 'cubic', 'fmask': 'nearest'}`` would
            use ``cubic`` for all bands except ``fmask`` for which ``nearest`` will be used.

            Valid values are: ::

              'nearest', 'average', 'bilinear', 'cubic', 'cubic_spline',
              'lanczos', 'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'

            Default is to use ``nearest`` for all bands.

            .. seealso::
               :meth:`load_data`

        :param (float,float) align:
            Load data such that point 'align' lies on the pixel boundary.
            Units are in the coordinate space of ``output_crs``.

            Default is ``(0, 0)``

        :param dict dask_chunks:
            If the data should be lazily loaded using :class:`dask.array.Array`,
            specify the chunking size in each output dimension.

            See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
            for more information.

        :param xarray.Dataset like:
            Use the output of a previous :meth:`load()` to load data into the same spatial grid and
            resolution (i.e. :class:`datacube.utils.geometry.GeoBox`).
            E.g.::

                pq = dc.load(product='ls5_pq_albers', like=nbar_dataset)

        :param str group_by:
            When specified, perform basic combining/reducing of the data. For example, ``group_by='solar_day'``
            can be used to combine consecutive observations along a single satellite overpass into a single time slice.

        :param fuse_func:
            Function used to fuse/combine/reduce data with the ``group_by`` parameter. By default,
            data is simply copied over the top of each other in a relatively undefined manner. This function can
            perform a specific combining step. This can be a dictionary if different
            fusers are needed per band.

        :param datasets:
            Optional. If this is a non-empty list of :class:`datacube.model.Dataset` objects, these will be loaded
            instead of performing a database lookup.

        :param bool skip_broken_datasets:
            Optional. If this is True, then don't break when failing to load a broken dataset.
            Default is False.

        :param function dataset_predicate:
            Optional. A function that can be passed to restrict loaded datasets. A predicate function should
            take a :class:`datacube.model.Dataset` object (e.g. as returned from :meth:`find_datasets`) and
            return a boolean.
            For example, loaded data could be filtered to January observations only by passing the following
            predicate function that returns True for datasets acquired in January::

                def filter_jan(dataset): return dataset.time.begin.month == 1

        :param int limit:
            Optional. If provided, limit the maximum number of datasets
            returned. Useful for testing and debugging.

        :param progress_cbk:
            ``Int, Int -> None``,
            if supplied will be called for every file read with ``files_processed_so_far, total_files``. This is
            only applicable to non-lazy loads, ignored when using dask.

        :param Callable[[str], str], patch_url:
            if supplied, will be used to patch/sign the url(s), as required to access some commercial archives
            (e.g. Microsoft Planetary Computer).

        :return:
            Requested data in a :class:`xarray.Dataset`

        :rtype:
            :class:`xarray.Dataset`
        """
        if product is None and datasets is None:
            raise ValueError("Must specify a product or supply datasets")

        if datasets is None:
            datasets = self.find_datasets(product=product,
                                          like=like,
                                          ensure_location=True,
                                          dataset_predicate=dataset_predicate,
                                          **query)
        elif isinstance(datasets, collections.abc.Iterator):
            datasets = list(datasets)

        if len(datasets) == 0:
            return xarray.Dataset()

        ds, *_ = datasets
        datacube_product = ds.product

        # Retrieve extra_dimension from product definition
        extra_dims = None
        if datacube_product:
            extra_dims = datacube_product.extra_dimensions

            # Extract extra_dims slice information
            extra_dims_slice = {
                k: query.pop(k, None)
                for k in list(query.keys())
                if k in extra_dims.dims and query.get(k, None) is not None
            }
            extra_dims = extra_dims[extra_dims_slice]
            # Check if empty
            if extra_dims.has_empty_dim():
                return xarray.Dataset()

        geobox = output_geobox(like=like, output_crs=output_crs, resolution=resolution, align=align,
                               grid_spec=datacube_product.grid_spec,
                               load_hints=datacube_product.load_hints(),
                               datasets=datasets, **query)
        group_by = query_group_by(**query)
        grouped = self.group_datasets(datasets, group_by)

        measurement_dicts = datacube_product.lookup_measurements(measurements)

        # `extra_dims` put last for backwards compability, but should really be the second position
        # betwween `grouped` and `geobox`
        result = self.load_data(grouped, geobox,
                                measurement_dicts,
                                resampling=resampling,
                                fuse_func=fuse_func,
                                dask_chunks=dask_chunks,
                                skip_broken_datasets=skip_broken_datasets,
                                progress_cbk=progress_cbk,
                                extra_dims=extra_dims,
                                patch_url=patch_url)

        return result

    def find_datasets(self, **search_terms):
        """
        Search the index and return all datasets for a product matching the search terms.

        :param search_terms: see :class:`datacube.api.query.Query`
        :return: list of datasets
        :rtype: list[:class:`datacube.model.Dataset`]

        .. seealso:: :meth:`group_datasets` :meth:`load_data` :meth:`find_datasets_lazy`
        """
        return list(self.find_datasets_lazy(**search_terms))

    def find_datasets_lazy(self, limit=None, ensure_location=False, dataset_predicate=None, **kwargs):
        """
        Find datasets matching query.

        :param kwargs: see :class:`datacube.api.query.Query`
        :param ensure_location: only return datasets that have locations
        :param limit: if provided, limit the maximum number of datasets returned
        :param dataset_predicate: an optional predicate to filter datasets
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

        # If a predicate function is provided, use this to filter datasets before load
        if dataset_predicate is not None:
            datasets = (dataset for dataset in datasets if dataset_predicate(dataset))

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

        def ds_sorter(ds):
            return group_by.sort_key(ds), getattr(ds, 'id', 0)

        def norm_axis_value(x):
            if isinstance(x, datetime.datetime):
                # For datetime we convert to UTC, then strip timezone info
                # to avoid numpy/pandas warning about timezones
                return numpy.datetime64(normalise_dt(x), 'ns')
            return x

        def mk_group(group):
            dss = tuple(sorted(group, key=ds_sorter))
            return (norm_axis_value(group_by.group_key(dss)), dss)

        datasets = sorted(datasets, key=group_by.group_by_func)

        groups = [mk_group(group)
                  for _, group in groupby(datasets, group_by.group_by_func)]

        groups.sort(key=lambda x: x[0])

        coords = numpy.asarray([coord for coord, _ in groups])
        data = numpy.empty(len(coords), dtype=object)
        for i, (_, dss) in enumerate(groups):
            data[i] = dss

        sources = xarray.DataArray(data,
                                   dims=[group_by.dimension],
                                   coords=[coords])
        if coords.dtype.kind == 'M':
            # skip units for time dimensions as it breaks .to_netcdf(..) functionality #972
            sources[group_by.dimension].attrs['units'] = group_by.units

        return sources

    @staticmethod
    def create_storage(coords, geobox, measurements, data_func=None, extra_dims=None):
        """
        Create a :class:`xarray.Dataset` and (optionally) fill it with data.

        This function makes the in memory storage structure to hold datacube data.

        :param dict coords:
            OrderedDict holding `DataArray` objects defining the dimensions not specified by `geobox`

        :param GeoBox geobox:
            A GeoBox defining the output spatial projection and resolution

        :param measurements:
            list of :class:`datacube.model.Measurement`

        :param data_func: Callable `Measurement -> np.ndarray`
            function to fill the storage with data. It is called once for each measurement, with the measurement
            as an argument. It should return an appropriately shaped numpy array. If not provided memory is
            allocated an filled with `nodata` value defined on a given Measurement.

        :param ExtraDimensions extra_dims:
            A ExtraDimensions describing the any additional dimensions on top of (t, y, x)

        :rtype: :class:`xarray.Dataset`

        .. seealso:: :meth:`find_datasets` :meth:`group_datasets`
        """
        from collections import OrderedDict
        from copy import deepcopy
        spatial_ref = 'spatial_ref'

        def empty_func(m, shape):
            return numpy.full(shape, m.nodata, dtype=m.dtype)

        crs_attrs = {}
        if geobox.crs is not None:
            crs_attrs['crs'] = str(geobox.crs)
            crs_attrs['grid_mapping'] = spatial_ref

        # Assumptions
        #  - 3D dims must fit between (t) and (y, x) or (lat, lon)

        # 2D defaults
        # retrieve dims from coords if DataArray
        dims_default = None
        if coords != {}:
            coords_value = next(iter(coords.values()))
            if isinstance(coords_value, xarray.DataArray):
                dims_default = coords_value.dims + geobox.dimensions

        if dims_default is None:
            dims_default = tuple(coords) + geobox.dimensions

        shape_default = tuple(c.size for k, c in coords.items() if k in dims_default) + geobox.shape
        coords_default = OrderedDict(**coords, **geobox.xr_coords(with_crs=spatial_ref))

        arrays = []
        ds_coords = deepcopy(coords_default)

        for m in measurements:
            if 'extra_dim' not in m:
                # 2D default case
                arrays.append((m, shape_default, coords_default, dims_default))
            elif extra_dims:
                # 3D case
                name = m.extra_dim
                new_dims = dims_default[:1] + (name,) + dims_default[1:]
                new_coords = deepcopy(coords_default)
                new_coords[name] = extra_dims._coords[name].copy()
                new_coords[name].attrs.update(crs_attrs)
                ds_coords.update(new_coords)

                new_shape = shape_default[:1] + (len(new_coords[name].values),) + shape_default[1:]
                arrays.append((m, new_shape, new_coords, new_dims))

        data_func = data_func or (lambda m, shape: empty_func(m, shape))

        def mk_data_var(m, shape, coords, dims, data_func):
            data = data_func(m, shape)
            attrs = dict(**m.dataarray_attrs(),
                         **crs_attrs)
            return xarray.DataArray(data,
                                    name=m.name,
                                    coords=coords,
                                    dims=dims,
                                    attrs=attrs)

        return xarray.Dataset({m.name: mk_data_var(m, shape, coords, dims, data_func)
                               for m, shape, coords, dims in arrays},
                              coords=ds_coords,
                              attrs=crs_attrs)

    @staticmethod
    def _dask_load(sources, geobox, measurements, dask_chunks,
                   skip_broken_datasets=False, extra_dims=None, patch_url=None):
        chunk_sizes = _calculate_chunk_sizes(sources, geobox, dask_chunks, extra_dims)
        needed_irr_chunks = chunk_sizes[0]
        if extra_dims:
            extra_dim_chunks = chunk_sizes[1]
        grid_chunks = chunk_sizes[-1]
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

        def data_func(measurement, shape):
            if 'extra_dim' in measurement:
                chunks = needed_irr_chunks + extra_dim_chunks + grid_chunks
            else:
                chunks = needed_irr_chunks + grid_chunks
            return _make_dask_array(chunked_srcs, dsk, gbt,
                                    measurement,
                                    chunks=chunks,
                                    skip_broken_datasets=skip_broken_datasets,
                                    extra_dims=extra_dims,
                                    patch_url=patch_url)

        return Datacube.create_storage(sources.coords, geobox, measurements, data_func, extra_dims)

    @staticmethod
    def _xr_load(sources, geobox, measurements,
                 skip_broken_datasets=False,
                 progress_cbk=None, extra_dims=None,
                 patch_url=None):

        def mk_cbk(cbk):
            if cbk is None:
                return None
            n = 0
            t_size = sum(len(x) for x in sources.values.ravel())
            n_total = 0
            for m in measurements:
                if 'extra_dim' in m:
                    index_subset = extra_dims.measurements_slice(m.extra_dim)
                    n_total += t_size*len(m.extra_dim.get('measurement_map')[index_subset])
                else:
                    n_total += t_size

            def _cbk(*ignored):
                nonlocal n
                n += 1
                return cbk(n, n_total)
            return _cbk

        data = Datacube.create_storage(sources.coords, geobox, measurements, extra_dims=extra_dims)
        _cbk = mk_cbk(progress_cbk)

        # Create a list of read IO operations
        read_ios = []
        for index, datasets in numpy.ndenumerate(sources.values):
            for m in measurements:
                if 'extra_dim' in m:
                    # When we want to support 3D native reads, we can start by replacing the for loop with
                    # read_ios.append(((index + extra_dim_index), (datasets, m, index_subset)))
                    index_subset = extra_dims.measurements_index(m.extra_dim)
                    for result_index, extra_dim_index in enumerate(range(*index_subset)):
                        read_ios.append(((index + (result_index,)), (datasets, m, extra_dim_index)))
                else:
                    # Get extra_dim index if available
                    extra_dim_index = m.get('extra_dim_index', None)
                    read_ios.append((index, (datasets, m, extra_dim_index)))

        # Perform the read IO operations
        for index, (datasets, m, extra_dim_index) in read_ios:
            data_slice = data[m.name].values[index]
            try:
                _fuse_measurement(data_slice, datasets, geobox, m,
                                  skip_broken_datasets=skip_broken_datasets,
                                  progress_cbk=_cbk, extra_dim_index=extra_dim_index,
                                  patch_url=patch_url)
            except (TerminateCurrentLoad, KeyboardInterrupt):
                data.attrs['dc_partial_load'] = True
                return data

        return data

    @staticmethod
    def load_data(sources, geobox, measurements, resampling=None,
                  fuse_func=None, dask_chunks=None, skip_broken_datasets=False,
                  progress_cbk=None, extra_dims=None, patch_url=None,
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

        :param ExtraDimensions extra_dims:
            A ExtraDimensions describing the any additional dimensions on top of (t, y, x)

        :param Callable[[str], str], patch_url:
            if supplied, will be used to patch/sign the url(s), as required to access some commercial archives.

        :rtype: xarray.Dataset

        .. seealso:: :meth:`find_datasets` :meth:`group_datasets`
        """
        measurements = per_band_load_data_settings(measurements, resampling=resampling, fuse_func=fuse_func)

        if dask_chunks is not None:
            return Datacube._dask_load(sources, geobox, measurements, dask_chunks,
                                       skip_broken_datasets=skip_broken_datasets,
                                       extra_dims=extra_dims,
                                       patch_url=patch_url)
        else:
            return Datacube._xr_load(sources, geobox, measurements,
                                     skip_broken_datasets=skip_broken_datasets,
                                     progress_cbk=progress_cbk,
                                     extra_dims=extra_dims,
                                     patch_url=patch_url)

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


def per_band_load_data_settings(measurements, resampling=None, fuse_func=None):
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

    return measurements


def output_geobox(like=None, output_crs=None, resolution=None, align=None,
                  grid_spec=None, load_hints=None, datasets=None, geopolygon=None, **query):
    """ Configure output geobox from user provided output specs. """

    if like is not None:
        assert output_crs is None, "'like' and 'output_crs' are not supported together"
        assert resolution is None, "'like' and 'resolution' are not supported together"
        assert align is None, "'like' and 'align' are not supported together"
        if isinstance(like, GeoBox):
            return like

        return like.geobox

    if load_hints:
        if output_crs is None:
            output_crs = load_hints.get('output_crs', None)

        if resolution is None:
            resolution = load_hints.get('resolution', None)

        if align is None:
            align = load_hints.get('align', None)

    if output_crs is not None:
        if resolution is None:
            raise ValueError("Must specify 'resolution' when specifying 'output_crs'")
        crs = geometry.CRS(output_crs)
    elif grid_spec is not None:
        # specification from grid_spec
        crs = grid_spec.crs
        if resolution is None:
            resolution = grid_spec.resolution
        align = align or grid_spec.alignment
    else:
        raise ValueError(
            "Product has no default CRS. \n"
            "Must specify 'output_crs' and 'resolution'"
        )

    # Try figuring out bounds
    #  1. Explicitly defined with geopolygon
    #  2. Extracted from x=,y=
    #  3. Computed from dataset footprints
    #  4. fail with ValueError
    if geopolygon is None:
        geopolygon = query_geopolygon(**query)

        if geopolygon is None:
            if datasets is None:
                raise ValueError("Bounds are not specified")

            geopolygon = get_bounds(datasets, crs)

    return geometry.GeoBox.from_geopolygon(geopolygon, resolution, crs, align)


def select_datasets_inside_polygon(datasets, polygon):
    # Check against the bounding box of the original scene, can throw away some portions
    assert polygon is not None
    query_crs = polygon.crs
    for dataset in datasets:
        if intersects(polygon, dataset.extent.to_crs(query_crs)):
            yield dataset


def fuse_lazy(datasets, geobox, measurement,
              skip_broken_datasets=False, prepend_dims=0, extra_dim_index=None, patch_url=None):
    prepend_shape = (1,) * prepend_dims
    data = numpy.full(geobox.shape, measurement.nodata, dtype=measurement.dtype)
    _fuse_measurement(data, datasets, geobox, measurement,
                      skip_broken_datasets=skip_broken_datasets,
                      extra_dim_index=extra_dim_index,
                      patch_url=patch_url)
    return data.reshape(prepend_shape + geobox.shape)


def _fuse_measurement(dest, datasets, geobox, measurement,
                      skip_broken_datasets=False,
                      progress_cbk=None,
                      extra_dim_index=None,
                      patch_url=None):
    srcs = []
    for ds in datasets:
        src = None
        with ignore_exceptions_if(skip_broken_datasets):
            src = new_datasource(
                BandInfo(ds, measurement.name, extra_dim_index=extra_dim_index, patch_url=patch_url)
            )

        if src is None:
            if not skip_broken_datasets:
                raise ValueError(f"Failed to load dataset: {ds.id}")
        else:
            srcs.append(src)

    reproject_and_fuse(srcs,
                       dest,
                       geobox,
                       dest.dtype.type(measurement.nodata),
                       resampling=measurement.get('resampling_method', 'nearest'),
                       fuse_func=measurement.get('fuser', None),
                       skip_broken_datasets=skip_broken_datasets,
                       progress_cbk=progress_cbk,
                       extra_dim_index=extra_dim_index)


def get_bounds(datasets, crs):
    bbox = geometry.bbox_union(ds.extent.to_crs(crs).boundingbox for ds in datasets)
    return geometry.box(*bbox, crs=crs)


def _calculate_chunk_sizes(sources: xarray.DataArray,
                           geobox: GeoBox,
                           dask_chunks: Dict[str, Union[str, int]],
                           extra_dims: Optional[ExtraDimensions] = None):
    extra_dim_names: Tuple[str, ...] = ()
    extra_dim_shapes: Tuple[int, ...] = ()
    if extra_dims is not None:
        extra_dim_names, extra_dim_shapes = extra_dims.chunk_size()

    valid_keys = sources.dims + extra_dim_names + geobox.dimensions
    bad_keys = cast(Set[str], set(dask_chunks)) - cast(Set[str], set(valid_keys))
    if bad_keys:
        raise KeyError('Unknown dask_chunk dimension {}. Valid dimensions are: {}'.format(bad_keys, valid_keys))

    chunk_maxsz = dict((dim, sz) for dim, sz in zip(sources.dims + extra_dim_names + geobox.dimensions,
                                                    sources.shape + extra_dim_shapes + geobox.shape))

    # defaults: 1 for non-spatial, whole dimension for Y/X
    chunk_defaults = dict([(dim, 1) for dim in sources.dims] + [(dim, 1) for dim in extra_dim_names]
                          + [(dim, -1) for dim in geobox.dimensions])

    def _resolve(k, v: Optional[Union[str, int]]) -> int:
        if v is None or v == "auto":
            v = _resolve(k, chunk_defaults[k])

        if isinstance(v, int):
            if v < 0:
                return chunk_maxsz[k]
            return v
        raise ValueError("Chunk should be one of int|'auto'")

    irr_chunks = tuple(_resolve(dim, dask_chunks.get(str(dim))) for dim in sources.dims)
    extra_dim_chunks = tuple(_resolve(dim, dask_chunks.get(str(dim))) for dim in extra_dim_names)
    grid_chunks = tuple(_resolve(dim, dask_chunks.get(str(dim))) for dim in geobox.dimensions)

    if extra_dim_chunks:
        return irr_chunks, extra_dim_chunks, grid_chunks
    else:
        return irr_chunks, grid_chunks


def _tokenize_dataset(dataset):
    return 'dataset-{}'.format(dataset.id.hex)


# pylint: disable=too-many-locals
def _make_dask_array(chunked_srcs,
                     dsk,
                     gbt,
                     measurement,
                     chunks,
                     skip_broken_datasets=False,
                     extra_dims=None,
                     patch_url=None):
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

    def _mk_empty(shape: Tuple[int, ...]) -> str:
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
                # 3D case
                if 'extra_dim' in measurement:
                    index_subset = extra_dims.measurements_index(measurement.extra_dim)
                    for result_index, extra_dim_index in numpy.ndenumerate(range(*index_subset)):
                        dsk[key_prefix + result_index + idx] = val
                else:
                    dsk[key_prefix + idx] = val
            else:
                val = (fuse_lazy,
                       [_tokenize_dataset(ds) for ds in dss],
                       gbt[idx],
                       measurement,
                       skip_broken_datasets,
                       len(needed_irr_chunks))

                # 3D case
                if 'extra_dim' in measurement:
                    # Do extra_dim subsetting here
                    index_subset = extra_dims.measurements_index(measurement.extra_dim)
                    for result_index, extra_dim_index in enumerate(range(*index_subset)):
                        dsk[key_prefix + (result_index,) + idx] = val + (extra_dim_index, patch_url)
                else:
                    # Get extra_dim index if available
                    extra_dim_index = measurement.get('extra_dim_index', None)
                    dsk[key_prefix + idx] = val + (extra_dim_index, patch_url)

    y_shapes = [grid_chunks[0]]*gbt.shape[0]
    x_shapes = [grid_chunks[1]]*gbt.shape[1]

    y_shapes[-1], x_shapes[-1] = gbt.chunk_shape(tuple(n-1 for n in gbt.shape))

    extra_dim_shape = ()
    if 'extra_dim' in measurement:
        dim_name = measurement.extra_dim
        extra_dim_shape += (len(extra_dims.measurements_values(dim_name)),)

    data = da.Array(dsk, dsk_name,
                    chunks=actual_irr_chunks + (tuple(y_shapes), tuple(x_shapes)),
                    dtype=measurement.dtype,
                    shape=(chunked_srcs.shape + extra_dim_shape + gbt.base.shape))

    if needed_irr_chunks != actual_irr_chunks:
        data = data.rechunk(chunks=chunks)
    return data
