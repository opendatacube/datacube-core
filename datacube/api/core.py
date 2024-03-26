# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
import uuid
import collections.abc
from itertools import groupby
from typing import Any, Iterable, cast, Callable, Hashable, Mapping, Sequence
import datetime

import deprecat
import numpy
import xarray
from dask import array as da

from datacube.cfg import GeneralisedRawCfg, GeneralisedCfg, GeneralisedEnv, ODCConfig
from datacube.storage import reproject_and_fuse, BandInfo
from datacube.utils import ignore_exceptions_if
from odc.geo import CRS, yx_, res_, resyx_, Resolution, XY
from odc.geo.xr import xr_coords
from datacube.utils.dates import normalise_dt
from odc.geo.geom import intersects, box, bbox_union, Geometry
from odc.geo.geobox import GeoBox, GeoboxTiles
from datacube.model import ExtraDimensions, ExtraDimensionSlices, Dataset, Measurement, GridSpec
from datacube.model.utils import xr_apply

from .query import Query, query_group_by, query_geopolygon, GroupBy
from ..index import index_connect, Index
from ..drivers import new_datasource
from ..index.abstract import QueryField
from ..migration import ODC2DeprecationWarning
from ..storage._load import ProgressFunction, FuserFunction

_LOG = logging.getLogger(__name__)


# Either a Pandas dataframe or a list of flat dictionaries.
# Pandas is loaded dynamically, so cannot be statically typed: use DataFrameLike | Any
DataFrameLike = list[dict[str, str | int | float | None]]


class TerminateCurrentLoad(Exception):  # noqa: N818
    """ This exception is raised by user code from `progress_cbk`
        to terminate currently running `.load`
    """
    pass


class Datacube:
    """
    Interface to search, read and write a datacube.

    :type index: datacube.index.index.Index
    """

    def __init__(self,
                 index: Index | None = None,
                 config: GeneralisedCfg | None = None,
                 env: GeneralisedEnv | None = None,
                 raw_config: GeneralisedRawCfg | None = None,
                 app: str | None = None,
                 validate_connection: bool = True) -> None:
        """
        Create an interface for the query and storage access.

        :param index: The database index to use. If provided, config, app, env and raw_config should all be None.

        :param config: One of:
            - None (Use provided ODCEnvironment or Index, or perform default config loading.)
            - An ODCConfig object
            - A file system path pointing to the location of the config file.
            - A list of file system paths to search for config files. The first readable file found will be used.
            If an index or an explicit ODCEnvironment is supplied, config and raw_config should be None.

        :param str env: The datacube environment to use.
            Either an explicit ODCEnvironment object, or a str which is a section name in the loaded config file.

            Defaults to 'default'. Falls back to 'datacube' with a deprecation warning if config file does not
            contain a 'default' section.

            Allows you to have multiple datacube instances in one configuration, specified on load,
            eg. 'dev', 'test' or 'landsat', 'modis' etc.

            If env is an ODCEnvironment object, config and index should both None.

        :param raw_config: Explicit configuration to use.  Either as a string (serialised in ini or yaml format) or
            a dictionary (deserialised).  If provided, config should be None.
            If an index or an explicit ODCEnvironment is supplied, config and raw_config should be None.

        :param app: A short, alphanumeric name to identify this application.

            The application name is used to track down problems with database queries, so it is strongly
            advised that be used.  Should be None if an index is supplied.

        :param bool validate_connection: Should we check that the database connection is available and valid.
            Defaults to True. Ignored if index is passed.
        """

        # Validate arguments

        if index is not None:
            # If an explicit index is provided, all other index-creation arguments should be None.
            should_be_none: list[str] = []
            if config is not None:
                should_be_none.append("config")
            if raw_config is not None:
                should_be_none.append("raw_config")
            if app is not None:
                should_be_none.append("app")
            if env is not None:
                should_be_none.append("env")
            if should_be_none:
                raise ValueError(
                    f"When an explicit index is provided, these arguments should be None: {','.join(should_be_none)}"
                )
            # Explicit index passed in?  Use it.
            self.index = index
            return

        # Obtain an ODCEnvironment object:
        cfg_env = ODCConfig.get_environment(env=env, config=config, raw_config=raw_config)

        self.index = index_connect(cfg_env,
                                   application_name=app,
                                   validate_connection=validate_connection)

    def list_products(self, with_pandas: bool = True, dataset_count: bool = False) -> DataFrameLike | Any:
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
            Return the list as a Pandas DataFrame. Defaults to True.  If False, return a list of dicts.

        :param bool dataset_count:
            Return a "dataset_count" column containing the number of datasets
            for each product. This can take several minutes on large datacubes.
            Defaults to False.

        :return: A table or list of every product in the datacube.
        :rtype: pandas.DataFrame or list(dict)
        """
        def _get_non_default(product, col):
            load_hints = product.load_hints()
            if load_hints:
                if col == 'crs':
                    return load_hints.get('output_crs', None)
                return load_hints.get(col, None)
            return getattr(product.grid_spec, col, None)

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
            if getattr(pr, col, None) or 'default' not in col
            # else get crs and resolution from load_hints or grid_spec
            # as per output_geobox() handling logic
            else _get_non_default(pr, col.replace('default_', ''))
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

    @deprecat.deprecat(
        deprecated_args={
            "show_archived": {
                "reason": "The show_archived argument has never done anything and will be removed in future.",
                "version": "1.9.0",
                "category": ODC2DeprecationWarning
            }
        }
    )
    def list_measurements(self, show_archived: bool = False, with_pandas: bool = True) -> DataFrameLike | Any:
        """
        List measurements for each product

        :param with_pandas: return the list as a Pandas DataFrame, otherwise as a list of dict. (defaults to True)
        :rtype: pandas.DataFrame or list(dict)
        """
        measurements = self._list_measurements()
        if not with_pandas:
            return measurements

        import pandas
        return pandas.DataFrame.from_dict(measurements).set_index(['product', 'measurement'])

    def _list_measurements(self) -> list[dict[str, Any]]:
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
    def load(self,
             product: str | None = None,
             measurements: str | list[str] | None = None,
             output_crs: Any = None,
             resolution: int | float | tuple[int | float, int | float] | Resolution | None = None,
             resampling: str | dict[str, str] | None = None,
             align: XY[float] | Iterable[float] | None = None,
             skip_broken_datasets: bool = False,
             dask_chunks: dict[str, str | int] | None = None,
             like: GeoBox | xarray.Dataset | xarray.DataArray | None = None,
             fuse_func: FuserFunction | Mapping[str, FuserFunction | None] | None = None,
             datasets: Sequence[Dataset] | None = None,
             dataset_predicate: Callable[[Dataset], bool] | None = None,
             progress_cbk: ProgressFunction | None = None,
             patch_url: Callable[[str], str] | None = None,
             limit: int | None = None,
             **query: QueryField):
        r"""
        Load data as an ``xarray.Dataset`` object.
        Each measurement will be a data variable in the :class:`xarray.Dataset`.

        See the `xarray documentation <http://xarray.pydata.org/en/stable/data-structures.html>`_ for usage of the
        :class:`xarray.Dataset` and :class:`xarray.DataArray` objects.

        **Product and Measurements**
            A product can be specified using the product name.
            ::

                product='ls5_ndvi_albers'

            See :meth:`list_products` for the list of products with their names and properties.

            A product name MUST be supplied unless search is bypassed all together by supplying an explicit
            list of datasets.

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


            You can also specify a polygon with an arbitrary CRS (in e.g. the native CRS)::

                geopolygon=polygon(coords, crs="EPSG:3577")

            Performance and accuracy of geopolygon queries may vary depending on the index driver in use and the CRS.

            The ``time`` dimension can be specified using a single or tuple of datetime objects or strings with
            ``YYYY-MM-DD hh:mm:ss`` format. Data will be loaded inclusive of the start and finish times.
            A ``None`` value in the range indicates an open range, with the provided date serving as either the
            upper or lower bound. E.g::

                time=('2000-01-01', '2001-12-31')
                time=('2000-01', '2001-12')
                time=('2000', '2001')
                time=('2000')
                time=('2000', None)  # all data from 2000 onward
                time=(None, '2000')  # all data up to and including 2000

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
                        resolution=30,
                        resampling='cubic'
                )

            odc-geo style xy objects are preferred for passing in resolution and align pairs to avoid x/y ordering
            ambiguity.

        :param str product:
            The name of the product to be loaded. Either ``product`` or ``datasets`` must be supplied

        :param measurements:
            Measurements name or list of names to be included, as listed in :meth:`list_measurements`.
            These will be loaded as individual ``xr.DataArray`` variables in
            the output ``xarray.Dataset`` object.

            If a list is specified, the measurements will be returned in the order requested.
            By default all available measurements are included.

        :param str output_crs:
            The CRS of the returned data, for example ``EPSG:3577``.
            If no CRS is supplied, the CRS of the stored data is used if available.

            Any form that can be converted to a CRS by odc-geo is accepted.

            This differs from the ``crs`` parameter desribed above, which is used to define the CRS
            of the coordinates in the query itself.

        :param resolution:
            The spatial resolution of the returned data. If using square pixels with an inverted Y axis, it
            should be provided as an int or float. If not, it should be provided as an odc-geo XY object
            to avoid coordinate-order ambiguity.  If passed as a tuple, y,x order is assumed for backwards
            compatibility.

            Units are in the coordinate space of ``output_crs``. This includes the direction (as indicated by
            a positive or negative number).

        :param resampling:
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

        :param align:
            Load data such that point 'align' lies on the pixel boundary.  A pair of floats between 0 and 1.

            An odc-geo XY object is preferred to avoid coordinate-order ambiguity.  If passed as a tuple, x,y
            order is assumed for backwards compatibility.

            Default is ``(0, 0)``

        :param bool skip_broken_datasets:
            Optional. If this is True, then don't break when failing to load a broken dataset.
            Default is False.

        :param dict dask_chunks:
            If the data should be lazily loaded using :class:`dask.array.Array`,
            specify the chunking size in each output dimension.

            See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
            for more information.

        :param xarray.Dataset like:
            Use the output of a previous :meth:`load()` to load data into the same spatial grid and
            resolution (i.e. :class:`odc.geo.geobox.GeoBox` or an xarray `Dataset` or `DataArray`).
            E.g.::

                pq = dc.load(product='ls5_pq_albers', like=nbar_dataset)

        :param fuse_func:
            Function used to fuse/combine/reduce data with the ``group_by`` parameter. By default,
            data is simply copied over the top of each other in a relatively undefined manner. This function can
            perform a specific combining step. This can be a dictionary if different
            fusers are needed per band (similar format to the resampling dict described above).

        :param group_by:
            When specified, perform basic combining/reducing of the data. For example, ``group_by='solar_day'``
            can be used to combine consecutive observations along a single satellite overpass into a single time slice.

            See also :class:`datacube.api.query.GroupBy`

        :param datasets:
            Optional. If this is a non-empty list of :class:`datacube.model.Dataset` objects, these will be loaded
            instead of performing a database lookup.

        :param dataset_predicate:
            Optional. A function that can be passed to restrict loaded datasets. A predicate function should
            take a :class:`datacube.model.Dataset` object (e.g. as returned from :meth:`find_datasets`) and
            return a boolean.
            For example, loaded data could be filtered to January observations only by passing the following
            predicate function that returns True for datasets acquired in January::

                def filter_jan(dataset): return dataset.time.begin.month == 1

        :param progress_cbk:
            ``Int, Int -> None``,
            if supplied will be called for every file read with ``files_processed_so_far, total_files``. This is
            only applicable to non-lazy loads, ignored when using dask.

        :param patch_url:
            if supplied, will be used to patch/sign the url(s), as required to access some commercial archives
            (e.g. Microsoft Planetary Computer).

        :param limit:
            Optional. If provided, limit the maximum number of datasets returned. Useful for testing and debugging.

        :param **query:
            Search parameters for products and dimension ranges as described above.
            For example: ``'x', 'y', 'time', 'crs'``.

        :return:
            Requested data in a :class:`xarray.Dataset`

        :rtype:
            :class:`xarray.Dataset`
        """
        if product is None and datasets is None:
            raise ValueError("Must specify a product or supply datasets")

        if datasets is None:
            assert product is not None   # For type checker
            datasets = self.find_datasets(ensure_location=True,
                                          dataset_predicate=dataset_predicate, like=like,
                                          limit=limit,
                                          product=product,
                                          **query)
        elif isinstance(datasets, collections.abc.Iterator):
            datasets = list(datasets)

        if len(datasets) == 0:
            return xarray.Dataset()

        ds, *_ = datasets
        datacube_product = ds.product

        # Retrieve extra_dimension from product definition
        extra_dims: ExtraDimensions | None = None
        if datacube_product:
            extra_dims = datacube_product.extra_dimensions

            # Extract extra_dims slice information
            extra_dims_slice = cast(ExtraDimensionSlices, {
                k: query.pop(k, None)
                for k in list(query.keys())
                if k in extra_dims.dims and query.get(k, None) is not None
            })
            extra_dims = extra_dims[extra_dims_slice]
            # Check if empty
            if extra_dims.has_empty_dim():
                return xarray.Dataset()

        if type(resolution) is tuple:
            _LOG.warning("Resolution should be provided as a single int or float, or the axis order specified "
                         "using odc.geo.resxy_ or odc.geo.resyx_")
            if resolution[0] == -resolution[1]:
                resolution = res_(resolution[1])
            else:
                _LOG.warning("Assuming resolution has been provided in (y, x) ordering. Please specify the order "
                             "with odc.geo.resxy_ or odc.geo.resyx_")
                resolution = resyx_(*resolution)

        geobox = output_geobox(like=like, output_crs=output_crs, resolution=resolution, align=align,
                               grid_spec=datacube_product.grid_spec,
                               load_hints=datacube_product.load_hints(),
                               datasets=datasets, geopolygon=None,
                               **query)
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

    def find_datasets(self,
                      ensure_location: bool = False,
                      dataset_predicate: Callable[[Dataset], bool] | None = None,
                      like: GeoBox | xarray.Dataset | xarray.DataArray | None = None,
                      limit: int | None = None,
                      **search_terms: QueryField) -> list[Dataset]:
        """
        Search the index and return all datasets for a product matching the search terms.

        :param ensure_location: only return datasets that have locations
        :param dataset_predicate: an optional predicate to filter datasets
        :param xarray.Dataset like:
            Use the output of a previous :meth:`load()` to load data into the same spatial grid and
            resolution (i.e. :class:`odc.geo.geobox.GeoBox` or an xarray `Dataset` or `DataArray`).
            E.g.::

                pq = dc.load(product='ls5_pq_albers', like=nbar_dataset)

        :param limit: if provided, limit the maximum number of datasets returned
        :param search_terms: see :class:`datacube.api.query.Query`
        :return: list of datasets

        .. seealso:: :meth:`group_datasets` :meth:`load_data` :meth:`find_datasets_lazy`
        """
        return list(self.find_datasets_lazy(limit=limit, ensure_location=ensure_location,
                                            dataset_predicate=dataset_predicate, like=like,
                                            **search_terms))  # type: ignore[arg-type]

    def find_datasets_lazy(self,
                           limit: int | None = None,
                           ensure_location: bool = False,
                           dataset_predicate: Callable[[Dataset], bool] | None = None,
                           like: GeoBox | xarray.Dataset | xarray.DataArray | None = None,
                           **kwargs: QueryField) -> Iterable[Dataset]:
        """
        Find datasets matching query.

        :param limit: if provided, limit the maximum number of datasets returned
        :param ensure_location: only return datasets that have locations
        :param dataset_predicate: an optional predicate to filter datasets
        :param xarray.Dataset like:
            Use the output of a previous :meth:`load()` to load data into the same spatial grid and
            resolution (i.e. :class:`odc.geo.geobox.GeoBox` or an xarray `Dataset` or `DataArray`).
            E.g.::

                pq = dc.load(product='ls5_pq_albers', like=nbar_dataset)
        :param kwargs: see :class:`datacube.api.query.Query`
        :return: iterator of datasets
        :rtype: __generator[:class:`datacube.model.Dataset`]

        .. seealso:: :meth:`group_datasets` :meth:`load_data` :meth:`find_datasets`
        """
        query = Query(self.index, like=like, **kwargs)
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
    def group_datasets(datasets: Iterable[Dataset], group_by: GroupBy) -> xarray.DataArray:
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

        def ds_sorter(ds: Dataset) -> Any:
            return group_by.sort_key(ds), getattr(ds, 'id', 0)

        def norm_axis_value(x: Any) -> Any:
            if isinstance(x, datetime.datetime):
                # For datetime we convert to UTC, then strip timezone info
                # to avoid numpy/pandas warning about timezones
                return numpy.datetime64(normalise_dt(x), 'ns')
            return x

        def mk_group(group: Iterable[Dataset]) -> tuple[Any, Iterable[Dataset]]:
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
    def create_storage(coords: Mapping[str, xarray.DataArray],
                       geobox: GeoBox,
                       measurements: list[Measurement],
                       data_func: Callable[[Measurement, tuple[int, ...]], numpy.ndarray] | None = None,
                       extra_dims: ExtraDimensions | None = None) -> xarray.Dataset:
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
            allocated and filled with `nodata` value defined on a given Measurement.

        :param ExtraDimensions extra_dims:
            A ExtraDimensions describing any additional dimensions on top of (t, y, x)

        :rtype: :class:`xarray.Dataset`

        .. seealso:: :meth:`find_datasets` :meth:`group_datasets`
        """
        from collections import OrderedDict
        from copy import deepcopy
        spatial_ref = 'spatial_ref'

        def empty_func(m: Measurement, shape: tuple[int, ...]) -> numpy.ndarray:
            return numpy.full(shape, m.nodata, dtype=m.dtype)

        crs_attrs = {}
        if geobox.crs is not None:
            crs_attrs['crs'] = str(geobox.crs)
            crs_attrs['grid_mapping'] = spatial_ref

        # Assumptions
        #  - 3D dims must fit between (t) and (y, x) or (lat, lon)

        # 2D defaults
        # retrieve dims from coords if DataArray
        dims_default = cast(tuple[Hashable, ...], tuple())
        if coords != {}:
            coords_value = next(iter(coords.values()))
            if isinstance(coords_value, xarray.DataArray):
                dims_default = coords_value.dims + geobox.dimensions

        if not dims_default:
            dims_default = tuple(coords) + geobox.dimensions

        shape_default = tuple(c.size for k, c in coords.items() if k in dims_default) + geobox.shape
        coords_default: OrderedDict[str, xarray.DataArray] = OrderedDict(**coords, **xr_coords(geobox, spatial_ref))

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

        def mk_data_var(m: Measurement,
                        shape: tuple[int, ...],
                        coords: OrderedDict[str, xarray.DataArray],
                        dims: tuple[Hashable, ...],
                        data_func: Callable[[Measurement, tuple[int, ...]], numpy.ndarray]) -> xarray.DataArray:
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
    def _dask_load(sources: xarray.DataArray,
                   geobox: GeoBox,
                   measurements: list[Measurement],
                   dask_chunks: dict[str, str | int],
                   skip_broken_datasets: bool = False,
                   extra_dims: ExtraDimensions | None = None,
                   patch_url: Callable[[str], str] | None = None) -> xarray.Dataset:
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

        return Datacube.create_storage(cast(Mapping[str, xarray.DataArray], sources.coords),
                                       geobox, measurements, data_func, extra_dims)

    @staticmethod
    def _xr_load(sources: xarray.DataArray, geobox: GeoBox, measurements: list[Measurement],
                 skip_broken_datasets: bool = False,
                 progress_cbk: ProgressFunction | None = None,
                 extra_dims: ExtraDimensions | None = None,
                 patch_url: Callable[[str], str] | None = None) -> xarray.Dataset:

        def mk_cbk(cbk: ProgressFunction | None) -> ProgressFunction | None:
            if cbk is None:
                return None
            n = 0
            t_size = sum(len(x) for x in sources.values.ravel())
            n_total = 0
            for m in measurements:
                if 'extra_dim' in m:
                    assert extra_dims is not None   # for type-checker
                    index_subset = extra_dims.measurements_slice(m.extra_dim)
                    n_total += t_size*len(m.extra_dim.get('measurement_map')[index_subset])
                else:
                    n_total += t_size

            def _cbk(*ignored):
                nonlocal n
                n += 1
                return cbk(n, n_total)
            return _cbk

        data = Datacube.create_storage(cast(Mapping[str, xarray.DataArray], sources.coords),
                                       geobox, measurements, extra_dims=extra_dims)
        _cbk = mk_cbk(progress_cbk)

        # Create a list of read IO operations
        read_ios = []
        for index, datasets in numpy.ndenumerate(sources.values):
            for m in measurements:
                if 'extra_dim' in m:
                    # When we want to support 3D native reads, we can start by replacing the for loop with
                    # read_ios.append(((index + extra_dim_index), (datasets, m, index_subset)))
                    assert extra_dims is not None   # for type-checker
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
    def load_data(sources: xarray.DataArray, geobox: GeoBox,
                  measurements: Mapping[str, Measurement] | list[Measurement],
                  resampling: str | dict[str, str] | None = None,
                  fuse_func: FuserFunction | Mapping[str, FuserFunction | None] | None = None,
                  dask_chunks: dict[str, str | int] | None = None,
                  skip_broken_datasets: bool = False,
                  progress_cbk: ProgressFunction | None = None,
                  extra_dims: ExtraDimensions | None = None,
                  patch_url: Callable[[str], str] | None = None,
                  **extra) -> xarray.Dataset:
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


def per_band_load_data_settings(measurements: list[Measurement] | Mapping[str, Measurement],
                                resampling: str | Mapping[str, str] | None = None,
                                fuse_func: FuserFunction | Mapping[str, FuserFunction | None] | None = None
                                ) -> list[Measurement]:
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

    if fuse_func is None or callable(fuse_func):
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


def output_geobox(like: GeoBox | xarray.Dataset | xarray.DataArray | None = None,
                  output_crs: Any = None,
                  resolution: int | float | tuple[int | float, int | float] | Resolution | None = None,
                  align: XY[float] | Iterable[float] | None = None,
                  grid_spec: GridSpec | None = None,
                  load_hints: Mapping[str, Any] | None = None,
                  datasets: Iterable[Dataset] | None = None,
                  geopolygon: Geometry | None = None,
                  **query: QueryField) -> GeoBox:
    """ Configure output geobox from user provided output specs. """

    if like is not None:
        assert output_crs is None, "'like' and 'output_crs' are not supported together"
        assert resolution is None, "'like' and 'resolution' are not supported together"
        assert align is None, "'like' and 'align' are not supported together"
        if isinstance(like, GeoBox):
            return like

        return like.odc.geobox

    if load_hints:
        if output_crs is None:
            output_crs = load_hints.get('output_crs', None)

        if resolution is None:
            resolution = cast(int | float | tuple[int | float, int | float] | None, load_hints.get('resolution', None))

        if align is None:
            align = load_hints.get('align', None)

    if output_crs is not None:
        if resolution is None:
            raise ValueError("Must specify 'resolution' when specifying 'output_crs'")
        crs = CRS(output_crs)
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

    if type(resolution) is tuple:
        _LOG.warning("Resolution should be provided as a single int or float, or the axis order specified "
                     "using odc.geo.resxy_ or odc.geo.resyx_")
        if resolution[0] == -resolution[1]:
            resolution = resolution[1]
        else:
            _LOG.warning("Assuming resolution has been provided in (y, x) ordering. Please specify the order "
                         "with odc.geo.resxy_ or odc.geo.resyx_")
            resolution = resyx_(*resolution)
    resolution = res_(cast(Resolution | int | float, resolution))

    if align is not None:
        align = yx_(align)

    return GeoBox.from_geopolygon(geopolygon, resolution, crs, align)


def select_datasets_inside_polygon(datasets: Iterable[Dataset], polygon: Geometry) -> Iterable[Dataset]:
    # Check against the bounding box of the original scene, can throw away some portions
    assert polygon is not None
    query_crs = polygon.crs
    for dataset in datasets:
        if intersects(polygon, dataset.extent.to_crs(query_crs)):
            yield dataset


def fuse_lazy(datasets: Iterable[Dataset], geobox: GeoBox, measurement: Measurement,
              skip_broken_datasets: bool = False, prepend_dims: int = 0,
              extra_dim_index: int | None = None,
              patch_url: Callable[[str], str] | None = None) -> numpy.ndarray:
    prepend_shape = (1,) * prepend_dims
    data = numpy.full(geobox.shape, measurement.nodata, dtype=measurement.dtype)
    _fuse_measurement(data, datasets, geobox, measurement,
                      skip_broken_datasets=skip_broken_datasets,
                      extra_dim_index=extra_dim_index,
                      patch_url=patch_url)
    return data.reshape(prepend_shape + geobox.shape)


def _fuse_measurement(dest: numpy.ndarray, datasets: Iterable[Dataset], geobox: GeoBox, measurement: Measurement,
                      skip_broken_datasets: bool = False,
                      progress_cbk: ProgressFunction | None = None,
                      extra_dim_index: int | None = None,
                      patch_url: Callable[[str], str] | None = None) -> None:
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


def get_bounds(datasets: Iterable[Dataset], crs: CRS) -> Geometry:
    bbox = bbox_union(ds.extent.to_crs(crs).boundingbox for ds in datasets)
    return box(*bbox, crs=crs)  # type: ignore[misc]


def _calculate_chunk_sizes(sources: xarray.DataArray,
                           geobox: GeoBox,
                           dask_chunks: dict[str, str | int],
                           extra_dims: ExtraDimensions | None = None) -> tuple[tuple, ...]:
    extra_dim_names: tuple[str, ...] = ()
    extra_dim_shapes: tuple[int, ...] = ()
    if extra_dims is not None:
        extra_dim_names, extra_dim_shapes = extra_dims.chunk_size()

    valid_keys = sources.dims + extra_dim_names + geobox.dimensions
    bad_keys = cast(set[str], set(dask_chunks)) - cast(set[str], set(valid_keys))
    if bad_keys:
        raise KeyError('Unknown dask_chunk dimension {}. Valid dimensions are: {}'.format(bad_keys, valid_keys))

    chunk_maxsz = dict((dim, sz) for dim, sz in zip(sources.dims + extra_dim_names + geobox.dimensions,
                                                    sources.shape + extra_dim_shapes + geobox.shape))

    # defaults: 1 for non-spatial, whole dimension for Y/X
    chunk_defaults = dict([(dim, 1) for dim in sources.dims] + [(dim, 1) for dim in extra_dim_names]
                          + [(dim, -1) for dim in geobox.dimensions])

    def _resolve(k, v: str | int | None) -> int:
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


def _tokenize_dataset(dataset: Dataset) -> str:
    return 'dataset-{}'.format(dataset.id.hex)


# pylint: disable=too-many-locals
def _make_dask_array(chunked_srcs: xarray.DataArray,
                     dsk,
                     gbt,
                     measurement: Measurement,
                     chunks,
                     skip_broken_datasets: bool = False,
                     extra_dims: ExtraDimensions | None = None,
                     patch_url: Callable[[str], str] | None = None):
    dsk = dsk.copy()  # this contains mapping from dataset id to dataset object

    token = uuid.uuid4().hex
    dsk_name = 'dc_load_{name}-{token}'.format(name=measurement.name, token=token)

    needed_irr_chunks, grid_chunks = chunks[:-2], chunks[-2:]
    actual_irr_chunks = (1,) * len(needed_irr_chunks)

    # we can have up to 4 empty chunk shapes: whole, right edge, bottom edge and
    # bottom right corner
    #  W R
    #  B BR
    empties: dict[tuple[int, int], str] = {}

    def _mk_empty(shape: tuple[int, int]) -> str:
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
        for idx in numpy.ndindex(gbt.shape.shape):
            dss = tiled_dss.get(idx, None)

            if dss is None:
                val3d = _mk_empty(gbt.chunk_shape(idx).xy)
                # 3D case
                if 'extra_dim' in measurement:
                    assert extra_dims is not None  # For type checker
                    index_subset = extra_dims.measurements_index(measurement.extra_dim)
                    for result_index, extra_dim_index in numpy.ndenumerate(range(*index_subset)):
                        dsk[key_prefix + result_index + idx] = val3d
                else:
                    dsk[key_prefix + idx] = val3d
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
                    assert extra_dims is not None  # For type checker
                    index_subset = extra_dims.measurements_index(measurement.extra_dim)
                    for result_index, extra_dim_index in enumerate(range(*index_subset)):  # type: ignore[assignment]
                        dsk[key_prefix + (result_index,) + idx] = val + (extra_dim_index, patch_url)
                else:
                    # Get extra_dim index if available
                    extra_dim_index = measurement.get('extra_dim_index', None)
                    dsk[key_prefix + idx] = val + (extra_dim_index, patch_url)

    y_shapes = [grid_chunks[0]]*gbt.shape[0]
    x_shapes = [grid_chunks[1]]*gbt.shape[1]

    y_shapes[-1], x_shapes[-1] = gbt.chunk_shape(tuple(n-1 for n in gbt.shape))

    extra_dim_shape: tuple = ()
    if 'extra_dim' in measurement:
        assert extra_dims is not None  # For type checker
        dim_name = measurement.extra_dim
        extra_dim_shape += (len(extra_dims.measurements_values(dim_name)),)

    data = da.Array(dsk, dsk_name,
                    chunks=actual_irr_chunks + (tuple(y_shapes), tuple(x_shapes)),
                    dtype=measurement.dtype,
                    shape=(chunked_srcs.shape + extra_dim_shape + gbt.base.shape))

    if needed_irr_chunks != actual_irr_chunks:
        data = data.rechunk(chunks=chunks)
    return data
