# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import typing
from collections import OrderedDict
from collections.abc import Generator, Hashable, Iterable

import numpy as np
import pandas as pd
import xarray as xr
from odc.geo.geom import Geometry, intersects
from odc.geo.gridspec import GridSpec
from typing_extensions import override

from datacube.model import Dataset, QueryField
from datacube.utils import DatacubeException

from .core import Datacube
from .query import GroupBy, Query, query_group_by

_LOG = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from odc.geo.geobox import GeoBox

    from datacube.index import Index
    from datacube.model import Product


class GridWorkflowException(DatacubeException):
    """An ODC Exception raised while building or running Grid Workflows"""


def _fast_slice(array: xr.DataArray, indexers):
    data = array.values[indexers]
    dims = [
        dim for dim, indexer in zip(array.dims, indexers) if isinstance(indexer, slice)
    ]
    coords = OrderedDict(
        (
            dim,
            xr.Variable(
                (dim,),
                array.coords[dim].values[indexer],
                attrs=array.coords[dim].attrs,
                fastpath=True,
            ),
        )
        for dim, indexer in zip(array.dims, indexers)
        if isinstance(indexer, slice)
    )
    return xr.DataArray(data, dims=dims, coords=coords, attrs=array.attrs)


class Tile:
    """
    The Tile object holds a lightweight representation of a datacube result.

    It is produced by :meth:`.GridWorkflow.list_cells` or :meth:`.GridWorkflow.list_tiles`.

    The Tile object can be passed to :meth:`GridWorkflow.load` to be loaded into memory as
    an :class:`xr.Dataset`.

    A portion of a tile can be created by using index notation. eg:

        tile[0:1, 0:1000, 0:1000]

    This can be used to load small portions of data into memory, instead of having to access
    the entire `Tile` at once.
    """

    def __init__(self, sources: xr.DataArray, geobox: GeoBox) -> None:
        """Create a Tile representing a dataset that can be loaded.

        :param sources: An array of non-spatial dimensions of the request, holding lists of
            datacube.storage.DatasetSource objects.
        :param geobox: The spatial footprint of the Tile
        """
        self.sources: xr.DataArray = sources
        self.geobox: GeoBox = geobox

    @property
    def dims(self) -> tuple[Hashable, ...]:
        """Names of the dimensions, eg ``('time', 'y', 'x')``"""
        return self.sources.dims + self.geobox.dimensions

    @property
    def shape(self) -> tuple[int, ...]:
        """Lengths of each dimension, eg ``(285, 4000, 4000)``"""
        return self.sources.shape + self.geobox.shape

    @property
    def product(self) -> Product:
        return self.sources.values[0][0].product

    def __getitem__(self, chunk):
        sources = _fast_slice(self.sources, chunk[: len(self.sources.shape)])
        geobox = self.geobox[chunk[len(self.sources.shape) :]]
        return Tile(sources, geobox)

    # TODO(csiro) Split on time range
    def split(self, dim: str, step: int = 1) -> Generator[tuple[str, Tile]]:
        """
        Splits along a non-spatial dimension into Tile objects with a length of 1 or more in the `dim` dimension.

        :param dim: Name of the non-spatial dimension to split
        :param step: step size to split
        """
        axis = self.dims.index(dim)
        indexer = [slice(None)] * len(self.dims)
        size = self.sources[dim].size
        for i in range(0, size, step):
            indexer[axis] = slice(i, min(size, i + step))
            yield self.sources[dim].values[i], self[tuple(indexer)]

    def split_by_time(
        self, freq: str = "A", time_dim: str = "time", **kwargs
    ) -> Generator[tuple[str, Tile]]:
        """
        Splits along the `time` dimension, into periods, using pandas offsets, such as:
        :
            'A': Annual
            'Q': Quarter
            'M': Month
        See: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects

        :param freq: time series frequency
        :param time_dim: name of the time dimension
        :param kwargs: other keyword arguments passed to ``pandas.period_range``
        :return:  generator of the key string (eg '1994') and the slice of Tile
        """
        # extract first and last timestamps from the time axis, note this will
        # work with 1 element arrays as well
        start_range, end_range = self.sources[time_dim].data[[0, -1]]

        for p in pd.period_range(start=start_range, end=end_range, freq=freq, **kwargs):
            sources_slice = self.sources.loc[
                {time_dim: slice(p.start_time, p.end_time)}
            ]
            yield str(p), Tile(sources=sources_slice, geobox=self.geobox)

    @override
    def __str__(self):
        return f"Tile<sources={self.sources!r},\n\tgeobox={self.geobox!r}>"

    @override
    def __repr__(self):
        return self.__str__()


class GridWorkflow:
    """
    GridWorkflow deals with cell- and tile-based processing using a grid defining a projection and resolution.

    Use GridWorkflow to specify your desired output grid.  The methods :meth:`list_cells` and :meth:`list_tiles`
    query the index and return a dictionary of cell or tile keys, each mapping to a :class:`Tile` object.

    The :class:`.Tile` object can then be used to load the data without needing the index,
    and can be serialized for use with the `distributed` package.
    """

    def __init__(
        self,
        index: Index,
        grid_spec: GridSpec | None = None,
        product: Product | str | None = None,
    ):
        """
        Create a grid workflow tool.

        Either grid_spec or product must be supplied.

        :param index: The database index to use.
        :param grid_spec: The grid projection and resolution
        :param product: The name of an existing product, if no grid_spec is supplied.
        """
        self.index = index

        # If available, use the provided grid_spec
        if grid_spec is not None:
            self.grid_spec: GridSpec = grid_spec
        else:
            # Otherwise, attempt to get the grid_spec by the provided product
            # which may or may not have one.
            if product is None:
                raise GridWorkflowException(
                    "Have to supply either grid_spec or product"
                )

            if isinstance(product, str):
                product = self.index.products.get_by_name(product)

            if product is None:
                raise GridWorkflowException("No such product", product)

            if product.grid_spec is None:
                raise GridWorkflowException(
                    "Supplied product does not have a gridspec", product
                )

            self.grid_spec = product.grid_spec

    def cell_observations(
        self,
        cell_index: tuple[int, int] | None = None,
        geopolygon: Geometry | None = None,
        tile_buffer: tuple[float, float] | None = None,
        **indexers: QueryField,
    ) -> dict[tuple[int, int], dict[str, Dataset | GeoBox]]:
        """
        List datasets, grouped by cell.

        :param geopolygon:
            Only return observations with data inside polygon.
        :param tile_buffer:
            buffer tiles by (y, x) in CRS units
        :param  cell_index:
            The cell index. E.g. (14, -40)
        :param indexers:
            Query to match the datasets, see :py:class:`datacube.api.query.Query`
        :return: A dictionary of cell index (int, int) mapping to a dict containing two keys,
          "datasets", with a list of datasets, and "geobox", containing the geobox for the cell.

        .. seealso::
            :meth:`datacube.Datacube.find_datasets`

            :class:`datacube.api.query.Query`
        """
        # pylint: disable=too-many-locals
        # TODO: split this method into 3: cell/polygon/unconstrained querying

        if tile_buffer is not None and geopolygon is not None:
            raise GridWorkflowException(
                "Cannot process tile_buffering and geopolygon together."
            )
        cells: dict[tuple[int, int], dict[str, Dataset | GeoBox]] = {}

        def add_dataset_to_cells(tile_index, tile_geobox, dataset_):
            cells.setdefault(tile_index, {"datasets": [], "geobox": tile_geobox})[
                "datasets"
            ].append(dataset_)

        if cell_index:
            geobox = self.grid_spec.tile_geobox(cell_index)
            geobox = geobox.buffered(*tile_buffer) if tile_buffer else geobox

            datasets, query = self._find_datasets(geobox.extent, indexers)
            for dataset in datasets:
                if intersects(geobox.extent, dataset.extent.to_crs(self.grid_spec.crs)):
                    add_dataset_to_cells(cell_index, geobox, dataset)
            return cells
        else:
            datasets, query = self._find_datasets(geopolygon, indexers)
            geobox_cache: dict[tuple[int, int], GeoBox] = {}

            if query.geopolygon:
                # Get a rough region of tiles
                query_tiles = {
                    tile_index
                    for tile_index, tile_geobox in self.grid_spec.tiles_from_geopolygon(
                        query.geopolygon, geobox_cache=geobox_cache
                    )
                }

                for dataset in datasets:
                    # Go through our datasets and see which tiles each dataset produces, and whether they intersect
                    # our query geopolygon.
                    dataset_extent = dataset.extent.to_crs(self.grid_spec.crs)
                    bbox = dataset_extent.boundingbox
                    bbox = bbox.buffered(*tile_buffer) if tile_buffer else bbox

                    for tile_index, tile_geobox in self.grid_spec.tiles(
                        bbox, geobox_cache=geobox_cache
                    ):
                        if tile_index in query_tiles and intersects(
                            tile_geobox.extent, dataset_extent
                        ):
                            add_dataset_to_cells(tile_index, tile_geobox, dataset)

            else:
                for dataset in datasets:
                    dataset_extent = (
                        dataset.extent.buffer(*tile_buffer)
                        if tile_buffer
                        else dataset.extent
                    )
                    for tile_index, tile_geobox in self.grid_spec.tiles_from_geopolygon(
                        dataset_extent, geobox_cache=geobox_cache
                    ):
                        if tile_buffer:
                            tile_geobox = tile_geobox.buffered(*tile_buffer)
                        add_dataset_to_cells(tile_index, tile_geobox, dataset)

            return cells

    def _find_datasets(self, geopolygon, indexers):
        query = Query(index=self.index, geopolygon=geopolygon, **indexers)
        if not query.product:
            raise RuntimeError("must specify a product")
        datasets = self.index.datasets.search_eager(**query.search_terms)
        return datasets, query

    @staticmethod
    def group_into_cells(
        observations, group_by: GroupBy
    ) -> dict[tuple[int, int], Tile]:
        """
        Group observations into a stack of source tiles.

        :param observations: datasets grouped by cell index, like from :py:meth:`cell_observations`
        :param group_by: grouping method, as returned by :py:meth:`datacube.api.query.query_group_by`
        :return: tiles grouped by cell index

        .. seealso::
            :meth:`load`

            :meth:`datacube.Datacube.group_datasets`
        """
        cells = {}
        for cell_index, observation in observations.items():
            sources = Datacube.group_datasets(observation["datasets"], group_by)
            cells[cell_index] = Tile(sources, observation["geobox"])
        return cells

    @staticmethod
    def tile_sources(
        observations, group_by: GroupBy
    ) -> dict[tuple[int, int, np.datetime64], Tile]:
        """
        Split observations into tiles and group into source tiles

        :param observations: datasets grouped by cell index, like from :meth:`cell_observations`
        :param group_by: grouping method, as returned by :py:meth:`datacube.api.query.query_group_by`
        :return: tiles grouped by cell index and time

        .. seealso::
            :meth:`load`

            :meth:`datacube.Datacube.group_datasets`
        """
        tiles = {}
        for cell_index, observation in observations.items():
            dss = observation["datasets"]
            geobox = observation["geobox"]

            sources = Datacube.group_datasets(dss, group_by)
            coord = sources[sources.dims[0]]
            for i in range(coord.size):
                tile_index = cell_index + (coord.values[i],)
                tiles[tile_index] = Tile(sources[i : i + 1], geobox)

        return tiles

    def list_cells(
        self, cell_index: tuple[int, int] | None = None, **query
    ) -> dict[tuple[int, int], Tile]:
        """
        List cells that match the query.

        Returns a dictionary of cell indexes to :class:`.Tile` objects.

        Cells are included if they contain any datasets that match the query using the same format as
        :meth:`datacube.Datacube.load`.

        E.g.::

            gw.list_cells(product='ls5_nbar_albers',
                          time=('2001-1-1 00:00:00', '2001-3-31 23:59:59'))

        :param cell_index: The cell index. E.g. (14, -40)
        :param query: see :py:class:`datacube.api.query.Query`
        """
        observations = self.cell_observations(cell_index, **query)
        return self.group_into_cells(observations, query_group_by(**query))

    def list_tiles(
        self, cell_index: tuple[int, int] | None = None, **query
    ) -> dict[tuple[int, int, np.datetime64], Tile]:
        """
        List tiles of data, sorted by cell.
        ::

            tiles = gw.list_tiles(product='ls5_nbar_albers',
                                  time=('2001-1-1 00:00:00', '2001-3-31 23:59:59'))

        The values can be passed to :meth:`load`

        :param cell_index: The cell index (optional). E.g. (14, -40)
        :param query: see :py:class:`datacube.api.query.Query`

        .. seealso:: :meth:`load`
        """
        observations = self.cell_observations(cell_index, **query)
        return self.tile_sources(observations, query_group_by(**query))

    @staticmethod
    def load(
        tile: Tile,
        measurements: Iterable[str] | None = None,
        dask_chunks: dict[str, str | int] | None = None,
        fuse_func=None,
        resampling: str | dict | None = None,
        skip_broken_datasets: bool = False,
    ) -> xr.Dataset:
        """
        Load data for a cell/tile.

        The data to be loaded is defined by the output of :meth:`list_tiles`.

        This is a static function and does not use the index. This can be useful when running as a worker in a
        distributed environment and you wish to minimize database connections.

        See the documentation on using `xr with dask <http://xr.pydata.org/en/stable/dask.html>`_
        for more information.

        :param tile: The tile to load.

        :param measurements: The names of measurements to load

        :param dask_chunks: If the data should be loaded as needed using :py:class:`dask.array.Array`,
            specify the chunk size in each output direction.

            See the documentation on using `xr with dask <http://xr.pydata.org/en/stable/dask.html>`_
            for more information.

        :param fuse_func: Function to fuse together a tile that has been pre-grouped by calling
            :meth:`list_cells` with a ``group_by`` parameter.

        :param resampling:

            The resampling method to use if re-projection is required, could be
            configured per band using a dictionary (:meth: `load_data`)

            Valid values are: ``'nearest', 'cubic', 'bilinear', 'cubic_spline', 'lanczos', 'average'``

            Defaults to ``'nearest'``.

        :param skip_broken_datasets: If True, ignore broken datasets and continue processing with the data
             that can be loaded. If False, an exception will be raised on a broken dataset. Defaults to False.


        .. seealso::
            :meth:`list_tiles` :meth:`list_cells`
        """
        measurement_dicts = tile.product.lookup_measurements(measurements)

        dataset = Datacube.load_data(
            tile.sources,
            tile.geobox,
            measurement_dicts,
            resampling=resampling,
            dask_chunks=dask_chunks,
            fuse_func=fuse_func,
            skip_broken_datasets=skip_broken_datasets,
        )

        return dataset

    def update_tile_lineage(self, tile: Tile):
        for i in range(tile.sources.size):
            sources = tile.sources.values[i]
            tile.sources.values[i] = tuple(
                self.index.datasets.get(dataset.id, include_sources=True)
                for dataset in sources
            )
        return tile

    @override
    def __str__(self):
        return f"GridWorkflow<index={self.index!r},\n\tgridspec={self.grid_spec!r}>"

    @override
    def __repr__(self):
        return self.__str__()
