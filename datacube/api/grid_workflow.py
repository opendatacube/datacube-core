
import logging
import numpy
import xarray
from itertools import groupby
from collections import OrderedDict
import warnings
import pandas as pd

from datacube.utils.geometry import intersects
from .query import Query, query_group_by
from .core import Datacube, apply_aliases

_LOG = logging.getLogger(__name__)


def _fast_slice(array, indexers):
    data = array.values[indexers]
    dims = [dim for dim, indexer in zip(array.dims, indexers) if isinstance(indexer, slice)]
    variable = xarray.Variable(dims, data, attrs=array.attrs, fastpath=True)
    coords = OrderedDict((dim,
                          xarray.Variable((dim,),
                                          array.coords[dim].values[indexer],
                                          attrs=array.coords[dim].attrs,
                                          fastpath=True))
                         for dim, indexer in zip(array.dims, indexers) if isinstance(indexer, slice))
    return xarray.DataArray(variable, coords=coords, fastpath=True)


class Tile(object):
    """
    The Tile object holds a lightweight representation of a datacube result.

    It is produced by :meth:`.GridWorkflow.list_cells` or :meth:`.GridWorkflow.list_tiles`.

    The Tile object can be passed to :meth:`GridWorkflow.load` to be loaded into memory as
    an :class:`xarray.Dataset`.

    A portion of a tile can be created by using index notation. eg:

        tile[0:1, 0:1000, 0:1000]

    This can be used to load small portions of data into memory, instead of having to access
    the entire `Tile` at once.
    """

    def __init__(self, sources, geobox):
        """Create a Tile representing a dataset that can be loaded.

        :param xarray.DataArray sources: An array of non-spatial dimensions of the request, holding lists of
            datacube.storage.DatasetSource objects.
        :param model.GeoBox geobox: The spatial footprint of the Tile
        """
        self.sources = sources
        self.geobox = geobox

    @property
    def dims(self):
        """Names of the dimensions, eg ``('time', 'y', 'x')``
        :return: tuple(str)
        """
        return self.sources.dims + self.geobox.dimensions

    @property
    def shape(self):
        """Lengths of each dimension, eg ``(285, 4000, 4000)``
        :return: tuple(int)
        """
        return self.sources.shape + self.geobox.shape

    @property
    def product(self):
        """
        :rtype: datacube.model.DatasetType
        """
        return self.sources.values[0][0].type

    def __getitem__(self, chunk):
        sources = _fast_slice(self.sources, chunk[:len(self.sources.shape)])
        geobox = self.geobox[chunk[len(self.sources.shape):]]
        return Tile(sources, geobox)

    # TODO(csiro) Split on time range
    def split(self, dim, step=1):
        """
        Splits along a non-spatial dimension into Tile objects with a length of 1 or more in the `dim` dimension.

        :param dim: Name of the non-spatial dimension to split
        :param step: step size to split
        :return: tuple(key, Tile)
        """
        axis = self.dims.index(dim)
        indexer = [slice(None)] * len(self.dims)
        size = self.sources[dim].size
        for i in range(0, size, step):
            indexer[axis] = slice(i, min(size, i + step))
            yield self.sources[dim].values[i], self[tuple(indexer)]

    def split_by_time(self, freq='A', time_dim='time', **kwargs):
        """
        Splits along the `time` dimension, into periods, using pandas offsets, such as:
        :
            'A': Annual
            'Q': Quarter
            'M': Month
        See: http://pandas.pydata.org/pandas-docs/stable/timeseries.html?highlight=rollback#timeseries-offset-aliases

        :param freq: time series frequency
        :param time_dim: name of the time dimension
        :param kwargs: other keyword arguments passed to ``pandas.period_range``
        :return: Generator[tuple(str, Tile)] generator of the key string (eg '1994') and the slice of Tile
        """
        start_range = self.sources[time_dim][0].data
        end_range = self.sources[time_dim][-1].data

        for p in pd.period_range(start=start_range,
                                 end=end_range,
                                 freq=freq,
                                 **kwargs):
            sources_slice = self.sources.loc[{time_dim: slice(p.start_time, p.end_time)}]
            yield str(p), Tile(sources=sources_slice, geobox=self.geobox)

    def __str__(self):
        return "Tile<sources={!r},\n\tgeobox={!r}>".format(self.sources, self.geobox)

    def __repr__(self):
        return self.__str__()


class GridWorkflow(object):
    """
    GridWorkflow deals with cell- and tile-based processing using a grid defining a projection and resolution.

    Use GridWorkflow to specify your desired output grid.  The methods :meth:`list_cells` and :meth:`list_tiles`
    query the index and return a dictionary of cell or tile keys, each mapping to a :class:`Tile` object.

    The :class:`.Tile` object can then be used to load the data without needing the index,
    and can be serialized for use with the `distributed` package.
    """

    def __init__(self, index, grid_spec=None, product=None):
        """
        Create a grid workflow tool.

        Either grid_spec or product must be supplied.

        :param datacube.index.Index index: The database index to use.
        :param GridSpec grid_spec: The grid projection and resolution
        :param str product: The name of an existing product, if no grid_spec is supplied.
        """
        self.index = index
        if grid_spec is None:
            product = self.index.products.get_by_name(product)
            grid_spec = product and product.grid_spec
        self.grid_spec = grid_spec

    def cell_observations(self, cell_index=None, geopolygon=None, tile_buffer=None, **indexers):
        """
        List datasets, grouped by cell.

        :param datacube.utils.Geometry geopolygon:
            Only return observations with data inside polygon.
        :param (float,float) tile_buffer:
            buffer tiles by (y, x) in CRS units
        :param (int,int) cell_index:
            The cell index. E.g. (14, -40)
        :param indexers:
            Query to match the datasets, see :py:class:`datacube.api.query.Query`
        :return: Datsets grouped by cell index
        :rtype: dict[(int,int), list[:py:class:`datacube.model.Dataset`]]

        .. seealso::
            :meth:`datacube.Datacube.find_datasets`

            :class:`datacube.api.query.Query`
        """
        # pylint: disable=too-many-locals
        # TODO: split this method into 3: cell/polygon/unconstrained querying

        if tile_buffer is not None and geopolygon is not None:
            raise ValueError('Cannot process tile_buffering and geopolygon together.')
        cells = {}

        def add_dataset_to_cells(tile_index, tile_geobox, dataset_):
            cells.setdefault(tile_index, {'datasets': [], 'geobox': tile_geobox})['datasets'].append(dataset_)

        if cell_index:
            assert len(cell_index) == 2
            cell_index = tuple(cell_index)
            geobox = self.grid_spec.tile_geobox(cell_index)
            geobox = geobox.buffered(*tile_buffer) if tile_buffer else geobox

            datasets, query = self._find_datasets(geobox.extent, indexers)
            for dataset in datasets:
                if intersects(geobox.extent, dataset.extent.to_crs(self.grid_spec.crs)):
                    add_dataset_to_cells(cell_index, geobox, dataset)
            return cells
        else:
            datasets, query = self._find_datasets(geopolygon, indexers)
            geobox_cache = {}

            if query.geopolygon:
                # Get a rough region of tiles
                query_tiles = set(
                    tile_index for tile_index, tile_geobox in
                    self.grid_spec.tiles_from_geopolygon(query.geopolygon, geobox_cache=geobox_cache))

                for dataset in datasets:
                    # Go through our datasets and see which tiles each dataset produces, and whether they intersect
                    # our query geopolygon.
                    dataset_extent = dataset.extent.to_crs(self.grid_spec.crs)
                    bbox = dataset_extent.boundingbox
                    bbox = bbox.buffered(*tile_buffer) if tile_buffer else bbox

                    for tile_index, tile_geobox in self.grid_spec.tiles(bbox, geobox_cache=geobox_cache):
                        if tile_index in query_tiles and intersects(tile_geobox.extent, dataset_extent):
                            add_dataset_to_cells(tile_index, tile_geobox, dataset)

            else:
                for dataset in datasets:
                    for tile_index, tile_geobox in self.grid_spec.tiles_from_geopolygon(dataset.extent,
                                                                                        tile_buffer=tile_buffer,
                                                                                        geobox_cache=geobox_cache):
                        add_dataset_to_cells(tile_index, tile_geobox, dataset)

            return cells

    def _find_datasets(self, geopolygon, indexers):
        query = Query(index=self.index, geopolygon=geopolygon, **indexers)
        if not query.product:
            raise RuntimeError('must specify a product')
        datasets = self.index.datasets.search_eager(**query.search_terms)
        return datasets, query

    @staticmethod
    def cell_sources(observations, group_by):
        warnings.warn("cell_sources() has been renamed to group_into_cells() and will eventually be removed",
                      DeprecationWarning)
        return GridWorkflow.group_into_cells(observations, group_by)

    @staticmethod
    def group_into_cells(observations, group_by):
        """
        Group observations into a stack of source tiles.

        :param observations: datasets grouped by cell index, like from :py:meth:`cell_observations`
        :param group_by: grouping method, as returned by :py:meth:`datacube.api.query.query_group_by`
        :type group_by: :py:class:`datacube.api.query.GroupBy`
        :return: tiles grouped by cell index
        :rtype: dict[(int,int), :class:`.Tile`]

        .. seealso::
            :meth:`load`

            :meth:`datacube.Datacube.group_datasets`
        """
        cells = {}
        for cell_index, observation in observations.items():
            sources = Datacube.group_datasets(observation['datasets'], group_by)
            cells[cell_index] = Tile(sources, observation['geobox'])
        return cells

    @staticmethod
    def tile_sources(observations, group_by):
        """
        Split observations into tiles and group into source tiles

        :param observations: datasets grouped by cell index, like from :meth:`cell_observations`
        :param group_by: grouping method, as returned by :py:meth:`datacube.api.query.query_group_by`
        :type group_by: :py:class:`datacube.api.query.GroupBy`
        :return: tiles grouped by cell index and time
        :rtype: dict[tuple(int, int, numpy.datetime64), :py:class:`.Tile`]

        .. seealso::
            :meth:`load`

            :meth:`datacube.Datacube.group_datasets`
        """
        tiles = {}
        for cell_index, observation in observations.items():
            observation['datasets'].sort(key=group_by.group_by_func)
            groups = [(key, tuple(group)) for key, group in groupby(observation['datasets'], group_by.group_by_func)]

            for key, datasets in groups:
                data = numpy.empty(1, dtype=object)
                data[0] = datasets
                variable = xarray.Variable((group_by.dimension,), data,
                                           fastpath=True)
                coord = xarray.Variable((group_by.dimension,),
                                        numpy.array([key], dtype='datetime64[ns]'),
                                        attrs={'units': group_by.units},
                                        fastpath=True)
                coords = OrderedDict([(group_by.dimension, coord)])
                sources = xarray.DataArray(variable, coords=coords, fastpath=True)

                tile_index = cell_index + (coord.values[0],)
                tiles[tile_index] = Tile(sources, observation['geobox'])
        return tiles

    def list_cells(self, cell_index=None, **query):
        """
        List cells that match the query.

        Returns a dictionary of cell indexes to :class:`.Tile` objects.

        Cells are included if they contain any datasets that match the query using the same format as
        :meth:`datacube.Datacube.load`.

        E.g.::

            gw.list_cells(product='ls5_nbar_albers',
                          time=('2001-1-1 00:00:00', '2001-3-31 23:59:59'))

        :param (int,int) cell_index: The cell index. E.g. (14, -40)
        :param query: see :py:class:`datacube.api.query.Query`
        :rtype: dict[(int, int), :class:`.Tile`]
        """
        observations = self.cell_observations(cell_index, **query)
        return self.group_into_cells(observations, query_group_by(**query))

    def list_tiles(self, cell_index=None, **query):
        """
        List tiles of data, sorted by cell.
        ::

            tiles = gw.list_tiles(product='ls5_nbar_albers',
                                  time=('2001-1-1 00:00:00', '2001-3-31 23:59:59'))

        The values can be passed to :meth:`load`

        :param (int,int) cell_index: The cell index (optional). E.g. (14, -40)
        :param query: see :py:class:`datacube.api.query.Query`
        :rtype: dict[(int, int, numpy.datetime64), :class:`.Tile`]

        .. seealso:: :meth:`load`
        """
        observations = self.cell_observations(cell_index, **query)
        return self.tile_sources(observations, query_group_by(**query))

    @staticmethod
    def load(tile, measurements=None, dask_chunks=None, fuse_func=None, resampling=None, skip_broken_datasets=False):
        """
        Load data for a cell/tile.

        The data to be loaded is defined by the output of :meth:`list_tiles`.

        This is a static function and does not use the index. This can be useful when running as a worker in a
        distributed environment and you wish to minimize database connections.

        See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
        for more information.

        :param `.Tile` tile: The tile to load.

        :param list(str) measurements: The names of measurements to load

        :param dict dask_chunks: If the data should be loaded as needed using :py:class:`dask.array.Array`,
            specify the chunk size in each output direction.

            See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
            for more information.

        :param fuse_func: Function to fuse together a tile that has been pre-grouped by calling
            :meth:`list_cells` with a ``group_by`` parameter.

        :param str|dict resampling:

            The resampling method to use if re-projection is required, could be
            configured per band using a dictionary (:meth: `load_data`)

            Valid values are: ``'nearest', 'cubic', 'bilinear', 'cubic_spline', 'lanczos', 'average'``

            Defaults to ``'nearest'``.

        :param bool skip_broken_datasets: If True, ignore broken datasets and continue processing with the data
             that can be loaded. If False, an exception will be raised on a broken dataset. Defaults to False.

        :rtype: :py:class:`xarray.Dataset`

        .. seealso::
            :meth:`list_tiles` :meth:`list_cells`
        """
        measurement_dicts = tile.product.lookup_measurements(measurements)

        dataset = Datacube.load_data(tile.sources, tile.geobox,
                                     measurement_dicts, resampling=resampling,
                                     dask_chunks=dask_chunks, fuse_func=fuse_func,
                                     skip_broken_datasets=skip_broken_datasets)

        return apply_aliases(dataset, tile.product, measurements)

    def update_tile_lineage(self, tile):
        for i in range(tile.sources.size):
            sources = tile.sources.values[i]
            tile.sources.values[i] = tuple(self.index.datasets.get(dataset.id, include_sources=True)
                                           for dataset in sources)
        return tile

    def __str__(self):
        return "GridWorkflow<index={!r},\n\tgridspec={!r}>".format(self.index, self.grid_spec)

    def __repr__(self):
        return self.__str__()
