from __future__ import absolute_import, division, print_function

import logging
import numpy
import xarray
from itertools import groupby
from collections import defaultdict, OrderedDict

from ..model import GeoBox
from ..utils import check_intersect
from .query import Query, query_group_by
from .core import Datacube, get_measurements, set_resampling_method

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
    def __init__(self, sources, geobox):
        self.sources = sources
        self.geobox = geobox

    @property
    def dims(self):
        return self.sources.dims + self.geobox.dimensions

    @property
    def shape(self):
        return self.sources.shape + self.geobox.shape

    def __getitem__(self, chunk):
        sources = _fast_slice(self.sources, chunk[:len(self.sources.shape)])
        geobox = self.geobox[chunk[len(self.sources.shape):]]
        return Tile(sources, geobox)

    def split(self, dim):
        axis = self.dims.index(dim)
        indexer = [slice(None)]*len(self.dims)
        for i in range(self.sources[dim].size):
            indexer[axis] = slice(i, i+1)
            yield self.sources[dim].values[i], self[tuple(indexer)]


class GridWorkflow(object):
    """
    GridWorkflow deals with cell- and tile-based processing using a grid defining a projection and resolution.
    """
    def __init__(self, index, grid_spec=None, product=None):
        """
        Create a grid workflow tool.

        Either grid_spec or product must be supplied.

        :param Index index: The database index to use.
        :param GridSpec grid_spec: The grid projection and resolution
        :param str product: The name of an existing product, if no grid_spec is supplied.
        """
        self.index = index
        if grid_spec is None:
            product = self.index.products.get_by_name(product)
            grid_spec = product and product.grid_spec
        self.grid_spec = grid_spec

    def cell_observations(self, cell_index=None, geopolygon=None, **indexers):
        """
        List datasets, grouped by cell.

        :param (int,int) cell_index: The cell index. E.g. (14, -40)
        :param indexers: Query to match the datasets, see :py:class:`datacube.api.query.Query`
        :return: Datsets grouped by cell index
        :rtype: dict[(int,int), list[:py:class:`datacube.model.Dataset`]]

        .. seealso::
            :meth:`datacube.Datacube.product_observations`

            :class:`datacube.api.query.Query`
        """
        if cell_index:
            assert isinstance(cell_index, tuple)
            assert len(cell_index) == 2
            geobox = GeoBox.from_grid_spec(self.grid_spec, cell_index)
            geopolygon = geobox.extent
        query = Query(index=self.index, geopolygon=geopolygon, **indexers)

        if not query.product:
            raise RuntimeError('must specify a product')

        observations = self.index.datasets.search_eager(**query.search_terms)
        if not observations:
            return {}

        tiles = {}
        if cell_index:
            tile_geopolygon = geobox.extent
            datasets = [dataset for dataset in observations
                        if check_intersect(tile_geopolygon, dataset.extent.to_crs(self.grid_spec.crs))]
            tiles[cell_index] = {
                'datasets': datasets,
                'geobox': geobox
            }
        else:
            for dataset in observations:
                dataset_extent = dataset.extent.to_crs(self.grid_spec.crs)
                for tile_index, tile_geobox in self.grid_spec.tiles(dataset_extent.boundingbox):
                    if check_intersect(tile_geobox.extent, dataset_extent):
                        tiles.setdefault(tile_index,
                                         {'datasets': [],
                                          'geobox': tile_geobox})['datasets'].append(dataset)
        return tiles

    @staticmethod
    def cell_sources(observations, group_by):
        """
        Group observations into sources

        :param observations: datasets grouped by cell index, like from :meth:`datacube.GridWorkflow.cell_observations`
        :param str group_by: grouping method, one of "time", "solar_day"
        :return: sources grouped by cell index
        :rtype: dict[(int,int), :py:class:`xarray.DataArray`]

        .. seealso::
            :meth:`load`

            :meth:`datacube.Datacube.product_sources`
        """
        cells = {}
        for cell_index, observation in observations.items():
            sources = Datacube.product_sources(observation['datasets'],
                                               group_func=group_by.group_by_func,
                                               dimension=group_by.dimension,
                                               units=group_by.units)
            cells[cell_index] = Tile(sources, observation['geobox'])
        return cells

    @staticmethod
    def tile_sources(observations, group_by):
        """
        Split observations into tiles and group into sources

        :param observations: datasets grouped by cell index, like from :meth:`datacube.GridWorkflow.cell_observations`
        :param str group_by: grouping method, one of "time", "solar_day"
        :return: sources grouped by cell index and time
        :rtype: dict[tuple(int, int, numpy.datetime64), :py:class:`xarray.DataArray`]

        .. seealso::
            :meth:`load`

            :meth:`datacube.Datacube.product_sources`
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

        Cells are included if they contain any datasets that match the query using the same format as
        :meth:`datacube.Datacube.load`.

        E.g.::

            gw.list_cells(product='ls5_nbar_albers',
                          time=('2001-1-1 00:00:00', '2001-3-31 23:59:59'))

        :param (int,int) cell_index: The cell index. E.g. (14, -40)
        :param query: see :py:class:`datacube.api.query.Query`
        :rtype: dict[(int, int), Tile]
        """
        observations = self.cell_observations(cell_index, **query)
        return self.cell_sources(observations, query_group_by(**query))

    def list_tiles(self, cell_index=None, **query):
        """
        List tiles of data, sorted by cell.
        ::

            tiles = gw.list_tiles(product='ls5_nbar_albers',
                                  time=('2001-1-1 00:00:00', '2001-3-31 23:59:59'))

        The values can be passed to :meth:`load`

        :param (int,int) cell_index: The cell index. E.g. (14, -40)
        :param query: see :py:class:`datacube.api.query.Query`
        :rtype: dict[(int, int, numpy.datetime64), Tile]

        .. seealso:: :meth:`load`
        """
        observations = self.cell_observations(cell_index, **query)
        return self.tile_sources(observations, query_group_by(**query))

    @staticmethod
    def load(tile, measurements=None, dask_chunks=None, fuse_func=None, resampling=None):
        """
        Load data for a cell/tile.

        The data to be loaded is defined by the output of :meth:`list_tiles`.

        See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
        for more information.

        :param tile: The tile to load.

        :param measurements: The name or list of names of measurements to load

        :param dict dask_chunks: If the data should be loaded as needed using :py:class:`dask.array.Array`,
            specify the chunk size in each output direction.

            See the documentation on using `xarray with dask <http://xarray.pydata.org/en/stable/dask.html>`_
            for more information.

        :param fuse_func: Function to fuse together a tile that has been pre-grouped by calling
            :meth:`list_cells` with a ``group_by`` parameter.

        :param str resampling: The resampling method to use if re-projection is required.

            Valid values are: ``'nearest', 'cubic', 'bilinear', 'cubic_spline', 'lanczos', 'average'``

            Defaults to ``'nearest'``.

        :return: The requested data.
        :rtype: :py:class:`xarray.Dataset`

        .. seealso::
            :meth:`list_tiles` :meth:`list_cells`
        """
        observations = []
        for dataset in tile.sources.values:
            observations += dataset

        all_measurements = get_measurements(observations)
        if measurements:
            measurements = [all_measurements[measurement] for measurement in measurements
                            if measurement in all_measurements]
        else:
            measurements = [measurement for measurement in all_measurements.values()]

        measurements = set_resampling_method(measurements, resampling)

        dataset = Datacube.product_data(tile.sources, tile.geobox, measurements, dask_chunks=dask_chunks,
                                        fuse_func=fuse_func)

        return dataset

    def __str__(self):
        return "GridWorkflow<index={!r},\n\tgridspec={!r}>".format(self.index, self.grid_spec)

    def __repr__(self):
        return self.__str__()
