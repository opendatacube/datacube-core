from __future__ import absolute_import, division, print_function

import logging
from collections import defaultdict

from ..model import GeoBox
from ..utils import check_intersect
from .query import Query, GroupByQuery
from .core import Datacube, get_measurements, get_bounds

_LOG = logging.getLogger(__name__)


class GridWorkflow(object):
    """
    GridWorkflow deals with cell- and tile-based processing using a grid defining a projection and resolution.
    """
    def __init__(self, index, grid_spec=None, product=None, lazy=False):
        """
        Creates a grid workflow tool.

        Either grid_spec or product must be supplied.

        :param index: The database index to use.
        :type index: from :py:class:`datacube.index.index_connect` or None
        :param grid_spec: The grid projection and resolution
        :type grid_spec: :class:`model.GridSpec`
        :param product: The name of an existing product, if no grid_spec is supplied.
        :type product: str
        :param lazy: If the data should be wrapped in a `dask` array.
        :type lazy: bool
        """
        if lazy:
            raise NotImplementedError('Lazy loading not fully implemented yet.')
        self.index = index
        if grid_spec is None:
            dataset_type = self.index.types.get_by_name(product)
            grid_spec = dataset_type and dataset_type.grid_spec
        self.grid_spec = grid_spec
        self.lazy = lazy

    def cell_observations(self, xy_cell=None, geopolygon=None, **indexers):
        """
        Lists datasets, grouped by cell.

        :param xy_cell: The cell index. E.g. (14, -40)
        :param indexers: Query to match the datasets
        :return: A dict of dicts of :py:class:`datacube.model.Dataset`

        .. seealso:: :meth:`datacube.Datacube.product_sources`
        """
        if xy_cell:
            assert isinstance(xy_cell, tuple)
            assert len(xy_cell) == 2
            geobox = GeoBox.from_grid_spec(self.grid_spec, xy_cell)
            geopolygon = geobox.extent
        query = Query(index=self.index, geopolygon=geopolygon, **indexers)

        if not query.product:
            raise RuntimeError('must specify a product')

        observations = self.index.datasets.search_eager(**query.search_terms)
        if not observations:
            return {}

        if xy_cell:
            tile_iter = [(xy_cell, geobox)]
        else:
            bounds_geopolygon = get_bounds(observations, self.grid_spec.crs)
            tile_iter = self.grid_spec.tiles(bounds_geopolygon.boundingbox)

        tiles = {}
        for tile_index, tile_geobox in tile_iter:
            tile_geopolygon = tile_geobox.extent
            datasets = [dataset for dataset in observations
                        if check_intersect(tile_geopolygon, dataset.extent.to_crs(self.grid_spec.crs))]
            tiles[tile_index] = {
                'datasets': datasets,
                'geobox': tile_geobox
            }
        return tiles

    @staticmethod
    def cell_sources(observations, group_by):
        stack = defaultdict(dict)
        for cell_index, observation in observations.items():
            sources = Datacube.product_sources(observation['datasets'],
                                               group_func=group_by.group_by_func,
                                               dimension=group_by.dimension,
                                               units=group_by.units)
            stack[cell_index] = {
                'sources': sources,
                'geobox': observation['geobox']
            }
        return stack

    @staticmethod
    def tile_sources(observations, group_by):
        stack = defaultdict(dict)
        for cell_index, observation in observations.items():
            sources = Datacube.product_sources(observation['datasets'],
                                               group_func=group_by.group_by_func,
                                               dimension=group_by.dimension,
                                               units=group_by.units)
            for tile_index in sources[group_by.dimension].values:
                stack[cell_index + (tile_index,)] = {
                    'sources': sources.sel(**{group_by.dimension: [tile_index]}),
                    'geobox': observation['geobox']
                }
        return stack

    def list_cells(self, xy_cell=None, **indexers):
        """
        Lists cell indices that match the query.

        Cells are included if they contain any datasets that match the query using the same format as
        :meth:`datacube.Datacube.load`.

        E.g.::

            gw.list_cells(product_type='nbar',
                          platform=['LANDSAT_5', 'LANDSAT_7', 'LANDSAT_8'],
                          time=('2001-1-1 00:00:00', '2001-3-31 23:59:59'))

        :param indexers: same as :meth:`datacube.Datacube.load`
        :return: list of cell index tuples. E.g.::

            [(14, -42), (14, -41), (14, -40), (14, -39), (15, -42), (15, -41), (15, -40), (15, -39)]

        """
        query = GroupByQuery(index=self.index, **indexers)
        observations = self.cell_observations(xy_cell, **indexers)
        return self.cell_sources(observations, query.group_by)

    def list_tiles(self, xy_cell=None, **indexers):
        """
        Lists tiles of data, sorted by cell.
        ::

            tiles = gw.list_tiles(product_type=['nbar', 'pq'], platform='LANDSAT_5')

        The values can be passed to :meth:`load`

        :param xy_cell: The cell index. E.g. (14, -40)
        :param indexers: Query to match the datasets
        :return: A dict of dicts, which can be used to call :meth:`load`

        .. seealso:: :meth:`load`
        """
        query = GroupByQuery(index=self.index, **indexers)
        observations = self.cell_observations(xy_cell, **indexers)
        return self.tile_sources(observations, query.group_by)

    @staticmethod
    def load(tile, measurements=None):
        """
        Loads data for a cell.

        The data to be loaded is defined by the output of :meth:`list_tiles`.

        :param xy_cell: The cell to load. E.g. (-13, 40)
        :param sources: A DataArray of the input data, as made by :meth:`list_tiles`.
        :type sources: :py:class:`xarray.DataArray`

        :param measurements: The name or list of names of measurements to load
        :return: The requested data.
        :rtype: :py:class:`xarray.Dataset`

        .. seealso:: :meth:`list_tiles`
        """
        sources = tile['sources']
        geobox = tile['geobox']

        observations = []
        for dataset in sources.values:
            observations += dataset

        all_measurements = get_measurements(observations)
        if measurements:
            measurements = [all_measurements[measurement] for measurement in measurements
                            if measurement in all_measurements]
        else:
            measurements = [measurement for measurement in all_measurements.values()]

        dataset = Datacube.product_data(sources, geobox, measurements)
        return dataset
