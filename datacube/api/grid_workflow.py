from __future__ import absolute_import, division, print_function

import logging
from collections import defaultdict

from ..model import GeoBox
from ..utils import check_intersect
from .query import Query
from .core import get_measurements, get_bounds

_LOG = logging.getLogger(__name__)


class GridWorkflow(object):
    """
    GridWorkflow deals with cell- and tile-based processing using a grid defining a projection and resolution.
    """
    def __init__(self, datacube, grid_spec=None, product=None, lazy=False):
        """
        Creates a grid workflow tool.

        Either grid_spec or product must be supplied.

        :param datacube: The Datacube object to use.
        :type datacube: :py:class:`datacube.Datacube`
        :param grid_spec: The grid projection and resolution
        :type grid_spec: :class:`model.GridSpec`
        :param product: The name of an existing product, if no grid_spec is supplied.
        :type product: str
        :param lazy: If the data should be wrapped in a `dask` array.
        :type lazy: bool
        """
        if lazy:
            raise NotImplementedError('Lazy loading not fully implemented yet.')
        self.datacube = datacube
        if grid_spec is None:
            grid_spec = datacube.grid_spec_for_product(product)
        self.grid_spec = grid_spec
        self.lazy = lazy

    def list_cells(self, **indexers):
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
        query = Query.from_kwargs(self.datacube.index, **indexers)
        observations = self.datacube.product_observations(**query.search_terms)

        cells = set()
        extents = [dataset.extent.to_crs(self.grid_spec.crs) for dataset in observations]

        bounds_geopolygon = get_bounds(observations, self.grid_spec.crs)
        for tile_index, tile_geobox in self.grid_spec.tiles(bounds_geopolygon.boundingbox):
            tile_geopolygon = tile_geobox.extent
            if any(check_intersect(tile_geopolygon, dataset_geopolygon) for dataset_geopolygon in extents):
                cells.add(tile_index)
        return cells

    def cell_observations(self, xy_cell=None, **indexers):
        """
        Lists datasets, grouped by cell.

        :param xy_cell: The cell index. E.g. (14, -40)
        :param indexers: Query to match the datasets
        :return: A dict of dicts of :py:class:`datacube.model.Dataset`

        .. seealso:: :meth:`datacube.Datacube.product_sources`
        """
        query = Query.from_kwargs(self.datacube.index, **indexers)
        geopolygon = None
        if xy_cell:
            assert isinstance(xy_cell, tuple)
            assert len(xy_cell) == 2
            geobox = GeoBox.from_grid_spec(self.grid_spec, xy_cell)
            geopolygon = geobox.extent

        observations = self.datacube.product_observations(geopolygon=geopolygon, **query.search_terms)
        if not observations:
            return {}

        tiles = {}
        bounds_geopolygon = get_bounds(observations, self.grid_spec.crs)
        for tile_index, tile_geobox in self.grid_spec.tiles(bounds_geopolygon.boundingbox):
            tile_geopolygon = tile_geobox.extent
            for dataset in observations:
                if not check_intersect(tile_geopolygon, dataset.extent.to_crs(self.grid_spec.crs)):
                    continue
                tiles.setdefault(tile_index, []).append(dataset)
        return tiles

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
        query = Query.from_kwargs(self.datacube.index, **indexers)
        gb = query.group_by

        cell_observations = self.cell_observations(xy_cell, **indexers)
        stack = defaultdict(dict)
        for cell, observations in cell_observations.items():
            sources = self.datacube.product_sources(observations,
                                                    group_func=gb.group_by_func,
                                                    dimension=gb.dimension,
                                                    units=gb.units)
            for tile_index in sources[gb.dimension].values:
                stack[cell][tile_index] = sources.sel(**{gb.dimension: [tile_index]})
        return stack

    def list_tile_stacks(self, xy_cell=None, **indexers):
        """
        Lists tiles of data, sorted by cell.
        ::

            tiles = gw.list_tiles(product_type=['nbar', 'pq'], platform='LANDSAT_5', by='cell')

        The values can be passed to :meth:`load`

        :param xy_cell: The cell index. E.g. (14, -40)
        :param indexers: Query to match the datasets
        :return: A dict of dicts, which can be used to call :meth:`load`

        .. seealso:: :meth:`load`
        """
        query = Query.from_kwargs(self.datacube.index, **indexers)
        gb = query.group_by

        cell_observations = self.cell_observations(xy_cell, **indexers)
        stack = defaultdict(dict)
        for cell, observations in cell_observations.items():
            stack[cell] = self.datacube.product_sources(observations,
                                                        group_func=gb.group_by_func,
                                                        dimension=gb.dimension,
                                                        units=gb.units)
        return stack

    def load(self, xy_cell, sources, measurements=None):
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
        assert isinstance(xy_cell, tuple)

        geobox = GeoBox.from_grid_spec(self.grid_spec, xy_cell)

        observations = []
        for dataset in sources.values:
            observations += dataset

        all_measurements = get_measurements(observations)
        if measurements:
            measurements = [all_measurements[measurement] for measurement in measurements
                            if measurement in all_measurements]
        else:
            measurements = [measurement for measurement in all_measurements.values()]

        dataset = self.datacube.product_data(sources, geobox, measurements)
        return dataset
