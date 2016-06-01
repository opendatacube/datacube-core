from __future__ import absolute_import, division, print_function

import logging
from itertools import groupby
from collections import namedtuple, OrderedDict
from math import ceil

import pandas
import numpy
import xarray
from dask import array as da
from rasterio.coords import BoundingBox

from ..model import GeoBox
from ..utils import check_intersect
from .query import Query, GroupBy
from .core import get_measurements, get_bounds

_LOG = logging.getLogger(__name__)


class GridWorkflow(object):
    def __init__(self, datacube, grid_spec, lazy=False):
        self.datacube = datacube
        self.grid_spec = grid_spec
        self.lazy = lazy

    def get_dataset(self, xy_cell, lazy=None, **indexers):
        if lazy is None:
            lazy = self.lazy
        assert isinstance(xy_cell, tuple)
        assert len(xy_cell) == 2

        geobox = GeoBox.from_grid_spec(self.grid_spec, xy_cell)

        query = Query.from_kwargs(self.datacube.index, **indexers)
        assert query.geopolygon is None
        geopolygon = geobox.extent  # intersection with query.geobox?
        observations = self.datacube.product_observations(geopolygon=geopolygon, **query.search_terms)

        group_by = query.group_by
        sources = self.datacube.product_sources(observations,
                                                group_by.group_by_func, group_by.dimension, group_by.units)

        all_measurements = get_measurements(observations)
        if query.variables:
            measurements = [all_measurements[variable] for variable in query.variables if variable in all_measurements]
        else:
            measurements = [measurement for measurement in all_measurements.values()]

        dataset = self.datacube.product_data(sources, geobox, measurements)
        return dataset

    def list_cells(self, **indexers):
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

    def list_tiles(self, **indexers):
        query = Query.from_kwargs(self.datacube.index, **indexers)
        observations = self.datacube.product_observations(**query.search_terms)
        if not observations:
            return {}

        tiles = {}
        # extents = [dataset.extent.to_crs(self.grid_spec.crs) for dataset in observations]

        bounds_geopolygon = get_bounds(observations, self.grid_spec.crs)
        for tile_index, tile_geobox in self.grid_spec.tiles(bounds_geopolygon.boundingbox):
            tile_geopolygon = tile_geobox.extent
            for dataset in observations:
                if not check_intersect(tile_geopolygon, dataset.extent.to_crs(self.grid_spec.crs)):
                    continue

                tiles.setdefault(dataset.type.name, {}).setdefault(tile_index, []).append(dataset)
        return tiles
