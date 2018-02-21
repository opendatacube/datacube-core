# Warning: this is a WIP

# TODO: fix juxtapose collate bug
# TODO: needs an aggregation phase
# TODO: collate index_measurement
"""
Implementation of virtual products.
Provides an interface to the products in the database
for querying and loading data, and combinators to
combine multiple products into "virtual" products
implementing the same interface.
"""

from __future__ import absolute_import

from abc import ABC, abstractmethod
from functools import reduce

import xarray

from datacube import Datacube
from datacube.model import Measurement
from datacube.model.utils import xr_apply
from datacube.api.query import Query, query_group_by, query_geopolygon
from datacube.api.grid_workflow import _fast_slice

from .utils import select_datasets_inside_polygon
from .utils import output_geobox
from .utils import product_definitions_from_index


class VirtualProductException(Exception):
    """ Raised if the construction of the virtual product cannot be validated. """
    pass


class VirtualProduct(ABC):
    """ Abstract class defining the common interface of virtual products. """

    @abstractmethod
    def output_measurements(self, product_definitions):
        # type: (Dict[str, dict]) -> Dict[str, Measurement]
        """ A dictionary mapping names to measurement metadata. """

    @abstractmethod
    def find_datasets(self, dc, **query):
        # type: (Datacube, Dict[str, Any]) -> DatasetPile
        """ Collection of datasets that match the query. """

    # no database access below this line

    @abstractmethod
    def build_raster(self, datasets, **query):
        # type: (VirtualDatasetPile, Dict[str, Any]) -> RasterRecipe
        """
        Datasets grouped by their timestamps.
        :param datasets: the datasets to fetch data from
        :param query: to specify a spatial sub-region
        """

    @abstractmethod
    def fetch_data(self, raster):
        # type: (RasterRecipe) -> xarray.Dataset
        """ Convert virtual raster to `xarray.Dataset`. """

    def load(self, dc, **query):
        # type: (Datacube, Dict[str, Any]) -> xarray.Dataset
        """ Mimic `datacube.Datacube.load`. """
        datasets = self.find_datasets(dc, **query)
        raster = self.build_raster(datasets, **query)
        observations = [self.fetch_data(observation)
                        for observation in raster.split(dim='time')]
        data = xarray.concat(observations, dim='time')

        return data


class DatasetPile(object):
    """ Result of `VirtualProduct.find_datasets`. """
    def __init__(self, kind, pile, grid_spec, output_measurements):
        assert kind in ['basic', 'collate', 'juxtapose']
        self.kind = kind
        self.pile = tuple(pile)
        self.grid_spec = grid_spec
        self.output_measurements = output_measurements


class RasterRecipe(object):
    """ Result of `VirtualProduct.build_raster`. """
    # our replacement for grid_workflow.Tile basically
    # TODO: copy the Tile API
    def __init__(self, grouped_dataset_pile, geobox, output_measurements):
        self.grouped_dataset_pile = grouped_dataset_pile
        self.geobox = geobox
        self.output_measurements = output_measurements

    @property
    def dims(self):
        """
        Names of the dimensions, e.g., ``('time', 'y', 'x')``.
        :return: tuple(str)
        """
        return self.grouped_dataset_pile.dims + self.geobox.dimensions

    @property
    def shape(self):
        """
        Lengths of each dimension, e.g., ``(285, 4000, 4000)``.
        :return: tuple(int)
        """
        return self.grouped_dataset_pile.shape + self.geobox.shape

    def __getitem__(self, chunk):
        pile = self.grouped_dataset_pile

        return RasterRecipe(_fast_slice(pile, chunk[:len(pile.shape)]),
                            self.geobox[chunk[len(pile.shape):]],
                            self.output_measurements)

    def map(self, func, dtype='O'):
        return RasterRecipe(xr_apply(self.grouped_dataset_pile, func, dtype=dtype),
                            self.geobox, self.output_measurements)

    def filter(self, predicate):
        mask = self.map(predicate, dtype='bool')
        return RasterRecipe(self.grouped_dataset_pile[mask.grouped_dataset_pile],
                            self.geobox, self.output_measurements)

    def split(self, dim='time'):
        # this is slightly different from Tile.split
        pile = self.grouped_dataset_pile

        (length,) = pile[dim].shape
        for i in range(length):
            yield RasterRecipe(pile.isel(**{dim: slice(i, i + 1)}),
                               self.geobox, self.output_measurements)


class BasicProduct(VirtualProduct):
    """ A product already in the datacube. """
    def __init__(self, product_name, measurement_names=None,
                 source_filter=None, fuse_func=None, resampling_method=None,
                 dataset_filter=None):
        """
        :param product_name: name of the product
        :param measurement_names: list of names of measurements to include (None if all)
        :param dataset_filter: a predicate on `datacube.Dataset` objects
        """
        self.product_name = product_name

        if measurement_names is not None and len(measurement_names) == 0:
            raise VirtualProductException("Product selects no measurements")

        self.measurement_names = measurement_names

        # is this a good place for it?
        self.source_filter = source_filter
        self.fuse_func = fuse_func
        self.resampling_method = resampling_method

        self.dataset_filter = dataset_filter

    def output_measurements(self, product_definitions):
        """ Output measurements metadata. """
        measurement_docs = product_definitions[self.product_name]['measurements']
        measurements = {measurement['name']: Measurement(**measurement)
                        for measurement in measurement_docs}

        if self.measurement_names is None:
            return measurements

        try:
            return {name: measurements[name] for name in self.measurement_names}
        except KeyError as ke:
            raise VirtualProductException("Could not find measurement: {}".format(ke.args))

    def find_datasets(self, dc, **query):
        # this is basically a copy of `datacube.Datacube.find_datasets_lazy`
        # ideally that method would look like this too in the future

        # `like` is implicitly supported here, not sure if we should
        # `platform` and `product_type` based queries are possibly ruled out
        # other possible query entries include `geopolygon`
        # and contents of `SPATIAL_KEYS` and `CRS_KEYS`
        # query should not include contents of `OTHER_KEYS` except `geopolygon`
        index = dc.index

        # find the datasets
        query = Query(index, product=self.product_name, measurements=self.measurement_names,
                      source_filter=self.source_filter, **query)
        assert query.product == self.product_name

        datasets = select_datasets_inside_polygon(index.datasets.search(**query.search_terms),
                                                  query.geopolygon)

        if self.dataset_filter is not None:
            datasets = [dataset for dataset in datasets if self.dataset_filter(dataset)]

        # gather information from the index before it disappears from sight
        product_definitions = product_definitions_from_index(index)
        output_measurements = self.output_measurements(product_definitions)
        grid_spec = index.products.get_by_name(self.product_name).grid_spec

        return DatasetPile('basic', datasets, grid_spec, output_measurements)
        # TODO: should we actually return (time-grouped) raster?

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, DatasetPile) and datasets.kind == 'basic'
        pile = datasets.pile
        grid_spec = datasets.grid_spec
        output_measurements = datasets.output_measurements

        # we will support group_by='solar_day' elsewhere
        assert 'group_by' not in query

        # possible query entries are contents of `SPATIAL_KEYS`, `CRS_KEYS`, and `OTHER_KEYS`
        # query should not include `product`, `measurements`, and `resampling`

        # select only those inside the ROI
        # ROI could be smaller than the query for `find_datasets`
        polygon = query_geopolygon(**query)
        selected = list(select_datasets_inside_polygon(pile, polygon))

        # geobox
        geobox = output_geobox(pile, grid_spec, **query)

        # group by time
        grouped = Datacube.group_datasets(selected, query_group_by(group_by='time'))

        def wrap(indexes, value):
            return DatasetPile('basic', value, grid_spec, output_measurements)

        # information needed for Datacube.load_data
        return RasterRecipe(grouped, geobox, output_measurements).map(wrap)

    def fetch_data(self, raster):
        assert isinstance(raster, RasterRecipe)

        # this method is basically `GridWorkflow.load`

        # convert Measurements back to dicts?
        # essentially what `datacube.api.core.set_resampling_method` does
        measurements = [{**measurement.__dict__}
                        for measurement in raster.output_measurements.values()]

        if self.resampling_method is not None:
            measurements = [{'resampling_method': self.resampling_method, **measurement}
                            for measurement in measurements]

        def unwrap(indexes, value):
            assert isinstance(value, DatasetPile) and value.kind == 'basic'
            return value.pile

        return Datacube.load_data(raster.map(unwrap).grouped_dataset_pile,
                                  raster.geobox, measurements, fuse_func=self.fuse_func)


def basic_product(product_name, measurement_names=None,
                  source_filter=None, fuse_func=None, resampling_method=None):
    return BasicProduct(product_name, measurement_names=measurement_names,
                        source_filter=source_filter, fuse_func=fuse_func,
                        resampling_method=resampling_method)


class Transform(VirtualProduct):
    def __init__(self, child,
                 data_transform=None, measurement_transform=None, raster_transform=None):
        """
        :param transform: a `TransformationFunction`
        """
        self.child = child

        def identity(x):
            return x

        def guard(func):
            if func is None:
                return identity
            return func

        self.data_transform = guard(data_transform)
        self.measurement_transform = guard(measurement_transform)
        self.raster_transform = guard(raster_transform)

    def output_measurements(self, product_definitions):
        return self.measurement_transform(self.child.output_measurements(product_definitions))

    def find_datasets(self, dc, **query):
        return self.child.find_datasets(dc, **query)

    def build_raster(self, datasets, **query):
        return self.child.build_raster(datasets, **query)

    def fetch_data(self, raster):
        return self.data_transform(self.child.fetch_data(raster))


def transform(child, data_transform=None, measurement_transform=None, raster_transform=None):
    return Transform(child, data_transform=data_transform,
                     measurement_transform=measurement_transform, raster_transform=raster_transform)


class Collate(VirtualProduct):
    def __init__(self, *children, index_measurement_name=None):
        if len(children) == 0:
            raise VirtualProductException("No children for collate node")

        self.children = children
        self.index_measurement_name = index_measurement_name

        name = self.index_measurement_name
        if name is not None:
            self.index_measurement = {
                name: Measurement(name=name, dtype='int8', nodata=-1, units='1')
            }

    def output_measurements(self, product_definitions):
        input_measurements = [child.output_measurements(product_definitions)
                              for child in self.children]

        first, *rest = input_measurements

        for child in rest:
            if set(child) != set(first):
                msg = "Child datasets do not all have the same set of measurements"
                raise VirtualProductException(msg)

        if self.index_measurement_name is None:
            return first

        if self.index_measurement_name in first:
            msg = "Source index measurement '{}' already present".format(self.index_measurement_name)
            raise VirtualProductException(msg)

        return {**first, **self.index_measurement}

    def find_datasets(self, dc, **query):
        index = dc.index

        result = [child.find_datasets(dc, **query)
                  for child in self.children]

        # should possibly check all the `grid_spec`s are the same
        # requires a `GridSpec.__eq__` method implementation
        product_definitions = product_definitions_from_index(index)
        return DatasetPile('collate', result, result[0].grid_spec, self.output_measurements(product_definitions))

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, DatasetPile) and datasets.kind == 'collate'
        assert len(datasets.pile) == len(self.children)
        grid_spec = datasets.grid_spec
        output_measurements = datasets.output_measurements

        def build(source_index, product, dataset_pile):
            raster = product.build_raster(dataset_pile, **query)

            def tag(indexes, value):
                return DatasetPile('collate',
                                   [value if i == source_index else None
                                    for i, _ in enumerate(self.children)],
                                   grid_spec, output_measurements)

            return raster.map(tag)

        rasters = [build(source_index, product, dataset_pile)
                   for source_index, (product, dataset_pile)
                   in enumerate(zip(self.children, datasets.pile))]

        # should possibly check all the geoboxes are the same
        first = rasters[0]

        concatenated = xarray.concat([raster.grouped_dataset_pile for raster in rasters], dim='time')
        return RasterRecipe(concatenated, first.geobox, output_measurements)

    def fetch_data(self, raster):
        assert isinstance(raster, RasterRecipe)
        grouped_dataset_pile = raster.grouped_dataset_pile
        geobox = raster.geobox
        output_measurements = raster.output_measurements

        def is_from(source_index):
            def result(indexes, value):
                assert isinstance(value, DatasetPile) and value.kind == 'collate'
                return value.pile[source_index] is not None

            return result

        def strip_source(indexes, value):
            assert isinstance(value, DatasetPile) and value.kind == 'collate'
            for data in value.pile:
                if data is not None:
                    return data

            raise ValueError("Every child of CollatedDatasetPile object is None")

        def fetch_data(child, r):
            size = reduce(lambda x, y: x * y, r.shape, 1)

            if size > 0:
                return child.fetch_data(r)
            else:
                # empty raster
                return None

        rasters = [fetch_data(child, raster.filter(is_from(source_index)).map(strip_source))
                   for source_index, child in enumerate(self.children)]

        return xarray.concat([r for r in rasters if r is not None], dim='time')


def collate(*children, index_measurement_name=None):
    return Collate(*children, index_measurement_name=index_measurement_name)


class Juxtapose(VirtualProduct):
    def __init__(self, *children):
        if len(children) == 0:
            raise VirtualProductException("No children for juxtapose node")

        self.children = children

    def output_measurements(self, product_definitions):
        input_measurements = [child.output_measurements(product_definitions)
                              for child in self.children]

        result = {}
        for measurements in input_measurements:
            common = set(result) & set(measurements)
            if common != set():
                msg = "Common measurements {} between children".format(common)
                raise VirtualProductException(msg)

            result.update(measurements)

        return result

    def find_datasets(self, dc, **query):
        index = dc.index

        product_definitions = product_definitions_from_index(index)
        result = [child.find_datasets(dc, **query)
                  for child in self.children]

        # should possibly check all the `grid_spec`s are the same
        # requires a `GridSpec.__eq__` method implementation
        return DatasetPile('juxtapose', result, result[0].grid_spec,
                           self.output_measurements(product_definitions))

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, DatasetPile) and datasets.kind == 'juxtapose'
        assert len(datasets.pile) == len(self.children)

        pile = datasets.pile
        grid_spec = datasets.grid_spec
        output_measurements = datasets.output_measurements

        rasters = [product.build_raster(datasets, **query)
                   for product, datasets in zip(self.children, pile)]

        # should possibly check all the geoboxes are the same
        geobox = rasters[0].geobox

        aligned_piles = xarray.align(*[raster.grouped_dataset_pile for raster in rasters])
        child_rasters = [RasterRecipe(aligned_piles[i], raster.geobox, raster.output_measurements)
                         for i, raster in enumerate(rasters)]

        def tuplify(indexes, value):
            return DatasetPile('juxtapose',
                               [raster.grouped_dataset_pile.sel(**indexes).item()
                                for raster in child_rasters],
                               grid_spec, output_measurements)

        merged = child_rasters[0].map(tuplify).grouped_dataset_pile

        return RasterRecipe(merged, geobox, output_measurements)

    def fetch_data(self, raster):
        assert isinstance(raster, RasterRecipe)
        grouped_dataset_pile = raster.grouped_dataset_pile
        geobox = raster.geobox
        output_measurements = raster.output_measurements

        def select_child(source_index):
            def result(indexes, value):
                assert isinstance(value, DatasetPile) and value.kind == 'juxtapose'
                return value.pile[source_index]

            return result

        def fetch_recipe(source_index):
            child_raster = raster.map(select_child(source_index))
            grouped = child_raster.grouped_dataset_pile

            # can `grouped` really be empty?
            child_measurements = grouped.item(0).output_measurements
            return RasterRecipe(grouped, geobox, child_measurements)

        rasters = [child.fetch_data(fetch_recipe(source_index))
                   for source_index, child in enumerate(self.children)]

        return xarray.merge(rasters)


def juxtapose(*children):
    return Juxtapose(*children)
