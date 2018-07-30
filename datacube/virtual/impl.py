# TODO: alias support
# TODO: needs an aggregation phase (use xarray.DataArray.groupby?)
# TODO: measurement dependency tracking
# TODO: ability to select measurements higher up in the tree
# TODO: a mechanism for per-leaf settings
# TODO: lineage tracking per observation
# TODO: what does GridWorkflow do more than this?
# TODO: provision for splitting the tasks before and after grouping
"""
Implementation of virtual products. Provides an interface for the products in the datacube
to query and to load data, and combinators to combine multiple products into "virtual"
products implementing the same interface.
"""
from abc import ABC, abstractmethod
from functools import reduce
import warnings

import xarray
import numpy

from datacube import Datacube
from datacube.model import Measurement, Range
from datacube.model.utils import xr_apply
from datacube.api.query import Query, query_group_by, query_geopolygon
from datacube.api.grid_workflow import _fast_slice
from datacube.api.core import select_datasets_inside_polygon, output_geobox


class VirtualProductException(Exception):
    """ Raised if the construction of the virtual product cannot be validated. """
    pass


class VirtualProduct(ABC):
    """ Abstract class defining the common interface of virtual products. """

    @abstractmethod
    def output_measurements(self, product_definitions):
        # type: (Dict[str, Dict]) -> Dict[str, Measurement]
        """
        A dictionary mapping names to measurement metadata.
        :param product_definitions: a dictionary mapping product names to definitions
        """

    @abstractmethod
    def find_datasets(self, dc, **query):
        # type: (Datacube, Dict[str, Any]) -> DatasetPile
        """ Collection of datasets that match the query. """

    # no index access below this line

    @abstractmethod
    def group_datasets(self, datasets, **query):
        # type: (DatasetPile, Dict[str, Any]) -> GroupedDatasetPile
        """
        Datasets grouped by their timestamps.
        :param datasets: the `DatasetPile` to fetch data from
        :param query: to specify a spatial sub-region
        """

    # TODO: provide `load_lazy` and `load_strict` instead

    @abstractmethod
    def fetch_data(self, grouped, product_definitions, **load_settings):
        # type: (GroupedDatasetPile, Dict[str, Dict], Dict[str, Any]) -> xarray.Dataset
        """ Convert grouped datasets to `xarray.Dataset`. """

    def load(self, dc, **query):
        # type: (Datacube, Dict[str, Any]) -> xarray.Dataset
        """ Mimic `datacube.Datacube.load`. """
        product_definitions = product_definitions_from_index(dc.index)
        datasets = self.find_datasets(dc, **query)
        grouped = self.group_datasets(datasets, **query)
        return self.fetch_data(grouped, product_definitions, **query).sortby('time')


class DatasetPile(object):
    """ Result of `VirtualProduct.find_datasets`. """
    def __init__(self, pile, grid_spec):
        self.pile = tuple(pile)
        self.grid_spec = grid_spec


class GroupedDatasetPile(object):
    """ Result of `VirtualProduct.group_datasets`. """
    # our replacement for grid_workflow.Tile basically
    # TODO: copy the Tile API
    def __init__(self, pile, geobox):
        self.pile = pile
        self.geobox = geobox

    @property
    def dims(self):
        """
        Names of the dimensions, e.g., ``('time', 'y', 'x')``.
        :return: tuple(str)
        """
        return self.pile.dims + self.geobox.dimensions

    @property
    def shape(self):
        """
        Lengths of each dimension, e.g., ``(285, 4000, 4000)``.
        :return: tuple(int)
        """
        return self.pile.shape + self.geobox.shape

    def __getitem__(self, chunk):
        pile = self.pile

        return GroupedDatasetPile(_fast_slice(pile, chunk[:len(pile.shape)]),
                                  self.geobox[chunk[len(pile.shape):]])

    def map(self, func, dtype='O'):
        return GroupedDatasetPile(xr_apply(self.pile, func, dtype=dtype), self.geobox)

    def filter(self, predicate):
        mask = self.map(predicate, dtype='bool')

        # NOTE: this could possibly result in an empty pile
        return GroupedDatasetPile(self.pile[mask.pile], self.geobox)

    def split(self, dim='time'):
        # this is slightly different from Tile.split
        pile = self.pile

        [length] = pile[dim].shape
        for i in range(length):
            yield GroupedDatasetPile(pile.isel(**{dim: slice(i, i + 1)}), self.geobox)


class BasicProduct(VirtualProduct):
    # initially copied from datacube.api.query
    # should update changes there too
    """ A product already in the datacube. """

    GEOBOX_KEYS = ['output_crs', 'resolution', 'align']
    GROUPING_KEYS = ['group_by']
    LOAD_KEYS = ['measurements', 'fuse_func', 'resampling', 'stack', 'dask_chunks', 'use_threads']
    ADDITIONAL_KEYS = ['dataset_predicate']

    NON_SPATIAL_KEYS = GEOBOX_KEYS + GROUPING_KEYS
    NON_QUERY_KEYS = NON_SPATIAL_KEYS + LOAD_KEYS + ADDITIONAL_KEYS

    def __init__(self, product, **settings):
        """
        :param product: name of the product
        :param settings: for querying, grouping and loading (may be overriden later)
        """
        # NOTE: resampling_method can easily be a per-measurement setting
        self.product = product
        self.settings = settings

    def output_measurements(self, product_definitions):
        """ Output measurements metadata. """
        measurement_docs = product_definitions[self.product]['measurements']
        measurements = {measurement['name']: Measurement(**measurement)
                        for measurement in measurement_docs}

        if self.settings.get('measurements') is None:
            return measurements

        try:
            return {name: measurements[name]
                    for name in self.settings['measurements']}
        except KeyError as ke:
            raise VirtualProductException("could not find measurement: {}".format(ke.args))

    def find_datasets(self, dc, **search_terms):
        index = dc.index
        originals = Query(index, product=self.product, like=self.settings.get('like'),
                          **reject_keys(self.settings, self.NON_QUERY_KEYS)).search_terms
        overrides = Query(index, product=self.product, like=search_terms.get('like'),
                          **reject_keys(search_terms, self.NON_QUERY_KEYS)).search_terms

        query = Query(index, **merge_search_terms(originals, overrides))
        assert query.product == self.product

        # find the datasets
        datasets = select_datasets_inside_polygon(index.datasets.search(**query.search_terms),
                                                  query.geopolygon)

        if self.settings.get('dataset_predicate') is not None:
            datasets = [dataset
                        for dataset in datasets
                        if self.settings['dataset_predicate'](dataset)]

        # gather information from the index before it disappears from sight
        # this can also possibly extracted from the product definitions but this is easier
        grid_spec = index.products.get_by_name(self.product).grid_spec

        return DatasetPile(datasets, grid_spec)

    def group_datasets(self, datasets, **search_terms):
        pile = datasets.pile
        grid_spec = datasets.grid_spec

        # select only those inside the ROI
        # ROI could be smaller than the query for `find_datasets`
        spatial_query = reject_keys(search_terms, self.NON_SPATIAL_KEYS)
        polygon = query_geopolygon(**spatial_query)
        selected = list(select_datasets_inside_polygon(pile, polygon))

        # geobox
        merged = merge_search_terms(select_keys(self.settings, self.NON_SPATIAL_KEYS),
                                    select_keys(spatial_query, self.NON_SPATIAL_KEYS))
        geobox_settings = select_keys(merged, self.GEOBOX_KEYS)

        geobox = output_geobox(datasets=selected, grid_spec=grid_spec,
                               **geobox_settings, **spatial_query)

        # group by time
        grouping_settings = select_keys(merged, self.GROUPING_KEYS)
        grouped = Datacube.group_datasets(selected,
                                          query_group_by(**grouping_settings))

        def wrap(_, value):
            return DatasetPile(value, grid_spec)

        # information needed for Datacube.load_data
        return GroupedDatasetPile(grouped, geobox).map(wrap)

    def fetch_data(self, grouped, product_definitions, **load_settings):
        assert isinstance(grouped, GroupedDatasetPile)

        merged = merge_search_terms(select_keys(self.settings, self.LOAD_KEYS),
                                    select_keys(load_settings, self.LOAD_KEYS))

        # this method is basically `GridWorkflow.load`

        # essentially what `datacube.api.core.set_resampling_method` does
        if merged.get('resampling') is not None:
            resampling = {'resampling_method': merged['resampling_method']}
        else:
            resampling = {}

        # load_settings should not contain `measurements`
        measurements = [Measurement(**measurement, **resampling)
                        for _, measurement in self.output_measurements(product_definitions).items()]

        def unwrap(_, value):
            return value.pile

        return Datacube.load_data(grouped.map(unwrap).pile,
                                  grouped.geobox, measurements,
                                  fuse_func=merged.get('fuse_func'),
                                  dask_chunks=merged.get('dask_chunks'),
                                  use_threads=merged.get('use_threads'))

    def __repr__(self):
        settings = ",".join("{}={}".format(key, value) for key, value in self.settings.items())
        return "BasicProduct(product={}, {})".format(self.product, settings)


class Transformation(ABC):
    """
    A user-defined on-the-fly data transformation.

    The data coming in and out of the `compute` method are `xarray.Dataset` objects.
    The measurements are stored as `xarray.DataArray` objects inside it.

    The `measurements` method transforms the list of `datacube.model.Measurement` objects
    describing the measurements of the input data into the list of
    `datacube.model.Measurement` objects describing the measurements of the output data
    produced by the `compute` method.
    """

    @abstractmethod
    def measurements(self, input_measurements):
        """
        Returns the list of output measurements from this transformation.
        Assumes the `data` provided to `compute` will have measurements
        given by the list `input_measurements`.
        """
        pass

    @abstractmethod
    def compute(self, data):
        """
        Perform computation on `data` that results in an `xarray.Dataset`
        having measurements reported by the `measurements` method.
        """
        pass


class Transform(VirtualProduct):
    """
    Apply some computation to the loaded data.
    """
    def __init__(self, source, transformation):
        self.source = source
        self.transformation = transformation

    def output_measurements(self, product_definitions):
        source_measurements = self.source.output_measurements(product_definitions)

        return self.transformation.measurements(source_measurements)

    def find_datasets(self, dc, **query):
        return self.source.find_datasets(dc, **query)

    def group_datasets(self, datasets, **query):
        return self.source.group_datasets(datasets, **query)

    def fetch_data(self, grouped, product_definitions, **load_settings):
        # validate data to be loaded
        _ = self.output_measurements(product_definitions)

        source_data = self.source.fetch_data(grouped, product_definitions, **load_settings)

        return self.transformation.compute(source_data)

    def __repr__(self):
        return ("Transform(source={}, transformation={})"
                .format(repr(self.source), repr(self.transformation)))


class Collate(VirtualProduct):
    def __init__(self, *children, index_measurement_name=None):
        if len(children) == 0:
            raise VirtualProductException("no children for collate node")

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
                msg = "child datasets do not all have the same set of measurements"
                raise VirtualProductException(msg)

        if self.index_measurement_name is None:
            return first

        if self.index_measurement_name in first:
            msg = "source index measurement '{}' already present".format(self.index_measurement_name)
            raise VirtualProductException(msg)

        return {**first, **self.index_measurement}

    def find_datasets(self, dc, **query):
        result = [child.find_datasets(dc, **query)
                  for child in self.children]

        grid_spec = select_unique([datasets.grid_spec for datasets in result])
        return DatasetPile(result, grid_spec)

    def group_datasets(self, datasets, **query):
        assert len(datasets.pile) == len(self.children)
        grid_spec = datasets.grid_spec

        def build(source_index, product, dataset_pile):
            grouped = product.group_datasets(dataset_pile, **query)

            def tag(_, value):
                in_position = [value if i == source_index else None
                               for i, _ in enumerate(self.children)]
                return DatasetPile(in_position, grid_spec)

            return grouped.map(tag)

        groups = [build(source_index, product, dataset_pile)
                  for source_index, (product, dataset_pile)
                  in enumerate(zip(self.children, datasets.pile))]

        geobox = select_unique([grouped.geobox for grouped in groups])

        concatenated = xarray.concat([grouped.pile for grouped in groups], dim='time')
        return GroupedDatasetPile(concatenated, geobox)

    def fetch_data(self, grouped, product_definitions, **load_settings):
        assert isinstance(grouped, GroupedDatasetPile)

        def is_from(source_index):
            def result(_, value):
                return value.pile[source_index] is not None

            return result

        def strip_source(_, value):
            for data in value.pile:
                if data is not None:
                    return data

            raise ValueError("Every child of GroupedDatasetPile object is None")

        def fetch_child(child, source_index, r):
            size = reduce(lambda x, y: x * y, r.shape, 1)

            if size > 0:
                result = child.fetch_data(r, product_definitions, **load_settings)
                name = self.index_measurement_name

                if name is None:
                    return result

                # implication for dask?
                measurement = self.index_measurement[name]
                shape = select_unique([result[band].shape for band in result.data_vars])
                array = numpy.full(shape, source_index, dtype=measurement.dtype)
                first = result[list(result.data_vars)[0]]
                result[name] = xarray.DataArray(array, dims=first.dims, coords=first.coords,
                                                name=name).assign_attrs(units=measurement.units,
                                                                        nodata=measurement.nodata)
                return result
            else:
                # empty raster
                return None

        # validate data to be loaded
        _ = self.output_measurements(product_definitions)

        groups = [fetch_child(child, source_index, grouped.filter(is_from(source_index)).map(strip_source))
                  for source_index, child in enumerate(self.children)]

        non_empty = [g for g in groups if g is not None]
        attrs = select_unique([g.attrs for g in non_empty])

        return xarray.concat(non_empty, dim='time').assign_attrs(**attrs)

    def __repr__(self):
        children = ",".join(repr(child) for child in self.children)
        return "Collate({}, index_measurement_name={})".format(children, self.index_measurement_name)


class Juxtapose(VirtualProduct):
    def __init__(self, *children):
        if len(children) == 0:
            raise VirtualProductException("no children for juxtapose node")

        self.children = children

    def output_measurements(self, product_definitions):
        input_measurements = [child.output_measurements(product_definitions)
                              for child in self.children]

        result = {}
        for measurements in input_measurements:
            common = set(result) & set(measurements)
            if common != set():
                msg = "common measurements {} between children".format(common)
                raise VirtualProductException(msg)

            result.update(measurements)

        return result

    def find_datasets(self, dc, **query):
        result = [child.find_datasets(dc, **query) for child in self.children]

        grid_spec = select_unique([datasets.grid_spec for datasets in result])
        return DatasetPile(result, grid_spec)

    def group_datasets(self, datasets, **query):
        assert len(datasets.pile) == len(self.children)

        pile = datasets.pile
        grid_spec = datasets.grid_spec

        groups = [product.group_datasets(datasets, **query)
                  for product, datasets in zip(self.children, pile)]

        geobox = select_unique([grouped.geobox for grouped in groups])

        aligned_piles = xarray.align(*[grouped.pile for grouped in groups])
        child_groups = [GroupedDatasetPile(aligned_piles[i], grouped.geobox)
                        for i, grouped in enumerate(groups)]

        def tuplify(indexes, _):
            return DatasetPile([grouped.pile.sel(**indexes).item()
                                for grouped in child_groups],
                               grid_spec)

        merged = child_groups[0].map(tuplify).pile

        return GroupedDatasetPile(merged, geobox)

    def fetch_data(self, grouped, product_definitions, **load_settings):
        assert isinstance(grouped, GroupedDatasetPile)
        geobox = grouped.geobox

        def select_child(source_index):
            def result(_, value):
                return value.pile[source_index]

            return result

        def fetch_recipe(source_index):
            child_groups = grouped.map(select_child(source_index))
            return GroupedDatasetPile(child_groups.pile, geobox)

        # validate data to be loaded
        _ = self.output_measurements(product_definitions)

        groups = [child.fetch_data(fetch_recipe(source_index), product_definitions, **load_settings)
                  for source_index, child in enumerate(self.children)]

        attrs = select_unique([g.attrs for g in groups])
        return xarray.merge(groups).assign_attrs(**attrs)

    def __repr__(self):
        children = ",".join(repr(child) for child in self.children)
        return "Juxtapose({})".format(children)


def product_definitions_from_index(index):
    return {product.name: product.definition
            for product in index.products.get_all()}


def select_unique(things):
    """ Checks that all the members of `things` are equal, and then returns it. """
    first, *rest = things
    for other in rest:
        if first != other:
            warnings.warn("select_unique may have failed: {} is not the same as {}"
                          .format(first, other))
            break

    return first


def select_keys(settings, keys):
    return {key: value
            for key, value in settings.items() if key in keys}


def reject_keys(settings, keys):
    return {key: value
            for key, value in settings.items() if key not in keys}


def merge_search_terms(original, override, keys=None):
    def pick(key, a, b):
        if a == b:
            return a

        if a is None:
            return b
        if b is None:
            return a

        # if they are ranges, take the intersection
        if isinstance(a, Range) and isinstance(b, Range):
            return Range(max(a.begin, b.begin), min(a.end, b.end))

        # trust the override
        return b

    return {key: pick(key, original.get(key), override.get(key))
            for key in list(original.keys()) + list(override.keys())
            if keys is None or key in keys}
