# TODO: alias support
# TODO: needs an aggregation phase (use xarray.DataArray.groupby?)
# TODO: measurement dependency tracking
# TODO: ability to select measurements higher up in the tree
# TODO: a mechanism for set settings for the leaf notes
# TODO: lineage tracking per observation
# TODO: integrate GridWorkflow functionality

"""
Implementation of virtual products. Provides an interface for the products in the datacube
to query and to load data, and combinators to combine multiple products into "virtual"
products implementing the same interface.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from functools import reduce

import numpy
import xarray
import yaml

from datacube import Datacube
from datacube.api.core import select_datasets_inside_polygon, output_geobox
from datacube.api.grid_workflow import _fast_slice
from datacube.api.query import Query, query_group_by, query_geopolygon
from datacube.model import Measurement
from datacube.model.utils import xr_apply
from datacube.utils import import_function

from .utils import product_definitions_from_index, qualified_name
from .utils import select_unique, select_keys, reject_keys, merge_search_terms


class VirtualProductException(Exception):
    """ Raised if the construction of the virtual product cannot be validated. """
    pass


class QueryResult:
    """ Result of `VirtualProduct.query`. """
    def __init__(self, pile, grid_spec):
        self.pile = tuple(pile)
        self.grid_spec = grid_spec


class DatasetPile:
    """ Result of `VirtualProduct.group`. """
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

        return DatasetPile(_fast_slice(pile, chunk[:len(pile.shape)]),
                           self.geobox[chunk[len(pile.shape):]])

    def map(self, func, dtype='O'):
        return DatasetPile(xr_apply(self.pile, func, dtype=dtype), self.geobox)

    def filter(self, predicate):
        mask = self.map(predicate, dtype='bool')

        # NOTE: this could possibly result in an empty pile
        return DatasetPile(self.pile[mask.pile], self.geobox)

    def split(self, dim='time'):
        # this is slightly different from Tile.split
        pile = self.pile

        [length] = pile[dim].shape
        for i in range(length):
            yield DatasetPile(pile.isel(**{dim: slice(i, i + 1)}), self.geobox)


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


class VirtualProduct(Mapping):
    """
    A recipe for combining loaded data from multiple datacube products.

    Basic combinators are:
        - product: an existing datacube product
        - transform: on-the-fly computation on data being loaded
        - collate: stack observations from products with the same set of measurements
        - juxtapose: put measurements from different products side-by-side
    """

    _GEOBOX_KEYS = ['output_crs', 'resolution', 'align']
    _GROUPING_KEYS = ['group_by']
    _LOAD_KEYS = ['measurements', 'fuse_func', 'resampling', 'stack', 'dask_chunks', 'use_threads']
    _ADDITIONAL_KEYS = ['dataset_predicate']

    _NON_SPATIAL_KEYS = _GEOBOX_KEYS + _GROUPING_KEYS
    _NON_QUERY_KEYS = _NON_SPATIAL_KEYS + _LOAD_KEYS + _ADDITIONAL_KEYS

    def __init__(self, settings):
        # type: (Dict[str, Any]) -> None
        """
        :param settings: validated and reference-resolved recipe
        """
        self._settings = settings

    # helper methods

    def __getitem__(self, key):
        return self._settings[key]

    def __len__(self):
        return len(self._settings)

    def __iter__(self):
        return iter(self._settings)

    @staticmethod
    def _assert(cond, msg):
        if not cond:
            raise VirtualProductException(msg)

    @property
    def _transformation(self):
        """ The `Transformation` object associated with a transform product. """
        cls = self['transform']

        obj = cls(**{key: value for key, value in self.items() if key not in ['transform', 'input']})
        self._assert(isinstance(obj, Transformation), "not a transformation object: {}".format(obj))

        return obj

    @property
    def _input(self):
        """ The input product of a transform product. """
        return VirtualProduct(self['input'])

    @property
    def _children(self):
        """ The children of a collate or a juxtapose product. """
        if 'collate' in self:
            return [VirtualProduct(child) for child in self['collate']]

        elif 'juxtapose' in self:
            return [VirtualProduct(child) for child in self['juxtapose']]

        else:
            raise VirtualProductException("not a collate or juxtapose product")

    @property
    def _product(self):
        """ The name of an existing datacube product. """
        return self['product']

    # public interface

    def output_measurements(self, product_definitions):
        # type: (Dict[str, Dict]) -> Dict[str, Measurement]
        """
        A dictionary mapping names to measurement metadata.
        :param product_definitions: a dictionary mapping product names to definitions
        """
        get = self.get

        if 'product' in self:
            self._assert(self._product in product_definitions,
                         "product {} not found in definitions".format(self._product))

            measurement_docs = product_definitions[self._product]['measurements']
            measurements = {measurement['name']: Measurement(**measurement)
                            for measurement in measurement_docs}

            if get('measurements') is None:
                return measurements

            try:
                return {name: measurements[name] for name in get('measurements')}
            except KeyError as ke:
                raise VirtualProductException("could not find measurement: {}".format(ke.args))

        elif 'transform' in self:
            input_measurements = self._input.output_measurements(product_definitions)

            return self._transformation.measurements(input_measurements)

        elif 'collate' in self:
            input_measurements = [child.output_measurements(product_definitions)
                                  for child in self._children]

            first, *rest = input_measurements

            for child in rest:
                self._assert(set(child) == set(first),
                             "child datasets do not all have the same set of measurements")

            name = get('index_measurement_name')
            if name is None:
                return first

            self._assert(name not in first, "source index measurement '{}' already present".format(name))

            first.update({name: Measurement(name=name, dtype='int8', nodata=-1, units='1')})
            return first

        elif 'juxtapose' in self:
            input_measurements = [child.output_measurements(product_definitions)
                                  for child in self._children]

            result = {}
            for measurements in input_measurements:
                common = set(result) & set(measurements)
                self._assert(not common, "common measurements {} between children".format(common))

                result.update(measurements)

            return result

        else:
            raise VirtualProductException("virtual product was not validated")

    def query(self, dc, **search_terms):
        # type: (Datacube, Dict[str, Any]) -> QueryResult
        """ Collection of datasets that match the query. """
        get = self.get

        if 'product' in self:
            originals = Query(dc.index, **reject_keys(self, self._NON_QUERY_KEYS))
            overrides = Query(dc.index, **reject_keys(search_terms, self._NON_QUERY_KEYS))

            query = Query(dc.index, **merge_search_terms(originals.search_terms, overrides.search_terms))
            self._assert(query.product == self._product,
                         "query for {} returned another product {}".format(self._product, query.product))

            # find the datasets
            datasets = select_datasets_inside_polygon(dc.index.datasets.search(**query.search_terms),
                                                      query.geopolygon)

            if get('dataset_predicate') is not None:
                datasets = [dataset
                            for dataset in datasets
                            if get('dataset_predicate')(dataset)]

            # gather information from the index before it disappears from sight
            # this can also possibly extracted from the product definitions but this is easier
            grid_spec = dc.index.products.get_by_name(self._product).grid_spec

            return QueryResult(datasets, grid_spec)

        elif 'transform' in self:
            return self._input.query(dc, **search_terms)

        elif 'collate' in self or 'juxtapose' in self:
            result = [child.query(dc, **search_terms)
                      for child in self._children]

            grid_spec = select_unique([datasets.grid_spec for datasets in result])
            return QueryResult(result, grid_spec)

        else:
            raise VirtualProductException("virtual product was not validated")

    # no index access below this line

    def group(self, datasets, **search_terms):
        # type: (QueryResult, Dict[str, Any]) -> DatasetPile
        """
        Datasets grouped by their timestamps.
        :param datasets: the `QueryResult` to fetch data from
        :param query: to specify a spatial sub-region
        """
        grid_spec = datasets.grid_spec

        if 'product' in self:
            # select only those inside the ROI
            # ROI could be smaller than the query for `query`
            spatial_query = reject_keys(search_terms, self._NON_SPATIAL_KEYS)
            selected = list(select_datasets_inside_polygon(datasets.pile,
                                                           query_geopolygon(**spatial_query)))

            # geobox
            merged = merge_search_terms(select_keys(self, self._NON_SPATIAL_KEYS),
                                        select_keys(spatial_query, self._NON_SPATIAL_KEYS))

            geobox = output_geobox(datasets=selected, grid_spec=grid_spec,
                                   **select_keys(merged, self._GEOBOX_KEYS), **spatial_query)

            # group by time
            group_query = query_group_by(**select_keys(merged, self._GROUPING_KEYS))

            def wrap(_, value):
                return QueryResult(value, grid_spec)

            # information needed for Datacube.load_data
            return DatasetPile(Datacube.group_datasets(selected, group_query), geobox).map(wrap)

        elif 'transform' in self:
            return self._input.group(datasets, **search_terms)

        elif 'collate' in self:
            self._assert(len(datasets.pile) == len(self._children), "invalid dataset pile")

            def build(source_index, product, dataset_pile):
                grouped = product.group(dataset_pile, **search_terms)

                def tag(_, value):
                    in_position = [value if i == source_index else None
                                   for i, _ in enumerate(datasets.pile)]
                    return QueryResult(in_position, grid_spec)

                return grouped.map(tag)

            groups = [build(source_index, product, dataset_pile)
                      for source_index, (product, dataset_pile)
                      in enumerate(zip(self._children, datasets.pile))]

            return DatasetPile(xarray.concat([grouped.pile for grouped in groups], dim='time'),
                               select_unique([grouped.geobox for grouped in groups]))

        elif 'juxtapose' in self:
            self._assert(len(datasets.pile) == len(self._children), "invalid dataset pile")

            groups = [product.group(datasets, **search_terms)
                      for product, datasets in zip(self._children, datasets.pile)]

            aligned_piles = xarray.align(*[grouped.pile for grouped in groups])
            child_groups = [DatasetPile(aligned_piles[i], grouped.geobox)
                            for i, grouped in enumerate(groups)]

            def tuplify(indexes, _):
                return QueryResult([grouped.pile.sel(**indexes).item() for grouped in child_groups],
                                   grid_spec)

            return DatasetPile(child_groups[0].map(tuplify).pile,
                               select_unique([grouped.geobox for grouped in groups]))

        else:
            raise VirtualProductException("virtual product was not validated")

    def fetch(self, grouped, product_definitions, **load_settings):
        # type: (DatasetPile, Dict[str, Dict], Dict[str, Any]) -> xarray.Dataset
        """ Convert grouped datasets to `xarray.Dataset`. """
        # TODO: provide `load_lazy` and `load_strict` instead

        # validate data to be loaded
        _ = self.output_measurements(product_definitions)

        if 'product' in self:
            merged = merge_search_terms(select_keys(self, self._LOAD_KEYS),
                                        select_keys(load_settings, self._LOAD_KEYS))

            # load_settings should not contain `measurements`
            measurements = list(self.output_measurements(product_definitions).values())

            def unwrap(_, value):
                return value.pile

            return Datacube.load_data(grouped.map(unwrap).pile,
                                      grouped.geobox, measurements,
                                      fuse_func=merged.get('fuse_func'),
                                      dask_chunks=merged.get('dask_chunks'),
                                      use_threads=merged.get('use_threads'))

        elif 'transform' in self:
            return self._transformation.compute(self._input.fetch(grouped, product_definitions, **load_settings))

        elif 'collate' in self:
            def is_from(source_index):
                def result(_, value):
                    return value.pile[source_index] is not None

                return result

            def strip_source(_, value):
                for data in value.pile:
                    if data is not None:
                        return data

                raise ValueError("Every child of DatasetPile object is None")

            def fetch_child(child, source_index, r):
                size = reduce(lambda x, y: x * y, r.shape, 1)

                if size > 0:
                    result = child.fetch(r, product_definitions, **load_settings)
                    name = self.get('index_measurement_name')

                    if name is None:
                        return result

                    # implication for dask?
                    measurement = Measurement(name=name, dtype='int8', nodata=-1, units='1')
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

            groups = [fetch_child(child, source_index, grouped.filter(is_from(source_index)).map(strip_source))
                      for source_index, child in enumerate(self._children)]

            non_empty = [g for g in groups if g is not None]

            return xarray.concat(non_empty, dim='time').assign_attrs(**select_unique([g.attrs for g in non_empty]))

        elif 'juxtapose' in self:
            def select_child(source_index):
                def result(_, value):
                    return value.pile[source_index]

                return result

            def fetch_recipe(source_index):
                child_groups = grouped.map(select_child(source_index))
                return DatasetPile(child_groups.pile, grouped.geobox)

            groups = [child.fetch(fetch_recipe(source_index), product_definitions, **load_settings)
                      for source_index, child in enumerate(self._children)]

            return xarray.merge(groups).assign_attrs(**select_unique([g.attrs for g in groups]))

        else:
            raise VirtualProductException("virtual product was not validated")

    def __str__(self):
        """ Reconstruct the recipe. """

        def reconstruct(product):
            if 'product' in product:
                return {key: value if key not in ['fuse_func', 'dataset_predicate'] else qualified_name(value)
                        for key, value in product.items()}

            if 'transform' in product:
                input_product = reconstruct(product['input'])
                return dict(transform=qualified_name(product['transform']),
                            input=input_product, **reject_keys(product, ['input', 'transform']))

            if 'collate' in product:
                children = [reconstruct(child) for child in product['collate']]
                return dict(collate=children, **reject_keys(product, ['collate']))

            if 'juxtapose' in product:
                children = [reconstruct(child) for child in product['juxtapose']]
                return dict(juxtapose=children, **reject_keys(product, ['juxtapose']))

            else:
                raise VirtualProductException("virtual product was not validated")

        return yaml.dump(reconstruct(self), Dumper=yaml.CDumper,
                         default_flow_style=False, indent=2)

    def load(self, dc, **query):
        # type: (Datacube, Dict[str, Any]) -> xarray.Dataset
        """ Mimic `datacube.Datacube.load`. For illustrative purposes. May be removed in the future. """
        product_definitions = product_definitions_from_index(dc.index)
        datasets = self.query(dc, **query)
        grouped = self.group(datasets, **query)
        return self.fetch(grouped, product_definitions, **query).sortby('time')
