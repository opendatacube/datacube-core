# TODO: needs an aggregation phase (use xarray.DataArray.groupby?)
# TODO: measurement dependency tracking
# TODO: a mechanism to set settings for the leaf notes
# TODO: lineage tracking per observation
# TODO: integrate GridWorkflow functionality (spatial binning)

"""
Implementation of virtual products. Provides an interface for the products in the datacube
to query and to load data, and combinators to combine multiple products into "virtual"
products implementing the same interface.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from functools import reduce
from typing import Any, Dict, List, cast

import numpy
import xarray
import yaml

from datacube import Datacube
from datacube.api.core import select_datasets_inside_polygon, output_geobox, apply_aliases
from datacube.api.grid_workflow import _fast_slice
from datacube.api.query import Query, query_group_by, query_geopolygon
from datacube.model import Measurement, DatasetType
from datacube.model.utils import xr_apply

from .utils import qualified_name, merge_dicts
from .utils import select_unique, select_keys, reject_keys, merge_search_terms


class VirtualProductException(Exception):
    """ Raised if the construction of the virtual product cannot be validated. """
    pass


class QueryResult:
    """ Result of `VirtualProduct.query`. """
    def __init__(self, pile, grid_spec, geopolygon, product_definitions):
        self.pile = pile
        self.grid_spec = grid_spec
        self.geopolygon = geopolygon
        self.product_definitions = product_definitions


class DatasetPile:
    """ Result of `VirtualProduct.group`. """
    # our replacement for grid_workflow.Tile basically
    # TODO: copy the Tile API
    def __init__(self, pile, geobox, product_definitions):
        self.pile = pile
        self.geobox = geobox
        self.product_definitions = product_definitions

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
                           self.geobox[chunk[len(pile.shape):]],
                           self.product_definitions)

    def map(self, func, dtype='O'):
        return DatasetPile(xr_apply(self.pile, func, dtype=dtype), self.geobox, self.product_definitions)

    def filter(self, predicate):
        mask = self.map(predicate, dtype='bool')

        # NOTE: this could possibly result in an empty pile
        return DatasetPile(self.pile[mask.pile], self.geobox, self.product_definitions)

    def split(self, dim='time'):
        # this is slightly different from Tile.split
        pile = self.pile

        [length] = pile[dim].shape
        for i in range(length):
            yield DatasetPile(pile.isel(**{dim: slice(i, i + 1)}), self.geobox, self.product_definitions)


class Transformation(ABC):
    """
    A user-defined on-the-fly data transformation.

    The data coming in and out of the `compute` method are `xarray.Dataset` objects.
    The measurements are stored as `xarray.DataArray` objects inside it.

    The `measurements` method transforms the dictionary mapping measurement names
    to `datacube.model.Measurement` objects describing the input data
    into a dictionary describing the measurements of the output data
    produced by the `compute` method.
    """

    @abstractmethod
    def measurements(self, input_measurements) -> Dict[str, Measurement]:
        """
        Returns the dictionary describing the output measurements from this transformation.
        Assumes the `data` provided to `compute` will have measurements
        given by the dictionary `input_measurements`.
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
    _LOAD_KEYS = ['measurements', 'fuse_func', 'resampling', 'stack', 'dask_chunks']
    _ADDITIONAL_KEYS = ['dataset_predicate']

    _NON_SPATIAL_KEYS = _GEOBOX_KEYS + _GROUPING_KEYS
    _NON_QUERY_KEYS = _NON_SPATIAL_KEYS + _LOAD_KEYS + _ADDITIONAL_KEYS

    def __init__(self, settings: Dict[str, Any]) -> None:
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
    def _transformation(self) -> Transformation:
        """ The `Transformation` object associated with a transform product. """
        cls = self['transform']

        try:
            obj = cls(**{key: value for key, value in self.items() if key not in ['transform', 'input']})
        except TypeError:
            raise VirtualProductException("transformation {} could not be instantiated".format(cls))

        self._assert(isinstance(obj, Transformation), "not a transformation object: {}".format(obj))

        return cast(Transformation, obj)

    @property
    def _input(self) -> 'VirtualProduct':
        """ The input product of a transform product. """
        return VirtualProduct(self['input'])

    @property
    def _children(self) -> List['VirtualProduct']:
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

    @property
    def _kind(self):
        """ One of product, transform, collate, or juxtapose. """
        candidates = [key for key in list(self) if key in ['product', 'transform', 'collate', 'juxtapose']]
        self._assert(len(candidates) == 1, "ambiguous kind")
        return candidates[0]

    # public interface

    def output_measurements(self, product_definitions: Dict[str, DatasetType]) -> Dict[str, Measurement]:
        """
        A dictionary mapping names to measurement metadata.
        :param product_definitions: a dictionary mapping product names to products (`DatasetType` objects)
        """
        get = self.get

        if 'product' in self:
            self._assert(self._product in product_definitions,
                         "product {} not found in definitions".format(self._product))

            product = product_definitions[self._product]
            measurements = {measurement['name']: Measurement(**measurement)
                            for measurement in product.definition['measurements']}

            if get('measurements') is None:
                return measurements

            try:
                return {name: measurements[product.canonical_measurement(name)]
                        for name in self['measurements']}
            except KeyError as ke:
                raise VirtualProductException("could not find measurement: {}".format(ke.args))

        elif 'transform' in self:
            input_measurements = self._input.output_measurements(product_definitions)

            return self._transformation.measurements(input_measurements)

        elif 'collate' in self:
            input_measurement_list = [child.output_measurements(product_definitions)
                                      for child in self._children]

            first, *rest = input_measurement_list

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
            input_measurement_list = [child.output_measurements(product_definitions)
                                      for child in self._children]

            result = cast(Dict[str, Measurement], {})
            for measurements in input_measurement_list:
                common = set(result) & set(measurements)
                self._assert(not common, "common measurements {} between children".format(common))

                result.update(measurements)

            return result

        else:
            raise VirtualProductException("virtual product was not validated")

    def query(self, dc: Datacube, **search_terms: Dict[str, Any]) -> QueryResult:
        """ Collection of datasets that match the query. """
        get = self.get

        if 'product' in self:
            product = dc.index.products.get_by_name(self._product)
            if product is None:
                raise VirtualProductException("could not find product {}".format(self._product))

            originals = Query(dc.index, **reject_keys(self, self._NON_QUERY_KEYS))
            overrides = Query(dc.index, **reject_keys(search_terms, self._NON_QUERY_KEYS))

            query = Query(dc.index, **merge_search_terms(originals.search_terms, overrides.search_terms))
            self._assert(query.product == self._product,
                         "query for {} returned another product {}".format(self._product, query.product))

            # find the datasets
            datasets = dc.index.datasets.search(**query.search_terms)
            if query.geopolygon is not None:
                datasets = select_datasets_inside_polygon(datasets, query.geopolygon)

            # should we put it in the Transformation class?
            if get('dataset_predicate') is not None:
                datasets = [dataset
                            for dataset in datasets
                            if self['dataset_predicate'](dataset)]

            return QueryResult(list(datasets), product.grid_spec, query.geopolygon,
                               {product.name: product})

        elif 'transform' in self:
            return self._input.query(dc, **search_terms)

        elif 'collate' in self or 'juxtapose' in self:
            result = [child.query(dc, **search_terms)
                      for child in self._children]

            return QueryResult({self._kind: [datasets.pile for datasets in result]},
                               select_unique([datasets.grid_spec for datasets in result]),
                               select_unique([datasets.geopolygon for datasets in result]),
                               merge_dicts([datasets.product_definitions for datasets in result]))

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
        geopolygon = datasets.geopolygon

        if 'product' in self:
            # select only those inside the ROI
            # ROI could be smaller than the query for the `query` method
            if query_geopolygon(**search_terms) is not None:
                geopolygon = query_geopolygon(**search_terms)
                selected = list(select_datasets_inside_polygon(datasets.pile, geopolygon))
            else:
                selected = list(datasets.pile)

            # geobox
            merged = merge_search_terms(select_keys(self, self._NON_SPATIAL_KEYS),
                                        select_keys(search_terms, self._NON_SPATIAL_KEYS))

            geobox = output_geobox(datasets=selected, grid_spec=grid_spec,
                                   geopolygon=geopolygon, **select_keys(merged, self._GEOBOX_KEYS))

            # group by time
            group_query = query_group_by(**select_keys(merged, self._GROUPING_KEYS))

            # information needed for Datacube.load_data
            return DatasetPile(Datacube.group_datasets(selected, group_query),
                               geobox,
                               datasets.product_definitions)

        elif 'transform' in self:
            return self._input.group(datasets, **search_terms)

        elif 'collate' in self:
            self._assert('collate' in datasets.pile and len(datasets.pile['collate']) == len(self._children),
                         "invalid dataset pile")

            def build(source_index, product, dataset_pile):
                grouped = product.group(QueryResult(dataset_pile, datasets.grid_spec,
                                                    datasets.geopolygon, datasets.product_definitions),
                                        **search_terms)

                def tag(_, value):
                    return {'collate': (source_index, value)}

                return grouped.map(tag)

            groups = [build(source_index, product, dataset_pile)
                      for source_index, (product, dataset_pile)
                      in enumerate(zip(self._children, datasets.pile['collate']))]

            return DatasetPile(xarray.concat([grouped.pile for grouped in groups], dim='time'),
                               select_unique([grouped.geobox for grouped in groups]),
                               merge_dicts([grouped.product_definitions for grouped in groups]))

        elif 'juxtapose' in self:
            self._assert('juxtapose' in datasets.pile and len(datasets.pile['juxtapose']) == len(self._children),
                         "invalid dataset pile")

            groups = [product.group(QueryResult(dataset_pile, datasets.grid_spec,
                                                datasets.geopolygon, datasets.product_definitions),
                                    **search_terms)
                      for product, dataset_pile in zip(self._children, datasets.pile['juxtapose'])]

            aligned_piles = xarray.align(*[grouped.pile for grouped in groups])

            def tuplify(indexes, _):
                return {'juxtapose': [pile.sel(**indexes).item() for pile in aligned_piles]}

            return DatasetPile(xr_apply(aligned_piles[0], tuplify),
                               select_unique([grouped.geobox for grouped in groups]),
                               merge_dicts([grouped.product_definitions for grouped in groups]))

        else:
            raise VirtualProductException("virtual product was not validated")

    def fetch(self, grouped: DatasetPile, **load_settings: Dict[str, Any]) -> xarray.Dataset:
        """ Convert grouped datasets to `xarray.Dataset`. """
        # TODO: provide `load_lazy` and `load_strict` instead

        # validate data to be loaded
        product_definitions = grouped.product_definitions
        _ = self.output_measurements(product_definitions)

        if 'product' in self:
            merged = merge_search_terms(select_keys(self, self._LOAD_KEYS),
                                        select_keys(load_settings, self._LOAD_KEYS))

            # load_settings should not contain `measurements` for now
            measurements = self.output_measurements(product_definitions)

            result = Datacube.load_data(grouped.pile,
                                        grouped.geobox, list(measurements.values()),
                                        fuse_func=merged.get('fuse_func'),
                                        dask_chunks=merged.get('dask_chunks'))

            return apply_aliases(result, product_definitions[self._product], list(measurements))

        elif 'transform' in self:
            return self._transformation.compute(self._input.fetch(grouped, **load_settings))

        elif 'collate' in self:
            def is_from(source_index):
                def result(_, value):
                    self._assert('collate' in value, "malformed dataset pile in collate")
                    return value['collate'][0] == source_index

                return result

            def strip_source(_, value):
                return value['collate'][1]

            def fetch_child(child, source_index, r):
                size = reduce(lambda x, y: x * y, r.shape, 1)

                if size > 0:
                    result = child.fetch(r, **load_settings)
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
                    self._assert('juxtapose' in value, "malformed dataset pile in juxtapose")
                    return value['juxtapose'][source_index]

                return result

            def fetch_recipe(source_index):
                child_groups = grouped.map(select_child(source_index))
                return DatasetPile(child_groups.pile, grouped.geobox, grouped.product_definitions)

            groups = [child.fetch(fetch_recipe(source_index), **load_settings)
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
        datasets = self.query(dc, **query)
        grouped = self.group(datasets, **query)
        return self.fetch(grouped, **query).sortby('time')
