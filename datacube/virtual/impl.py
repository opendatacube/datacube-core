# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Implementation of virtual products. Provides an interface for the products in the datacube
to query and to load data, and combinators to combine multiple products into "virtual"
products implementing the same interface.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from functools import reduce
from typing import Any, Dict, List, cast

import uuid
import numpy
import xarray
import dask.array
from dask.core import flatten
import yaml

from datacube import Datacube
from datacube.api.core import select_datasets_inside_polygon, output_geobox
from datacube.api.grid_workflow import _fast_slice
from datacube.api.query import Query, query_group_by
from datacube.model import Measurement, DatasetType
from datacube.model.utils import xr_apply, xr_iter, SafeDumper
from datacube.testutils.io import native_geobox
from datacube.utils.geometry import GeoBox, rio_reproject, geobox_union_conservative
from datacube.utils.geometry import compute_reproject_roi
from datacube.utils.geometry.gbox import GeoboxTiles
from datacube.utils.geometry._warp import resampling_s2rio
from datacube.api.core import per_band_load_data_settings
try:
    from odc.algo import xr_reproject
except ImportError as e:
    no_reproject = True
else:
    no_reproject = False

from .utils import qualified_name, merge_dicts
from .utils import select_unique, select_keys, reject_keys, merge_search_terms


class VirtualProductException(Exception):
    """ Raised if the construction of the virtual product cannot be validated. """


class VirtualDatasetBag:
    """ Result of `VirtualProduct.query`. """
    def __init__(self, bag, geopolygon, product_definitions):
        self.bag = bag
        self.geopolygon = geopolygon
        self.product_definitions = product_definitions

    def contained_datasets(self):
        def worker(bag):
            if isinstance(bag, Sequence):
                for child in bag:
                    yield child

            elif isinstance(bag, Mapping):
                # there should only be one key (collate or juxtapose)
                for key in bag:
                    # bag[key] should be a list
                    for child in bag[key]:
                        yield from worker(child)

            else:
                raise VirtualProductException("unexpected bag")

        return worker(self.bag)

    def explode(self):
        def worker(bag):
            if isinstance(bag, Sequence):
                for child in bag:
                    yield [child]

            elif isinstance(bag, Mapping):
                if 'juxtapose' in bag:
                    # too hard, giving up
                    raise NotImplementedError

                for index, child_bag in enumerate(bag['collate']):
                    for child in worker(child_bag):
                        yield {'collate': [child if i == index else []
                                           for i, _ in enumerate(bag['collate'])]}

            else:
                raise VirtualProductException("unexpected bag")

        for child in worker(self.bag):
            yield VirtualDatasetBag(child, self.geopolygon, self.product_definitions)

    def __repr__(self):
        return "<VirtualDatasetBag of {} datacube datasets>".format(len(list(self.contained_datasets())))


class VirtualDatasetBox:
    """ Result of `VirtualProduct.group`. """

    def __init__(self, box, geobox, load_natively, product_definitions, geopolygon=None):
        if not load_natively and geobox is None:
            raise VirtualProductException("VirtualDatasetBox has no geobox")
        if not load_natively and geopolygon is not None:
            raise VirtualProductException("unexpected geopolygon for VirtualDatasetBox")

        self.box = box
        self.geobox = geobox
        self.load_natively = load_natively
        self.product_definitions = product_definitions
        self.geopolygon = geopolygon

    def __repr__(self):
        if not self.load_natively:
            return "<VirtualDatasetBox of shape {}>".format(dict(zip(self.dims, self.shape)))

        return "<natively loaded VirtualDatasetBox>"

    @property
    def dims(self):
        """
        Names of the dimensions, e.g., ``('time', 'y', 'x')``.
        :return: tuple(str)
        """
        if self.load_natively:
            raise VirtualProductException("dims requires known geobox")

        return self.box.dims + self.geobox.dimensions

    @property
    def shape(self):
        """
        Lengths of each dimension, e.g., ``(285, 4000, 4000)``.
        :return: tuple(int)
        """
        if self.load_natively:
            raise VirtualProductException("shape requires known geobox")

        return self.box.shape + self.geobox.shape

    def __getitem__(self, chunk):
        if self.load_natively:
            raise VirtualProductException("slicing requires known geobox")

        # TODO implement this properly
        box = self.box

        return VirtualDatasetBox(_fast_slice(box, chunk[:len(box.shape)]),
                                 self.geobox[chunk[len(box.shape):]],
                                 self.load_natively,
                                 self.product_definitions,
                                 geopolygon=self.geopolygon)

    def map(self, func, dtype='O'):
        return VirtualDatasetBox(xr_apply(self.box, func, dtype=dtype),
                                 self.geobox,
                                 self.load_natively,
                                 self.product_definitions,
                                 geopolygon=self.geopolygon)

    def filter(self, predicate):
        mask = self.map(predicate, dtype='bool')

        # NOTE: this could possibly result in an empty box
        return VirtualDatasetBox(self.box[mask.box], self.geobox, self.load_natively, self.product_definitions,
                                 geopolygon=self.geopolygon)

    def split(self, dim='time'):
        box = self.box

        [length] = box[dim].shape
        for i in range(length):
            yield VirtualDatasetBox(box.isel(**{dim: slice(i, i + 1)}),
                                    self.geobox,
                                    self.load_natively,
                                    self.product_definitions,
                                    geopolygon=self.geopolygon)

    def input_datasets(self):
        def traverse(entry):
            if isinstance(entry, Mapping):
                if 'collate' in entry:
                    _, child = entry['collate']
                    yield from traverse(child)
                elif 'juxtapose' in entry:
                    for child in entry['juxtapose']:
                        yield from traverse(child)
                else:
                    raise VirtualProductException("malformed box")

            elif isinstance(entry, Sequence):
                yield from entry

            elif isinstance(entry, VirtualDatasetBox):
                for _, _, child in xr_iter(entry.input_datasets()):
                    yield from child

            else:
                raise VirtualProductException("malformed box")

        def worker(index, entry):
            return set(traverse(entry))

        return self.map(worker).box


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

    @abstractmethod
    def compute(self, data):
        """
        Perform computation on `data` that results in an `xarray.Dataset`
        having measurements reported by the `measurements` method.
        """


class VirtualProduct(Mapping):
    """
    A recipe for combining loaded data from multiple datacube products.

    Basic combinators are:
        - product: an existing datacube product
        - transform: on-the-fly computation on data being loaded
        - collate: stack observations from products with the same set of measurements
        - juxtapose: put measurements from different products side-by-side
        - aggregate: take (non-spatial) statistics of grouped data
        - reproject: on-the-fly reprojection of raster data
    """

    _GEOBOX_KEYS = {'output_crs', 'resolution', 'align'}
    _GROUPING_KEYS = {'group_by'}
    _LOAD_KEYS = {'measurements', 'fuse_func', 'resampling', 'dask_chunks', 'like'}
    _ADDITIONAL_KEYS = {'dataset_predicate'}

    _NON_SPATIAL_KEYS = _GEOBOX_KEYS | _GROUPING_KEYS
    _NON_QUERY_KEYS = _NON_SPATIAL_KEYS | _LOAD_KEYS | _ADDITIONAL_KEYS

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

    # public interface

    def __init__(self, settings: Dict[str, Any]) -> None:
        """
        :param settings: validated and reference-resolved recipe
        """
        self._settings = settings

    def output_measurements(self, product_definitions: Dict[str, DatasetType]) -> Dict[str, Measurement]:
        """
        A dictionary mapping names to measurement metadata.
        :param product_definitions: a dictionary mapping product names to products (`DatasetType` objects)
        """
        raise NotImplementedError

    def query(self, dc: Datacube, **search_terms: Dict[str, Any]) -> VirtualDatasetBag:
        """ Collection of datasets that match the query. """
        raise NotImplementedError

    # no index access below this line

    def group(self, datasets: VirtualDatasetBag, **group_settings: Dict[str, Any]) -> VirtualDatasetBox:
        """
        Datasets grouped by their timestamps.
        :param datasets: the `VirtualDatasetBag` to fetch data from
        """
        raise NotImplementedError

    def fetch(self, grouped: VirtualDatasetBox, **load_settings: Dict[str, Any]) -> xarray.Dataset:
        """ Convert grouped datasets to `xarray.Dataset`. """
        raise NotImplementedError

    def __repr__(self):
        return yaml.dump(self._reconstruct(), Dumper=SafeDumper,
                         default_flow_style=False, indent=2)

    def load(self, dc: Datacube, **query: Dict[str, Any]) -> xarray.Dataset:
        """ Mimic `datacube.Datacube.load`. For illustrative purposes. May be removed in the future. """
        datasets = self.query(dc, **query)
        grouped = self.group(datasets, **query)
        return self.fetch(grouped, **query)


class Product(VirtualProduct):
    """ An existing datacube product. """

    @property
    def _product(self):
        """ The name of an existing datacube product. """
        return self['product']

    def _reconstruct(self):
        return {key: value if key not in ['fuse_func', 'dataset_predicate'] else qualified_name(value)
                for key, value in self.items()}

    def output_measurements(self, product_definitions: Dict[str, DatasetType],
                            measurements: List[str] = None) -> Dict[str, Measurement]:
        self._assert(self._product in product_definitions,
                     "product {} not found in definitions".format(self._product))

        if measurements is None:
            measurements = self.get('measurements')

        product = product_definitions[self._product]
        return product.lookup_measurements(measurements)

    def query(self, dc: Datacube, **search_terms: Dict[str, Any]) -> VirtualDatasetBag:
        product = dc.index.products.get_by_name(self._product)
        if product is None:
            raise VirtualProductException("could not find product {}".format(self._product))

        originals = reject_keys(self, self._NON_QUERY_KEYS)
        overrides = reject_keys(search_terms, self._NON_QUERY_KEYS)

        query = Query(dc.index, **merge_search_terms(originals, overrides))
        self._assert(query.product == self._product,
                     "query for {} returned another product {}".format(self._product, query.product))

        # find the datasets
        datasets = (dataset for dataset in dc.index.datasets.search(**query.search_terms) if dataset.uris)

        if query.geopolygon is not None:
            datasets = select_datasets_inside_polygon(datasets, query.geopolygon)

        # should we put it in the Transformation class?
        if self.get('dataset_predicate') is not None:
            datasets = [dataset
                        for dataset in datasets
                        if self['dataset_predicate'](dataset)]

        return VirtualDatasetBag(list(datasets), query.geopolygon,
                                 {product.name: product})

    def group(self, datasets: VirtualDatasetBag, **group_settings: Dict[str, Any]) -> VirtualDatasetBox:
        geopolygon = datasets.geopolygon
        selected = list(datasets.bag)

        # geobox
        merged = merge_search_terms(self, group_settings)

        try:
            geobox = output_geobox(datasets=selected,
                                   grid_spec=datasets.product_definitions[self._product].grid_spec,
                                   geopolygon=geopolygon, **select_keys(merged, self._GEOBOX_KEYS))
            load_natively = False

        except ValueError:
            # we are not calculating geoboxes here for the moment
            # since it may require filesystem access
            # in ODC 2.0 the dataset should know the information required
            geobox = None
            load_natively = True

        # group by time
        group_query = query_group_by(**select_keys(merged, self._GROUPING_KEYS))

        # information needed for Datacube.load_data
        return VirtualDatasetBox(Datacube.group_datasets(selected, group_query),
                                 geobox,
                                 load_natively,
                                 datasets.product_definitions,
                                 geopolygon=None if not load_natively else geopolygon)

    def fetch(self, grouped: VirtualDatasetBox, **load_settings: Dict[str, Any]) -> xarray.Dataset:
        """ Convert grouped datasets to `xarray.Dataset`. """

        load_keys = self._LOAD_KEYS - {'measurements'}
        merged = merge_search_terms(select_keys(self, load_keys),
                                    select_keys(load_settings, load_keys))

        product = grouped.product_definitions[self._product]

        if 'measurements' in self and 'measurements' in load_settings:
            for measurement in load_settings['measurements']:
                self._assert(measurement in self['measurements'],
                             '{} not found in {}'.format(measurement, self._product))

        measurement_dicts = self.output_measurements(grouped.product_definitions,
                                                     load_settings.get('measurements'))

        if grouped.load_natively:
            canonical_names = [product.canonical_measurement(measurement) for measurement in measurement_dicts]
            dataset_geobox = geobox_union_conservative([native_geobox(ds,
                                                                      measurements=canonical_names,
                                                                      basis=merged.get('like'))
                                                        for ds in grouped.box.sum().item()])

            if grouped.geopolygon is not None:
                reproject_roi = compute_reproject_roi(dataset_geobox,
                                                      GeoBox.from_geopolygon(grouped.geopolygon,
                                                                             crs=dataset_geobox.crs,
                                                                             align=dataset_geobox.alignment,
                                                                             resolution=dataset_geobox.resolution))

                self._assert(reproject_roi.is_st, "native load is not axis-aligned")
                self._assert(numpy.isclose(reproject_roi.scale, 1.0), "native load should not require scaling")

                geobox = dataset_geobox[reproject_roi.roi_src]
            else:
                geobox = dataset_geobox
        else:
            geobox = grouped.geobox

        result = Datacube.load_data(grouped.box,
                                    geobox, list(measurement_dicts.values()),
                                    fuse_func=merged.get('fuse_func'),
                                    dask_chunks=merged.get('dask_chunks'),
                                    resampling=merged.get('resampling', 'nearest'))

        return result


class Transform(VirtualProduct):
    """ An on-the-fly transformation. """

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
    def _input(self) -> VirtualProduct:
        """ The input product of a transform product. """
        return from_validated_recipe(self['input'])

    def _reconstruct(self):
        # pylint: disable=protected-access
        return dict(transform=qualified_name(self['transform']),
                    input=self._input._reconstruct(), **reject_keys(self, ['input', 'transform']))

    def output_measurements(self, product_definitions: Dict[str, DatasetType]) -> Dict[str, Measurement]:
        input_measurements = self._input.output_measurements(product_definitions)

        return self._transformation.measurements(input_measurements)

    def query(self, dc: Datacube, **search_terms: Dict[str, Any]) -> VirtualDatasetBag:
        return self._input.query(dc, **search_terms)

    def group(self, datasets: VirtualDatasetBag, **group_settings: Dict[str, Any]) -> VirtualDatasetBox:
        return self._input.group(datasets, **group_settings)

    def fetch(self, grouped: VirtualDatasetBox, **load_settings: Dict[str, Any]) -> xarray.Dataset:
        input_data = self._input.fetch(grouped, **load_settings)
        output_data = self._transformation.compute(input_data)
        output_data.attrs['crs'] = input_data.attrs['crs']
        for data_var in output_data.data_vars:
            output_data[data_var].attrs['crs'] = input_data.attrs['crs']
        return output_data


class Aggregate(VirtualProduct):
    """ A (non-spatial) statistic of grouped data. """

    @property
    def _statistic(self) -> Transformation:
        """ The `Transformation` object associated with an aggregate product. """
        cls = self['aggregate']

        try:
            obj = cls(**{key: value for key, value in self.items()
                         if key not in ['aggregate', 'input', 'group_by']})
        except TypeError:
            raise VirtualProductException("transformation {} could not be instantiated".format(cls))

        self._assert(isinstance(obj, Transformation), "not a transformation object: {}".format(obj))

        return cast(Transformation, obj)

    @property
    def _input(self) -> VirtualProduct:
        """ The input product of a transform product. """
        return from_validated_recipe(self['input'])

    def _reconstruct(self):
        # pylint: disable=protected-access
        return dict(aggregate=qualified_name(self['aggregate']),
                    group_by=qualified_name(self['group_by']),
                    input=self._input._reconstruct(),
                    **reject_keys(self, ['input', 'aggregate', 'group_by']))

    def output_measurements(self, product_definitions: Dict[str, DatasetType]) -> Dict[str, Measurement]:
        input_measurements = self._input.output_measurements(product_definitions)

        return self._statistic.measurements(input_measurements)

    def query(self, dc: Datacube, **search_terms: Dict[str, Any]) -> VirtualDatasetBag:
        return self._input.query(dc, **search_terms)

    def group(self, datasets: VirtualDatasetBag, **group_settings: Dict[str, Any]) -> VirtualDatasetBox:
        grouped = self._input.group(datasets, **group_settings)
        dim = self.get('dim', 'time')

        def to_box(value):
            return xarray.DataArray([VirtualDatasetBox(value, grouped.geobox,
                                                       grouped.load_natively, grouped.product_definitions,
                                                       geopolygon=grouped.geopolygon)],
                                    dims=['_fake_'])

        result = grouped.box.groupby(self['group_by'](grouped.box[dim]), squeeze=False).apply(to_box).squeeze('_fake_')
        result[dim].attrs.update(grouped.box[dim].attrs)

        return VirtualDatasetBox(result, grouped.geobox, grouped.load_natively, grouped.product_definitions,
                                 geopolygon=grouped.geopolygon)

    def fetch(self, grouped: VirtualDatasetBox, **load_settings: Dict[str, Any]) -> xarray.Dataset:
        dim = self.get('dim', 'time')

        def xr_map(array, func):
            # convenient function close to `xr_apply` in spirit
            coords = {key: value.values for key, value in array.coords.items()}
            for i in numpy.ndindex(array.shape):
                yield func({key: value[i] for key, value in coords.items()}, array.values[i])

        def statistic(coords, value):
            data = self._input.fetch(value, **load_settings)
            result = self._statistic.compute(data)
            result.coords[dim] = coords[dim]
            return result

        groups = list(xr_map(grouped.box, statistic))
        result = xarray.concat(groups, dim=dim).assign_attrs(**select_unique([g.attrs for g in groups]))
        result.coords[dim].attrs.update(grouped.box[dim].attrs)
        return result


class Collate(VirtualProduct):
    """ Stack observations from products with the same set of measurements. """

    @property
    def _children(self) -> List[VirtualProduct]:
        """ The children of a collate product. """
        return [from_validated_recipe(child) for child in self['collate']]

    def _reconstruct(self):
        # pylint: disable=protected-access
        children = [child._reconstruct() for child in self._children]
        return dict(collate=children, **reject_keys(self, ['collate']))

    def output_measurements(self, product_definitions: Dict[str, DatasetType]) -> Dict[str, Measurement]:
        input_measurement_list = [child.output_measurements(product_definitions)
                                  for child in self._children]

        first, *rest = input_measurement_list

        for child in rest:
            self._assert(set(child) == set(first),
                         "child datasets do not all have the same set of measurements")

        name = self.get('index_measurement_name')
        if name is None:
            return first

        self._assert(name not in first, "source index measurement '{}' already present".format(name))

        first.update({name: Measurement(name=name, dtype='int8', nodata=-1, units='1')})
        return first

    def query(self, dc: Datacube, **search_terms: Dict[str, Any]) -> VirtualDatasetBag:
        result = [child.query(dc, **search_terms) for child in self._children]

        return VirtualDatasetBag({'collate': [datasets.bag for datasets in result]},
                                 select_unique([datasets.geopolygon for datasets in result]),
                                 merge_dicts([datasets.product_definitions for datasets in result]))

    def group(self, datasets: VirtualDatasetBag, **group_settings: Dict[str, Any]) -> VirtualDatasetBox:
        self._assert('collate' in datasets.bag and len(datasets.bag['collate']) == len(self._children),
                     "invalid dataset bag")

        def build(source_index, product, dataset_bag):
            grouped = product.group(VirtualDatasetBag(dataset_bag,
                                                      datasets.geopolygon, datasets.product_definitions),
                                    **group_settings)

            def tag(_, value):
                return {'collate': (source_index, value)}

            return grouped.map(tag)

        groups = [build(source_index, product, dataset_bag)
                  for source_index, (product, dataset_bag)
                  in enumerate(zip(self._children, datasets.bag['collate']))]

        dim = self.get('dim', 'time')
        return VirtualDatasetBox(xarray.concat([grouped.box for grouped in groups], dim=dim).sortby(dim),
                                 select_unique([grouped.geobox for grouped in groups]),
                                 select_unique([grouped.load_natively for grouped in groups]),
                                 merge_dicts([grouped.product_definitions for grouped in groups]),
                                 geopolygon=select_unique([grouped.geopolygon for grouped in groups]))

    def fetch(self, grouped: VirtualDatasetBox, **load_settings: Dict[str, Any]) -> xarray.Dataset:
        def is_from(source_index):
            def result(_, value):
                self._assert('collate' in value, "malformed dataset box in collate")
                return value['collate'][0] == source_index

            return result

        def strip_source(_, value):
            return value['collate'][1]

        def fetch_child(child, source_index, r):
            if any([x == 0 for x in r.box.shape]):
                # empty raster
                return None
            else:
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

        groups = [fetch_child(child, source_index, grouped.filter(is_from(source_index)).map(strip_source))
                  for source_index, child in enumerate(self._children)]

        non_empty = [g for g in groups if g is not None]

        dim = self.get('dim', 'time')

        result = xarray.concat(non_empty,
                               dim=dim).sortby(dim).assign_attrs(**select_unique([g.attrs
                                                                                  for g in non_empty]))

        # concat and sortby mess up chunking
        if 'dask_chunks' not in load_settings or dim not in load_settings['dask_chunks']:
            return result
        return result.apply(lambda x: x.chunk({dim: load_settings['dask_chunks'][dim]}), keep_attrs=True)


class Juxtapose(VirtualProduct):
    """ Put measurements from different products side-by-side. """

    @property
    def _children(self) -> List[VirtualProduct]:
        """ The children of a juxtapose product. """
        return [from_validated_recipe(child) for child in self['juxtapose']]

    def _reconstruct(self):
        # pylint: disable=protected-access
        children = [child._reconstruct() for child in self._children]
        return dict(juxtapose=children, **reject_keys(self, ['juxtapose']))

    def output_measurements(self, product_definitions: Dict[str, DatasetType]) -> Dict[str, Measurement]:
        input_measurement_list = [child.output_measurements(product_definitions)
                                  for child in self._children]

        result = cast(Dict[str, Measurement], {})
        for measurements in input_measurement_list:
            common = set(result) & set(measurements)
            self._assert(not common, "common measurements {} between children".format(common))

            result.update(measurements)

        return result

    def query(self, dc: Datacube, **search_terms: Dict[str, Any]) -> VirtualDatasetBag:
        result = [child.query(dc, **search_terms)
                  for child in self._children]

        return VirtualDatasetBag({'juxtapose': [datasets.bag for datasets in result]},
                                 select_unique([datasets.geopolygon for datasets in result]),
                                 merge_dicts([datasets.product_definitions for datasets in result]))

    def group(self, datasets: VirtualDatasetBag, **group_settings: Dict[str, Any]) -> VirtualDatasetBox:
        self._assert('juxtapose' in datasets.bag and len(datasets.bag['juxtapose']) == len(self._children),
                     "invalid dataset bag")

        groups = [product.group(VirtualDatasetBag(dataset_bag, datasets.geopolygon, datasets.product_definitions),
                                **group_settings)
                  for product, dataset_bag in zip(self._children, datasets.bag['juxtapose'])]

        aligned_boxes = xarray.align(*[grouped.box for grouped in groups])

        def tuplify(indexes, _):
            return {'juxtapose': [box.sel(**indexes).item() for box in aligned_boxes]}

        return VirtualDatasetBox(xr_apply(aligned_boxes[0], tuplify),
                                 select_unique([grouped.geobox for grouped in groups]),
                                 select_unique([grouped.load_natively for grouped in groups]),
                                 merge_dicts([grouped.product_definitions for grouped in groups]),
                                 geopolygon=select_unique([grouped.geopolygon for grouped in groups]))

    def fetch(self, grouped: VirtualDatasetBox, **load_settings: Dict[str, Any]) -> xarray.Dataset:
        def select_child(source_index):
            def result(_, value):
                self._assert('juxtapose' in value, "malformed dataset box in juxtapose")
                return value['juxtapose'][source_index]

            return result

        def fetch_recipe(source_index):
            child_groups = grouped.map(select_child(source_index))
            return VirtualDatasetBox(child_groups.box, grouped.geobox,
                                     grouped.load_natively, grouped.product_definitions,
                                     geopolygon=grouped.geopolygon)

        groups = [child.fetch(fetch_recipe(source_index), **load_settings)
                  for source_index, child in enumerate(self._children)]

        return xarray.merge(groups).assign_attrs(**select_unique([g.attrs for g in groups]))


class Reproject(VirtualProduct):
    """
    On-the-fly reprojection of raster data.
    """

    @property
    def _input(self) -> "VirtualProduct":
        """ The input product of a transform product. """
        return from_validated_recipe(self["input"])

    def _reconstruct(self):
        # pylint: disable=protected-access
        return dict(input=self._input._reconstruct(), **reject_keys(self, ["input"]))

    def output_measurements(self, product_definitions: Dict[str, DatasetType]) -> Dict[str, Measurement]:
        """
        A dictionary mapping names to measurement metadata.
        :param product_definitions: a dictionary mapping product names to products (`DatasetType` objects)
        """
        return self._input.output_measurements(product_definitions)

    def query(self, dc: Datacube, **search_terms: Dict[str, Any]) -> VirtualDatasetBag:
        """ Collection of datasets that match the query. """
        return self._input.query(dc, **reject_keys(search_terms, self._GEOBOX_KEYS))

    def group(self, datasets: VirtualDatasetBag, **group_settings: Dict[str, Any]) -> VirtualDatasetBox:
        """
        Datasets grouped by their timestamps.
        :param datasets: the `VirtualDatasetBag` to fetch data from
        """
        geopolygon = datasets.geopolygon

        merged = merge_search_terms(self, group_settings)
        if geopolygon is None:
            selected = list(datasets.contained_datasets())
        else:
            selected = None

        geobox = output_geobox(datasets=selected,
                               output_crs=self['reproject']['output_crs'],
                               resolution=self['reproject']['resolution'],
                               align=self['reproject'].get('align'),
                               geopolygon=geopolygon)

        # load natively
        input_box = self._input.group(datasets, **reject_keys(merged, self._GEOBOX_KEYS))

        return VirtualDatasetBox(input_box.box,
                                 geobox,
                                 True,
                                 datasets.product_definitions,
                                 geopolygon=geopolygon)

    def fetch(self, grouped: VirtualDatasetBox, **load_settings: Dict[str, Any]) -> xarray.Dataset:
        """ Convert grouped datasets to `xarray.Dataset`. """
        if no_reproject:
            raise VirtualProductException("Reproject not available without package odc-algo")
        geobox = grouped.geobox

        measurements = self.output_measurements(grouped.product_definitions)
        # it looks an overkill to resample differently among bands
        # keep as it was there
        band_settings = dict(zip(list(measurements),
                                 per_band_load_data_settings(measurements,
                                                             resampling=self.get('resampling', 'nearest'))))
        resamplings = set([s.get('resampling_method') for s in band_settings.values()])
        resampling_group = dict()
        for resampling in resamplings:
            resampling_group[resampling] = [key for key, value in band_settings.items() if value.get('resampling_method') == resampling]

        boxes = [VirtualDatasetBox(box_slice.box, None, True, box_slice.product_definitions, geopolygon=geobox.extent)
                 for box_slice in grouped.split()]

        dask_chunks = load_settings.get('dask_chunks')
        if dask_chunks is None:
            rasters = [self._input.fetch(box, **load_settings) for box in boxes]
        else:
            rasters = [self._input.fetch(box, dask_chunks={key: 1 for key in dask_chunks if key not in geobox.dims},
                                         **reject_keys(load_settings, ['dask_chunks']))
                       for box in boxes]
        dask_chunks = [dask_chunks.get(k) for k in ['y', 'x']]
        if None in dask_chunks:
            dask_chunks = None
        result = xarray.Dataset()
        for resampling, bands in resampling_group.items():
            result = result.merge(xarray.concat([xr_reproject(raster[bands], geobox, resampling, dask_chunks)
                                 for raster in rasters], dim='time'))
        result.attrs['crs'] = geobox.crs
        return result


def virtual_product_kind(recipe):
    """ One of product, transform, collate, juxtapose, aggregate, or reproject. """
    candidates = [key for key in list(recipe)
                  if key in ['product', 'transform', 'collate', 'juxtapose', 'aggregate', 'reproject']]
    if len(candidates) > 1:
        raise VirtualProductException("ambiguous kind in recipe: {}".format(recipe))
    if len(candidates) < 1:
        raise VirtualProductException("virtual product kind not specified in recipe: {}".format(recipe))
    return candidates[0]


def from_validated_recipe(recipe):
    lookup = dict(product=Product, transform=Transform, collate=Collate,
                  juxtapose=Juxtapose, aggregate=Aggregate, reproject=Reproject)
    return lookup[virtual_product_kind(recipe)](recipe)
