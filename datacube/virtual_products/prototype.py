# Warning: this is a WIP

from abc import ABC, abstractmethod
from itertools import combinations

import xarray

from datacube import Datacube
from datacube.model import Measurement
from datacube.api.query import Query, query_group_by, query_geopolygon

from datacube.virtual_products.utils import select_datasets_inside_polygon, output_geobox, xr_map


class VirtualProductException(Exception):
    """ Raised if the construction of the virtual product cannot be validated. """
    pass


class VirtualProduct(ABC):
    """ Abstract class defining the common interface of virtual products. """

    @abstractmethod
    def output_measurements(self, index):
        # type: (Index) -> Dict[str, Measurement]
        """ A dictionary mapping names to measurement metadata. """

    @abstractmethod
    def find_datasets(self, index, **query):
        # type: (Index, Dict[str, Any]) -> VirtualDatasetPile
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

    def load(self, index, **query):
        # type: (Index, Dict[str, Any]) -> xarray.Dataset
        """ Mimic `datacube.Datacube.load`. """
        datasets = self.find_datasets(index, **query)
        raster = self.build_raster(datasets, **query)
        data = self.fetch_data(raster)
        return data


class VirtualDatasetPile(ABC):
    """ Result of `VirtualProduct.find_datasets`. """


class RasterRecipe(object):
    """ Result of `VirtualProduct.build_raster`. """
    # our replacement for grid_workflow.Tile basically
    # TODO: copy the Tile API
    def __init__(self, grouped_dataset_pile, geobox, output_measurements):
        self.grouped_dataset_pile = grouped_dataset_pile
        self.geobox = geobox
        self.output_measurements = output_measurements


class BasicDatasets(VirtualDatasetPile):
    def __init__(self, dataset_list, grid_spec, output_measurements):
        self.dataset_list = list(dataset_list)
        self.grid_spec = grid_spec
        self.output_measurements = output_measurements


class BasicTimeslice(object):
    def __init__(self, datasets):
        self.datasets = datasets


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

    def output_measurements(self, index):
        """ Output measurements metadata. """
        measurement_docs = index.products.get_by_name(self.product_name).measurements
        measurements = {key: Measurement(value)
                        for key, value in measurement_docs.items()}

        if self.measurement_names is None:
            return measurements

        try:
            return {name: measurements[name] for name in self.measurement_names}
        except KeyError as ke:
            raise VirtualProductException("Could not find measurement: {}".format(ke.args))

    def find_datasets(self, index, **query):
        # this is basically a copy of `datacube.Datacube.find_datasets_lazy`
        # ideally that method would look like this too in the future

        # `like` is implicitly supported here, not sure if we should
        # `platform` and `product_type` based queries are possibly ruled out
        # other possible query entries include `geopolygon` and contents of `SPATIAL_KEYS` and `CRS_KEYS`
        # query should not include contents of `OTHER_KEYS` except `geopolygon`

        # find the datasets
        query = Query(index, product=self.product_name, measurements=self.measurement_names,
                      source_filter=self.source_filter, **query)
        assert query.product == self.product_name

        datasets = select_datasets_inside_polygon(index.datasets.search(**query.search_terms),
                                                  query.geopolygon)

        if self.dataset_filter is not None:
            datasets = [dataset for dataset in datasets if self.dataset_filter(dataset)]

        # gather information from the index before it disappears from sight
        output_measurements = self.output_measurements(index)
        grid_spec = index.products.get_by_name(self.product_name).grid_spec

        return BasicDatasets(datasets, grid_spec, output_measurements)

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, BasicDatasets)
        # we will support group_by='solar_day' elsewhere
        assert 'group_by' not in query

        # possible query entries are contents of `SPATIAL_KEYS`, `CRS_KEYS`, and `OTHER_KEYS`
        # query should not include `product`, `measurements`, and `resampling`

        # select only those inside the ROI
        # ROI could be smaller than the query for `find_datasets`
        polygon = query_geopolygon(**query)
        selected = list(select_datasets_inside_polygon(datasets.dataset_list, polygon))

        # group by time
        grouped = Datacube.group_datasets(selected, query_group_by(group_by='time'))

        def wrap_timeslice(indexes, value):
            return BasicTimeslice(value)

        grouped = xr_map(grouped, wrap_timeslice)

        # geobox
        geobox = output_geobox(datasets.dataset_list, datasets.grid_spec, **query)

        # information needed for Datacube.load_data
        return RasterRecipe(grouped, geobox, datasets.output_measurements)

    def fetch_data(self, raster):
        assert isinstance(raster, RasterRecipe)

        # this method is basically `GridWorkflow.load`

        # convert Measurements back to dicts?
        # essentially what `datacube.api.core.set_resampling_method` does
        measurements = [dict(**measurement.__dict__)
                        for measurement in raster.output_measurements.values()]

        if self.resampling_method is not None:
            measurements = [dict(resampling_method=self.resampling_method, **measurement)
                            for measurement in measurements]

        def unwrap_timeslice(indexes, value):
            assert isinstance(value, BasicTimeslice)
            return value.datasets

        grouped = xr_map(raster.grouped_dataset_pile, unwrap_timeslice)
        return Datacube.load_data(grouped, raster.geobox,
                                  measurements, fuse_func=self.fuse_func)


def basic_product(product_name, measurement_names=None,
                  source_filter=None, fuse_func=None, resampling_method=None):
    return BasicProduct(product_name, measurement_names=measurement_names,
                        source_filter=source_filter, fuse_func=fuse_func,
                        resampling_method=resampling_method)


class TransformationFunction(ABC):
    """ Describes a transformation of data from one virtual product to another.  """

    @abstractmethod
    def compute(self, data):
        """
        Computation to perform.
        :param data: input `xarray.Dataset`
        """

    @abstractmethod
    def output_measurements(self, input_measurements):
        """
        Describe the output measurements.
        :param input_measurements: measurement information of the input
        """


class Transform(VirtualProduct):
    def __init__(self, child, transformation):
        """
        :param transform: a `TransformationFunction`
        """
        self.child = child
        self.transformation = transformation

    def output_measurements(self, index):
        return self.transformation.output_measurements(self.child.output_measurements(index))

    def find_datasets(self, index, **query):
        return self.child.find_datasets(index, **query)

    def build_raster(self, datasets, **query):
        return self.child.build_raster(datasets, **query)

    def fetch_data(self, raster):
        input_data = self.child.fetch_data(raster)
        return self.transformation.compute(input_data)


def transform(child, transformation_function):
    return Transform(child, transformation_function)


class CollatedDatasets(VirtualDatasetPile):
    def __init__(self, dataset_tuple, grid_spec, output_measurements):
        self.dataset_tuple = tuple(dataset_tuple)
        self.grid_spec = grid_spec
        self.output_measurements = output_measurements


class CollatedTimeslice(object):
    def __init__(self, source_index, datasets):
        self.source_index = source_index
        self.datasets = datasets


class Collate(VirtualProduct):
    def __init__(self, *children, **kwargs):
        if len(children) == 0:
            raise VirtualProductException("No children for collate node")

        self.children = children
        self.index_measurement_name = kwargs.get('index_measurement_name')

        if self.index_measurement_name is not None:
            self.index_measurement = {
                index_measurement_name: Measurement({
                    'name': index_measurement_name,
                    'dtype': 'int8',
                    'nodata': -1,
                    'units': '1'
                })
            }

    def output_measurements(self, index):
        input_measurements = [child.output_measurements(index)
                              for child in self.children]

        first = input_measurements[0]
        rest = input_measurements[1:]

        for child in rest:
            if set(child) != set(first):
                msg = "Child datasets do not all have the same set of measurements"
                raise VirtualProductException(msg)

        if self.index_measurement_name is None:
            return first

        if self.index_measurement_name in first:
            msg = "Source index measurement '{}' already present".format(self.index_measurement_name)
            raise VirtualProductException(msg)

        first.update(self.index_measurement)
        return first

    def find_datasets(self, index, **query):
        result = [child.find_datasets(index, **query)
                  for child in self.children]

        # should possibly check all the `grid_spec`s are the same
        # requires a `GridSpec.__eq__` method implementation
        return CollatedDatasets(result, result[0].grid_spec, self.output_measurements(index))

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, CollatedDatasets)
        assert len(datasets.dataset_tuple) == len(self.children)

        def build(source_index, pair):
            product, datasets = pair
            raster = product.build_raster(datasets, **query)

            def tag(indexes, value):
                return CollatedTimeslice(source_index, value)

            return RasterRecipe(xr_map(raster.grouped_dataset_pile, tag),
                                raster.geobox, raster.output_measurements)

        rasters = [build(*args)
                   for args in enumerate(zip(self.children, datasets.dataset_tuple))]

        # should possibly check all the geoboxes are the same
        first = rasters[0]

        concatenated = xarray.concat([raster.grouped_dataset_pile for raster in rasters], dim='time')
        return RasterRecipe(concatenated,
                            first.geobox, first.output_measurements)

    def fetch_data(self, raster):
        assert isinstance(raster, RasterRecipe)
        grouped_dataset_pile = raster.grouped_dataset_pile
        geobox = raster.geobox
        output_measurements = raster.output_measurements

        def mask_for_source(source_index):
            def result(indexes, value):
                assert isinstance(value, CollatedTimeslice)
                return source_index == value.source_index

            return result

        def strip_source(indexes, value):
            assert isinstance(value, CollatedTimeslice)
            return value.datasets

        def fetch_by_source(source_index, child):
            mask = xr_map(grouped_dataset_pile, mask_for_source(source_index), 'bool')
            relevant = xr_map(grouped_dataset_pile[mask], strip_source)
            # TODO: insert index measurement
            return child.fetch_data(RasterRecipe(relevant, geobox, output_measurements))

        rasters = [fetch_by_source(source_index, child)
                   for source_index, child in enumerate(self.children)]

        return xarray.concat(rasters, dim='time')


def collate(*children, index_measurement_name=None):
    return Collate(*children, index_measurement_name=index_measurement_name)


class JuxtaposedDatasets(VirtualDatasetPile):
    def __init__(self, dataset_tuple, grid_spec, output_measurements):
        self.dataset_tuple = tuple(dataset_tuple)
        self.grid_spec = grid_spec
        self.output_measurements = output_measurements


class JuxtaposedTimeslice(object):
    def __init__(self, datasets):
        self.datasets = datasets


class Juxtapose(VirtualProduct):
    def __init__(self, *children):
        if len(children) == 0:
            raise VirtualProductException("No children for juxtapose node")

        self.children = children

    def output_measurements(self, index):
        input_measurements = [child.output_measurements(index)
                              for child in self.children]

        result = {}
        for measurements in input_measurements:
            common = set(result) & set(measurements)
            if common != set():
                msg = "Common measurements {} between children".format(common)
                raise VirtualProductException(msg)

            result.update(measurements)

        return result

    def find_datasets(self, index, **query):
        result = [child.find_datasets(index, **query)
                  for child in self.children]

        # should possibly check all the `grid_spec`s are the same
        # requires a `GridSpec.__eq__` method implementation
        return JuxtaposedDatasets(result, result[0].grid_spec, self.output_measurements(index))

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, JuxtaposedDatasets)
        assert len(datasets.dataset_tuple) == len(self.children)

        rasters = [product.build_raster(datasets, **query)
                   for product, datasets in zip(self.children, datasets.dataset_tuple)]

        # should possibly check all the geoboxes are the same
        geobox = rasters[0].geobox

        aligned = xarray.align(*[raster.grouped_dataset_pile for raster in rasters])
        output_measurements = [raster.output_measurements for raster in rasters]

        def tuplify(indexes, value):
            return JuxtaposedTimeslice([raster.sel(**indexes).item() for raster in aligned])

        merged = xr_map(aligned[0], tuplify)
        return RasterRecipe(merged, geobox, output_measurements)

    def fetch_data(self, raster):
        assert isinstance(raster, RasterRecipe)
        grouped_dataset_pile = raster.grouped_dataset_pile
        geobox = raster.geobox
        output_measurements = raster.output_measurements

        assert isinstance(output_measurements, list)

        def select_child(source_index):
            def result(indexes, value):
                assert isinstance(value, JuxtaposedTimeslice)
                return value.datasets[source_index]

            return result

        def fetch_child(source_index, child):
            datasets = xr_map(grouped_dataset_pile, select_child(source_index))
            recipe = RasterRecipe(datasets, geobox, output_measurements[source_index])
            return child.fetch_data(recipe)

        rasters = [fetch_child(source_index, child)
                   for source_index, child in enumerate(self.children)]

        return xarray.merge(rasters)


def juxtapose(*children):
    return Juxtapose(*children)
