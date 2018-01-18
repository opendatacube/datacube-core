# Warning: this is a WIP

from abc import ABC, abstractmethod
from itertools import combinations

import xarray

from datacube import Datacube
from datacube.model import Measurement
from datacube.api.query import Query, query_group_by, query_geopolygon
from datacube.api.core import get_bounds
from datacube.utils import geometry, intersects


class VirtualProductException(Exception):
    """ Raised if the construction of the virtual product cannot be validated. """
    pass


class VirtualDatasets(ABC):
    """ Result of `VirtualProduct.find_datasets`. """

    @abstractmethod
    def drop(self, predicate):
        """ Drop datasets that do not satisfy `predicate`. """


class VirtualRaster(ABC):
    """ Result of `VirtualProduct.build_raster`. """
    # our replacement for grid_workflow.Tile basically
    # TODO: copy the Tile API


class VirtualProduct(ABC):
    """ Abstract class defining the common interface of virtual products. """

    @abstractmethod
    def output_measurements(self, index):
        # type: (Index) -> Dict[str, Measurement]
        """ A dictionary mapping names to measurement metadata. """

    @abstractmethod
    def validate_construction(self, index):
        # type: (Index) -> None
        """
        Validate that the virtual product is well-formed.
        :raises: `VirtualProductException` if not well-formed.
        """

    @abstractmethod
    def find_datasets(self, index, **query):
        # type: (Index, Dict[str, Any]) -> VirtualDatasets
        """ Collection of datasets that match the query. """

    # no database access below this line

    @abstractmethod
    def build_raster(self, datasets, **query):
        # type: (VirtualDatasets, Dict[str, Any]) -> VirtualRaster
        """
        Datasets grouped by their timestamps.
        :param datasets: the datasets to fetch data from
        :param query: to specify a spatial sub-region
        """

    @abstractmethod
    def fetch_data(self, raster):
        # type: (VirtualRaster) -> xarray.Dataset
        """ Convert virtual raster to `xarray.Dataset`. """

    def load(self, index, **query):
        # type: (Index, Dict[str, Any]) -> xarray.Dataset
        """ Mimic `datacube.Datacube.load`. """
        datasets = self.find_datasets(index, **query)
        raster = self.build_raster(datasets, **query)
        data = self.fetch_data(raster)
        return data


class BasicDatasets(VirtualDatasets):
    def __init__(self, dataset_list, grid_spec, output_measurements):
        self.dataset_list = list(dataset_list)
        self.grid_spec = grid_spec
        self.output_measurements = output_measurements

    def drop(self, predicate):
        # type: Callable[[datacube.Dataset], bool] -> BasicDatasets
        return BasicDatasets([dataset
                              for dataset in self.dataset_list
                              if predicate is None or predicate(dataset)],
                             self.grid_spec, self.output_measurements)


class BasicRaster(VirtualRaster):
    def __init__(self, grouped_datasets, geobox, output_measurements):
        self.grouped_datasets = grouped_datasets
        self.geobox = geobox
        self.output_measurements = output_measurements


def select_datasets_inside_polygon(datasets, polygon):
    # essentially copied from Datacube.find_datasets
    # TODO: place it somewhere so that Datacube.find_datasets can access and use it
    for dataset in datasets:
        if polygon is None:
            yield dataset
        else:
            if intersects(polygon.to_crs(dataset.crs), dataset.extent):
                yield dataset


def output_geobox(datasets, grid_spec,
                  like=None, output_crs=None, resolution=None, align=None,
                  **query):
    # this is a copy of the logic in `datacube.Datacube.load`
    # TODO: hopefully that method can make use of this function in the future

    if like is not None:
        assert output_crs is None, "'like' and 'output_crs' are not supported together"
        assert resolution is None, "'like' and 'resolution' are not supported together"
        assert align is None, "'like' and 'align' are not supported together"
        return like.geobox

    if output_crs is not None:
        # user provided specifications
        if resolution is None:
            raise ValueError("Must specify 'resolution' when specifying 'output_crs'")
        crs = geometry.CRS(output_crs)
    else:
        # specification from grid_spec
        if grid_spec is None or grid_spec.crs is None:
            raise ValueError("Product has no default CRS. Must specify 'output_crs' and 'resolution'")
        crs = grid_spec.crs
        if resolution is None:
            if grid_spec.resolution is None:
                raise ValueError("Product has no default resolution. Must specify 'resolution'")
            resolution = grid_spec.resolution
            align = align or grid_spec.alignment  # is the indentation wrong here?

    return geometry.GeoBox.from_geopolygon(query_geopolygon(**query) or get_bounds(datasets, crs),
                                           resolution, crs, align)


class BasicProduct(VirtualProduct):
    """ A product already in the datacube. """
    def __init__(self, product_name, measurement_names=None,
                 fuse_func=None, resampling_method=None):
        """
        :param product_name: name of the product
        :param  measurement_names: list of names of measurements to include (None if all)
        """
        self.product_name = product_name
        self.measurement_names = measurement_names

        # is this a good place for it?
        self.fuse_func = fuse_func
        self.resampling_method = resampling_method

    def output_measurements(self, index):
        """ Output measurements metadata.  """
        measurement_docs = index.products.get_by_name(self.product_name).measurements
        measurements = {key: Measurement(value)
                        for key, value in measurement_docs.items()}

        if self.measurement_names is None:
            return measurements

        return {name: measurements[name] for name in self.measurement_names}

    def validate_construction(self, index):
        if self.measurement_names is not None:
            if not self.measurement_names:
                raise VirtualProductException("Product selects no measurements")

        try:
            _ = self.output_measurements(index)
        except KeyError as ke:
            raise VirtualProductException("Could not find measurement: {}".format(ke.args))

    def find_datasets(self, index, **query):
        # this is basically a copy of `datacube.Datacube.find_datasets_lazy`
        # ideally that method would look like this too in the future

        # `like` is implicitly supported here, not sure if we should
        # `platform` and `product_type` based queries are possibly ruled out
        # other possible query entries include `geopolygon` and contents of `SPATIAL_KEYS` and `CRS_KEYS`
        # query should not include contents of `OTHER_KEYS` except `geopolygon`
        # uncertain about `source_filter`, do not know what it does

        # find the datasets
        query = Query(index, product=self.product_name, measurements=self.measurement_names, **query)
        assert query.product == self.product_name

        datasets = select_datasets_inside_polygon(index.datasets.search(**query.search_terms),
                                                  query.geopolygon)

        # gather information from the index before it disappears from sight
        output_measurements = self.output_measurements(index)
        grid_spec = index.products.get_by_name(self.product_name).grid_spec

        return BasicDatasets(datasets, grid_spec, output_measurements)

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, BasicDatasets)

        # possible query entries are contents of `SPATIAL_KEYS`, `CRS_KEYS`, and `OTHER_KEYS`
        # query should not include `product`, `measurements`, and `resampling`

        # select only those inside the ROI and group by time
        polygon = query_geopolygon(**query)
        group_by = query_group_by(**query)
        selected = list(select_datasets_inside_polygon(datasets.dataset_list, polygon))

        grouped = Datacube.group_datasets(selected, group_by)
        grouped = grouped.isel(time=grouped.time.argsort())

        # geobox
        # grid_spec = datasets.grid_spec
        # assert grid_spec is not None and grid_spec.crs is not None
        geobox = output_geobox(datasets.dataset_list, datasets.grid_spec, **query)

        # information needed for Datacube.load_data
        return BasicRaster(grouped, geobox, datasets.output_measurements)

    def fetch_data(self, raster):
        assert isinstance(raster, BasicRaster)

        # this method is basically `GridWorkflow.load`

        # convert Measurements back to dicts?
        # essentially what `datacube.api.core.set_resampling_method` does
        measurements = [dict(**measurement.__dict__)
                        for measurement in raster.output_measurements.values()]

        if self.resampling_method is not None:
            measurements = [dict(resampling_method=self.resampling_method, **measurement)
                            for measurement in measurements]

        result = Datacube.load_data(raster.grouped_datasets, raster.geobox,
                                    measurements, fuse_func=self.fuse_func)
        return result.isel(time=result.time.argsort())


class Drop(VirtualProduct):
    def __init__(self, child, predicate):
        self.child = child
        self.predicate = predicate

    def output_measurements(self, index):
        return self.child.output_measurements(index)

    def validate_construction(self, index):
        self.child.validate_construction(index)

    def find_datasets(self, index, **query):
        return self.child.find_datasets(index, **query).drop(self.predicate)

    def build_raster(self, datasets, **query):
        return self.child.build_raster(datasets, **query)

    def fetch_data(self, raster):
        return self.child.fetch_data(raster)


class Transform(VirtualProduct):
    def __init__(self, child, transform, transform_measurement):
        """
        :param transform: a function that transforms the loaded data
        :param transform_measurement: a function that returns output measurement metadata
                                      when provided with input measurement metadata
        """
        self.child = child

        # maybe transform should be a class with methods to compute and
        # to report output measurements
        self.transform = transform
        self.transform_measurement = transform_measurement

    def output_measurements(self, index):
        return self.transform_measurement(self.child.output_measurements)

    def validate_construction(self, index):
        self.child.validate_construction(index)

    def find_datasets(self, index, **query):
        return self.child.find_datasets(index, **query)

    def build_raster(self, datasets, **query):
        return self.child.build_raster(datasets, **query)

    def fetch_data(self, raster):
        # is this the right level?
        # or should the transform be applied to every time slice individually?
        return self.transform(self.child.fetch_data(raster))


class CollatedDatasets(VirtualDatasets):
    def __init__(self, dataset_tuple, grid_spec, output_measurements):
        self.dataset_tuple = tuple(dataset_tuple)
        self.grid_spec = grid_spec
        self.output_measurements = output_measurements

    def drop(self, predicate):
        assert isinstance(predicate, tuple)
        assert len(self.dataset_tuple) == len(predicate)

        result = [datasets.drop(pred)
                  for pred, datasets in zip(predicate, self.dataset_tuple)]
        return CollatedDatasets(result, self.grid_spec, self.output_measurements)


class CollatedRaster(VirtualRaster):
    def __init__(self, raster_tuple, geobox, output_measurements):
        self.raster_tuple = raster_tuple
        self.geobox = geobox
        self.output_measurements = output_measurements


class Collate(VirtualProduct):
    def __init__(self, *children, index_measurement_name=None):
        self.children = children
        self.index_measurement_name = index_measurement_name

        if self.index_measurement_name is not None:
            self.index_measurement = {
                index_measurement_name: Measurement({
                    'name': index_measurement_name,
                    'dtype': 'int8',
                    'nodata': -1,
                    'units': 1
                })
            }

    def validate_construction(self, index):
        if len(self.children) == 0:
            raise VirtualProductException("No children for collate node")

        for child in self.children:
            child.validate_construction(index)

        input_measurements = [child.output_measurements(index)
                              for child in self.children]

        first = input_measurements[0]
        rest = input_measurements[1:]

        for child in rest:
            if set(child) != set(first):
                msg = "Child datasets do not all have the same set of measurements"
                raise VirtualProductException(msg)

        if self.index_measurement_name is not None:
            if self.index_measurement_name in first:
                msg = "Source index measurement '{}' already present".format(self.index_measurement_name)
                raise VirtualProductException(msg)

    def output_measurements(self, index):
        measurements = self.children[0].output_measurements(index)

        if self.index_measurement_name is None:
            return measurements

        return {**measurements, **self.index_measurement}

    def find_datasets(self, index, **query):
        result = [child.find_datasets(index, **query)
                  for child in self.children]

        # should possibly check all the `grid_spec`s are the same
        # requires a `GridSpec.__eq__` method implementation
        return CollatedDatasets(result, result[0].grid_spec, self.output_measurements(index))

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, CollatedDatasets)
        assert len(datasets.dataset_tuple) == len(self.children)

        rasters = [product.build_raster(datasets, **query)
                   for product, datasets in zip(self.children, datasets.dataset_tuple)]

        first = rasters[0]

        # should possibly check all the geoboxes are the same
        return CollatedRaster(rasters, first.geobox, first.output_measurements)

    def fetch_data(self, raster):
        assert isinstance(raster, CollatedRaster)

        arrays = [product.fetch_data(raster)
                  for product, raster in zip(self.children, raster.raster_tuple)]

        # TODO: insert index measurement
        result = xarray.concat(arrays, dim='time')
        return result.isel(time=result.time.argsort())


class JuxtaposedDatasets(VirtualDatasets):
    def __init__(self, dataset_tuple, grid_spec, output_measurements):
        self.dataset_tuple = tuple(dataset_tuple)
        self.grid_spec = grid_spec
        self.output_measurements = output_measurements

    def drop(self, predicate):
        assert isinstance(predicate, tuple)
        assert len(self.dataset_tuple) == len(predicate)

        result = [datasets.drop(pred)
                  for pred, datasets in zip(predicate, self.dataset_tuple)]
        return JuxtaposedDatasets(result, self.grid_spec, self.output_measurements)


class JuxtaposedRaster(VirtualRaster):
    def __init__(self, raster_tuple, geobox, output_measurements):
        self.raster_tuple = raster_tuple
        self.geobox = geobox
        self.output_measurements = output_measurements


class Juxtapose(VirtualProduct):
    def __init__(self, *children):
        self.children = children

    def validate_construction(self, index):
        if len(self.children) == 0:
            raise VirtualProductException("No children for juxtapose node")

        for child in self.children:
            child.validate_construction(index)

        input_measurement_names = [list(child.output_measurements(index).keys())
                                   for child in self.children]

        for first, second in combinations(input_measurement_names, 2):
            common = set(first) & set(second)
            if common != set():
                msg = "Common measurements {} between {} and {}".format(common, first, second)
                raise VirtualProductException(msg)

    def output_measurements(self, index):
        input_measurements = [child.output_measurements(index)
                              for child in self.children]

        result = {}
        for measurements in input_measurements:
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

        first = rasters[0]

        # TODO xarray.align
        # NOTE cannot do, raster[0] is not an xarray because of collate
        # NOTE if we inject the tree structure into the xarray then who loads it?

        # should possibly check all the geoboxes are the same
        return JuxtaposedRaster(rasters, first.geobox, datasets.output_measurements)

    def fetch_data(self, raster):
        assert isinstance(raster, JuxtaposedRaster)

        arrays = [product.fetch_data(raster)
                  for product, raster in zip(self.children, raster.raster_tuple)]

        result = xarray.merge(arrays)
        return result.isel(time=result.time.argsort())
