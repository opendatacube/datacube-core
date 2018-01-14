# Warning: this is a WIP

from abc import ABC, abstractmethod

import xarray

from datacube import Datacube
from datacube.model import Measurement
from datacube.api.query import query_group_by, query_geopolygon
from datacube.utils import geometry


class VirtualProductConstructionException(Exception):
    pass


class VirtualProduct(ABC):
    def output_measurements(self, index):
        """ A dictionary mapping names to measurement metadata. """
        raise NotImplementedError

    def validate_construction(self, index):
        """
        Validate that the virtual product is well-formed.
        :raises: VirtualProductConstructionException if not well-formed.
        """
        raise NotImplementedError

    def find_datasets(self, index, **query):
        """ Collection of datasets that match the query. """
        # next step: group data by time here
        raise NotImplementedError

    def build_raster(self, datasets, **query):
        """
        Data (represented as an `xarray.Dataset`) from datasets.
        :param datasets: the datasets to fetch data from
        :param query: to specify a geobox
        """
        raise NotImplementedError

    def load(self, index, **query):
        """ Mimic `datacube.Datacube.load`. """
        return self.build_raster(self.find_datasets(index, **query), **query)


def product_measurements(index, product_name):
    """ The measurement metadata for an existing product. """
    measurement_docs = index.products.get_by_name(product_name).measurements
    return {key: Measurement(value)
            for key, value in measurement_docs.items()}


class ExistingDatasets(object):
    def __init__(self, dataset_list, geobox, output_measurements):
        # so that it can be serialized
        self.dataset_list = dataset_list
        self.geobox = geobox
        self.output_measurements = output_measurements


class ExistingProduct(VirtualProduct):
    """ A product already in the datacube. """
    def __init__(self, product_name, measurement_names=None):
        """
        :param product_name: name of the product
        :param  measurement_names: list of names of measurements to include
        """
        self.product_name = product_name

        if measurement_names is not None:
            if len(measurement_names) == 0:
                raise VirtualProductConstructionException()

        self.measurement_names = measurement_names

    def output_measurements(self, index):
        """
        Output measurements metadata.
        """
        measurements = product_measurements(index, self.product_name)

        return {name: measurement
                for name, measurement in measurements.items()
                if name in self.measurement_names}

    def validate_construction(self, index):
        measurements = product_measurements(index, self.product_name)
        measurement_names = list(measurements.keys())

        if self.measurement_names is None:
            self.measurement_names = measurement_names
        else:
            for m in self.measurement_names:
                if m not in measurement_names:
                    raise VirtualProductConstructionException()

    def find_datasets(self, index, **query):
        # find the datasets
        dc = Datacube(index=index)
        datasets = dc.find_datasets(product=self.product_name, **query)

        # group by time
        group_by = query_group_by(**query)
        grouped = Datacube.group_datasets(datasets, group_by)

        # geobox
        grid_spec = index.products.get_by_name(self.product_name).grid_spec
        assert grid_spec is not None and grid_spec.crs is not None
        geobox = geometry.GeoBox.from_geopolygon(query_geopolygon(**query),
                                                 grid_spec.resolution,
                                                 grid_spec.crs,
                                                 grid_spec.alignment)

        # information needed for Datacube.load_data
        return ExistingDatasets(grouped, geobox, self.output_measurements(index))

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, ExistingDatasets)

        # convert Measurements back to dicts?
        measurements = [dict(name=m.name, dtype=m.dtype, nodata=m.nodata, units=m.units)
                        for m in datasets.output_measurements.values()]

        return Datacube.load_data(datasets.dataset_list, datasets.geobox,
                                  measurements)


class Drop(VirtualProduct):
    def __init__(self, child, predicate):
        self.child = child
        self.predicate = predicate

    def output_measurements(self, index):
        return self.child.output_measurements(index)

    def validate_construction(self, index):
        self.child.validate_construction(index)

    def find_datasets(self, index, **query):
        # this won't work for datasets with more complicated structure
        def result():
            for ds in self.child.find_datasets(index, **query):
                if self.predicate(ds):
                    yield ds

        return ExistingDatasets(result())

    def build_raster(self, datasets, **query):
        return self.child.build_raster(datasets, **query)


class Transform(VirtualProduct):
    def __init__(self, child, transform, transform_measurement):
        """
        :param transform: a function that transforms the loaded data
        :param transform_measurement: a function that returns output measurement metadata
                                      when provided with input measurement metadata
        """
        self.child = child
        self.transform = transform
        self.transform_measurement = transform_measurement

    def output_measurements(self, index):
        return self.transform_measurement(self.child.output_measurements)

    def validate_construction(self, index):
        return self.child.validate_construction(index)

    def find_datasets(self, index, **query):
        return self.child.find_datasets(index, **query)

    def build_raster(self, datasets, **query):
        return self.transform(self.child.build_raster(datasets, **query))


class CollatedDatasets(object):
    def __init__(self, *datasets):
        self.dataset_tuple = tuple(datasets)


class Collate(VirtualProduct):
    def __init__(self, *children):
        self.children = children

    def validate_construction(self, index):
        for child in self.children:
            child.validate_construction(index)

        input_measurements = [child.output_measurements(index)
                              for child in self.children]

        first = input_measurements[0]
        rest = input_measurements[1:]

        for child in rest:
            if set(child) != set(first):
                raise VirtualProductConstructionException()

    def output_measurements(self, index):
        return self.children[0].output_measurements()

    def find_datasets(self, index, **query):
        result = [child.find_datasets(index, **query)
                  for child in self.children]

        return CollatedDatasets(*result)

    def build_raster(self, datasets, **query):
        assert isinstance(datasets, CollatedDatasets)
        assert len(datasets.dataset_tuple) == len(self.children)

        paired_up = zip(self.children, datasets.dataset_tuple)

        rasters = [product.build_raster(datasets, **query)
                   for product, datasets in paired_up]

        # in reality this should be time sorted
        return xarray.concat(rasters, dim='time')
