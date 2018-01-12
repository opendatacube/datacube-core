# Warning: this is a WIP

from abc import ABC, abstractmethod

import datacube
from datacube.model import Measurement

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
        """ Iterator of datasets that match the query. """
        raise NotImplementedError

    def fetch_data(self, datasets, **query):
        """
        Data (represented as an `xarray.Dataset` from datasets.
        :param datasets: the datasets to fetch data from
        :param query: to specify a geobox
        """
        raise NotImplementedError

    def load(self, index, **query):
        """ Mimic `datacube.Datacube.load`. """
        return self.fetch_data(self.find_datasets(index, **query), **query)


def product_measurements(index, product_name):
    """ The measurement metadata for an existing product. """
    return index.products.get_by_name(product_name).measurements


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
        return datacube.Datacube().find_datasets(product=self.product_name, **query)

    def fetch_data(self, datasets, **query):
        return datacube.Datacube().load(product=self.product_name, measurements=self.measurement_names,
                                        datasets=list(datasets), **query)


class Drop(VirtualProduct):
    def __init__(self, child, predicate):
        self.child = child
        self.predicate = predicate

    def output_measurements(self, index):
        return self.child.output_measurements(index)

    def validate_construction(self, index):
        self.child.validate_construction(index)

    def find_datasets(self, index, **query):
        for ds in self.child.find_datasets(index, **query):
            if self.predicate(ds):
                yield ds

    def fetch_data(self, datasets, **query):
        return self.child.fetch_data(datasets, **query)


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

    def fetch_data(self, datasets, **query):
        return self.transform(self.child.fetch_data(datasets, **query))
