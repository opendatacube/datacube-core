# NOTE: Currently this is a WIP mock. It probably does not even run.

from abc import ABC, abstractmethod

import datacube
from datacube.model import Measurement

class VirtualProductConstructionException(Exception):
    pass


class VirtualProduct(ABC):
    def output_measurements(self, index):
        pass

    def validate_construction(self, index):
        pass

    def load(self, index, **search_terms):
        """ TEST FUNCTION. """
        pass

    def run_query(self, index, **search_terms):
        pass

    def fetch_data(self, datasets):
        """ Current datasets must be a list, not a generator """
        pass

    def load(self, index, **search_terms):
        datasets = list(self.run_query(index, **search_terms))
        return self.fetch_data(datasets)


class Drop(VirtualProduct):
    def __init__(self, child, predicate):
        self.child = child
        self.predicate = predicate

    def output_measurements(self, index):
        return self.child.output_measurements(index)

    def validate_construction(self, index):
        self.child.validate_construction(index)

    def run_query(self, index, **search_terms):
        for ds in self.child.run_query(index, **search_terms):
            if self.predicate(ds):
                yield ds

    def fetch_data(self, datasets):
        return self.child.fetch_data(datasets)


def product_measurements(index, product_name):
    return index.products.get_by_name(product_name).measurements

class ExistingProduct(VirtualProduct):
    def __init__(self, product_name, measurement_names=None):
        self.product_name = product_name

        if measurement_names is not None:
            if len(measurement_names) == 0:
                raise VirtualProductConstructionException()

        self.measurement_names = measurement_names

    def output_measurements(self, index):
        measurements = product_measurements(index, self.product_name)
        return [measurements[m] for m in measurements if m in self.measurement_names]

    def validate_construction(self, index):
        measurements = product_measurements(index, self.product_name)
        measurement_names = list(measurements.keys())

        if self.measurement_names is None:
            self.measurement_names = measurement_names
        else:
            for m in self.measurement_names:
                if m not in measurement_names:
                    raise VirtualProductConstructionException()

    def run_query(self, index, **search_terms):
        return datacube.Datacube().find_datasets(product=self.product_name, **search_terms)

    def fetch_data(self, datasets):
        return datacube.Datacube().load(product=self.product_name,
                                        measurements=self.measurement_names,
datasets=datasets)