'''NetCDF indexing module.
'''
from __future__ import absolute_import

import logging

import datacube.drivers.index as base_index


class Index(base_index.Index, base_index.IndexExtension):
    '''NetCDF driver wrapper.'''

    def __init__(self, driver_manager, index=None, *args, **kargs):
        '''Initialise the index.'''
        super(Index, self).__init__(driver_manager, index, *args, **kargs)


    def add_specifics(self, dataset):
        '''Extend the dataset doc with driver specific index data.

        There is no specific info to add for NetCDF as is constitutes
        the base index.

        :param :cls:`datacube.model.Dataset` dataset: The dataset to
          add NetCDF-specific indexing data to.
        '''
        pass


    def add_datasets(self, datasets, sources_policy='verify'):
        '''Index several datasets using the current driver.

        :param datasets: The datasets to be indexed.
        :param str sources_policy: The sources policy.
        :return: The number of datasets indexed.
        :rtype: int
        '''
        n = 0
        for dataset in datasets.values:
            self.datasets.add(dataset, sources_policy='skip')
            n += 1
        return n
