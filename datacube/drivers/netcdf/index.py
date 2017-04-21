'''NetCDF indexing module.
'''
from __future__ import absolute_import

import logging

from datacube.config import LocalConfig
import datacube.index._api
from datacube.index.postgres import PostgresDb

# pylint: disable=protected-access
class Index(datacube.index._api.Index):
    '''NetCDF driver wrapper.'''

    def __init__(self, local_config=None, application_name=None, validate_connection=True):
        '''Initialise the index.'''
        if local_config is None:
            local_config = LocalConfig.find()
        super(Index, self).__init__(PostgresDb.from_config(local_config,
                                                           application_name=application_name,
                                                           validate_connection=validate_connection))
        self.logger = logging.getLogger(self.__class__.__name__)


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
