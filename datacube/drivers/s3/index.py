from __future__ import absolute_import

import logging

from datacube.config import LocalConfig
import datacube.index._api
from datacube.index.postgres import PostgresDb

# pylint: disable=protected-access
class Index(datacube.index._api.Index):

    def __init__(self, local_config=None, application_name=None, validate_connection=True):
        if local_config is None:
            local_config = LocalConfig.find()
        super(Index, self).__init__(PostgresDb.from_config(local_config,
                                                           application_name=application_name,
                                                           validate_connection=validate_connection))
        self.logger = logging.getLogger(self.__class__.__name__)
        self.datasets = DatasetResource(self._db, self.products)


    def add_datasets(self, new_datasets, sources_policy='verify'):
        n = 0
        for dataset in new_datasets.values:
            key_maps = new_datasets['storage_output'] if 'storage_output' in new_datasets else None
            self.datasets.add_with_key_maps(dataset,
                                            key_maps=key_maps,
                                            sources_policy='skip')
            n += 1
        return n


# pylint: disable=protected-access
class DatasetResource(datacube.index._datasets.DatasetResource):

    def __init__(self, db, dataset_type_resource):
        super(DatasetResource, self).__init__(db, dataset_type_resource)
        self.logger = logging.getLogger(self.__class__.__name__)

    def add_with_key_maps(self, dataset, key_maps=None, skip_sources=False, sources_policy='verify'):
        super(DatasetResource, self).add(dataset, skip_sources=skip_sources, sources_policy=sources_policy)
        ## Low level stuff: call in our own Postgres low level module
