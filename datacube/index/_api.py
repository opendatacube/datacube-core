# coding=utf-8
"""
Access methods for indexing datasets & storage units.
"""
from __future__ import absolute_import

import logging

from datacube.config import LocalConfig
from ._datasets import DatasetResource
from ._storage import StorageUnitResource, StorageMappingResource, StorageTypeResource
from .postgres import PostgresDb

_LOG = logging.getLogger(__name__)


def connect(local_config=LocalConfig.find()):
    """
    Connect to the index. Default Postgres implementation.
    :type local_config: datacube.config.LocalConfig
    :rtype: Index
    """
    return Index(
        PostgresDb.from_config(local_config),
        local_config
    )


class Index(object):
    def __init__(self, db, local_config):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        """
        self._db = db

        self.datasets = DatasetResource(db)
        self.storage_types = StorageTypeResource(db)
        self.mappings = StorageMappingResource(db, self.storage_types, local_config)
        self.storage = StorageUnitResource(db, self.mappings)
