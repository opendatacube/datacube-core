# coding=utf-8
"""
Access methods for indexing datasets & storage units.
"""
from __future__ import absolute_import

import logging
from pathlib import Path

import yaml

from datacube.config import LocalConfig
from ._datasets import DatasetResource, CollectionResource
from ._storage import StorageUnitResource, StorageMappingResource, StorageTypeResource
from .postgres import PostgresDb

_LOG = logging.getLogger(__name__)

_DEFAULT_COLLECTIONS_FILE = Path(__file__).parent.joinpath('default-collections.yaml')


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

        self.collections = CollectionResource(db, local_config)
        self.datasets = DatasetResource(db, local_config, self.collections)
        self.storage_types = StorageTypeResource(db)
        self.mappings = StorageMappingResource(db, self.storage_types, local_config)
        self.storage = StorageUnitResource(db, self.mappings, self.collections, local_config)

    def init_db(self, with_default_collection=True):
        is_new = self._db.init()

        if is_new and with_default_collection:
            self._add_default_collection()

    def _add_default_collection(self):
        collection_descriptors = yaml.load(_DEFAULT_COLLECTIONS_FILE.open('r'))
        self.collections.add(collection_descriptors)
        # Names of added collections
        return list(collection_descriptors.keys())
