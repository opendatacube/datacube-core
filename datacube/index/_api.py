# coding=utf-8
"""
Access methods for indexing datasets & storage units.
"""
from __future__ import absolute_import

import logging
from pathlib import Path

from datacube import ui
from datacube.config import LocalConfig
from ._datasets import DatasetResource, CollectionResource, MetadataTypeResource
from ._storage import StorageUnitResource, StorageTypeResource
from .postgres import PostgresDb

_LOG = logging.getLogger(__name__)

_DEFAULT_METADATA_TYPES_PATH = Path(__file__).parent.joinpath('default-metadata-types.yaml')
_DEFAULT_COLLECTIONS_PATH = Path(__file__).parent.joinpath('default-collections.yaml')


def connect(local_config=LocalConfig.find()):
    """
    Connect to the index. Default Postgres implementation.

    :param local_config: Config object to use.
    :type local_config: :py:class:`datacube.config.LocalConfig`, optional
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

        self.metadata_types = MetadataTypeResource(db)
        self.collections = CollectionResource(db, self.metadata_types)
        self.datasets = DatasetResource(db, local_config, self.collections)
        self.storage = StorageUnitResource(db, StorageTypeResource(db, local_config), self.collections, local_config)

    def init_db(self, with_default_collection=True):
        is_new = self._db.init()

        if is_new and with_default_collection:
            for _, doc in ui.read_documents(_DEFAULT_METADATA_TYPES_PATH):
                self.metadata_types.add(doc)
            for _, doc in ui.read_documents(_DEFAULT_COLLECTIONS_PATH):
                self.collections.add(doc)

        return is_new

    def __repr__(self):
        return "Index<db={!r}>".format(self._db)
