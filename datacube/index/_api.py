# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import logging

from sqlalchemy import create_engine

from ._db import Db

_LOG = logging.getLogger(__name__)


def _connection_string(host=None, database=None):
    """
    >>> _connection_string(database='agdc')
    'postgresql:///agdc'
    >>> _connection_string(host='postgres.dev.lan', database='agdc')
    'postgresql://postgres.dev.lan/agdc'
    """
    return 'postgresql://{host}/{database}'.format(
        host=host or '',
        database=database or ''
    )


def connect(config):
    """
    Connect to the index.
    :type config: datacube.config.UserConfig
    :rtype: AccessIndex
    """
    connection_string = _connection_string(config.db_hostname, config.db_database)
    _LOG.debug('Connecting: %r', connection_string)
    engine = create_engine(
        connection_string,
        echo=True
    )

    return AccessIndex(Db(engine))


def _index_dataset(db, dataset_doc, path=None):
    """

    :type db: Db
    :type dataset_doc: dict
    :type path: pathlib.Path
    :return:
    """
    # TODO: These lookups will depend on the document type.
    dataset_id = dataset_doc['id']
    source_datsets = dataset_doc['lineage']['source_datasets']
    product_type = dataset_doc['product_type']

    # Clear them. We store them separately.
    dataset_doc['lineage']['source_datasets'] = None

    # Get source datasets & index them.
    sources = {}
    for classifier, source_dataset in source_datsets.items():
        source_id = _index_dataset(db, source_dataset)
        sources[classifier] = source_id

    # TODO: If throws error, dataset may exist already.
    db.insert_dataset(dataset_doc, dataset_id, path, product_type)

    # Link to sources.
    for classifier, source_dataset_id in sources.items():
        db.insert_dataset_source(classifier, dataset_id, source_dataset_id)

    return dataset_id


class AccessIndex(object):
    def __init__(self, db):
        self.db = db

    def add_dataset(self, dataset):
        """
        :type dataset: datacube.model.Dataset
        """
        _index_dataset(self.db, dataset.metadata_doc, path=dataset.metadata_path)

    # Dummy implementation: TODO.
    def contains_dataset(self, dataset):
        """
        Have we already indexed this dataset?

        :type dataset: datacube.model.Dataset
        :rtype: bool
        """
        # We haven't indexed anything.
        return False
