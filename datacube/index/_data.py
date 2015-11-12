# coding=utf-8
"""
High-level API code for accessing the Index.
"""
from __future__ import absolute_import

import copy
import logging

from datacube.config import SystemConfig
from ._core_db import Db

_LOG = logging.getLogger(__name__)


def connect(config=SystemConfig.find()):
    """
    Connect to the index.
    :type config: datacube.config.SystemConfig
    :rtype: DataIndex
    """
    return DataIndex(Db.connect(config.db_hostname, config.db_database))


def _ensure_dataset(db, dataset_doc, path=None):
    """
    Ensure a dataset is in the index (add it if needed).

    :type db: Db
    :type dataset_doc: dict
    :type path: pathlib.Path
    :returns: The dataset_id if we ingested it.
    :rtype: uuid.UUID or None
    """

    # TODO: These lookups will depend on the document type.
    dataset_id = dataset_doc['id']
    source_datasets = dataset_doc['lineage']['source_datasets']
    product_type = dataset_doc['product_type']

    indexable_doc = copy.deepcopy(dataset_doc)
    # Clear source datasets: We store them separately.
    indexable_doc['lineage']['source_datasets'] = None

    _LOG.info('Indexing %s @ %s', dataset_id, path)
    was_inserted = db.insert_dataset(indexable_doc, dataset_id, path, product_type)

    if not was_inserted:
        # No need to index sources: the dataset already existed.
        return None

    if source_datasets:
        # Get source datasets & index them.
        sources = {}
        for classifier, source_dataset in source_datasets.items():
            source_id = _ensure_dataset(db, source_dataset)
            if source_id is None:
                # Was already indexed.
                continue
            sources[classifier] = source_id

        # Link to sources.
        for classifier, source_dataset_id in sources.items():
            db.insert_dataset_source(classifier, dataset_id, source_dataset_id)

    return dataset_id


class DataIndex(object):
    def __init__(self, db):
        """
        :type db: datacube.index._core_db.Db
        """
        self.db = db

    def ensure_dataset(self, dataset):
        """
        Ensure a dataset is in the index. Add it if not present.
        :type dataset: datacube.model.Dataset
        :return: dataset id if newly indexed.
        :rtype: uuid.UUID or None
        """
        with self.db.begin() as transaction:
            return _ensure_dataset(self.db, dataset.metadata_doc, path=dataset.metadata_path)

    def contains_dataset(self, dataset):
        """
        Have we already indexed this dataset?

        :type dataset: datacube.model.Dataset
        :rtype: bool
        """
        return self.db.contains_dataset(dataset.id)
