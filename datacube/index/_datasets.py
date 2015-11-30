# coding=utf-8
"""
API for dataset indexing, access and search.
"""
from __future__ import absolute_import

import copy
import logging

from datacube.model import Dataset, Collection, DatasetMatcher
from .fields import to_expressions

_LOG = logging.getLogger(__name__)


def _ensure_dataset(db, dataset_doc, path=None):
    """
    Ensure a dataset is in the index (add it if needed).

    :type db: PostgresDb
    :type dataset_doc: dict
    :type path: pathlib.Path
    :returns: The dataset_id if we ingested it.
    :rtype: uuid.UUID or None
    """

    # TODO: These lookups will depend on the document type.
    dataset_id = dataset_doc['id']
    source_datasets = dataset_doc['lineage']['source_datasets']

    indexable_doc = copy.deepcopy(dataset_doc)
    # Clear source datasets: We store them separately.
    indexable_doc['lineage']['source_datasets'] = None
    # For now everything is 'eo'
    metadata_type = 'eo'

    _LOG.info('Indexing %s @ %s', dataset_id, path)
    was_inserted = db.insert_dataset(indexable_doc, dataset_id, path, metadata_type)

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


class CollectionResource(object):
    def __init__(self, db):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        """
        self._db = db

    def get_for_dataset_doc(self, metadata_doc):
        """
        :type metadata_doc: dict
        :rtype list[datacube.model.Collection]
        """
        return self._make(self._db.get_collection_for_doc(metadata_doc))

    def _make(self, query_result):
        """
        :rtype list[datacube.model.Collection]
        """
        return (
            Collection(
                collection['name'],
                collection['description'],
                DatasetMatcher(collection['dataset_metadata']),
                dataset_id_offset=collection['dataset_id_offset'],
                dataset_label_offset=collection['dataset_label_offset'],
                dataset_creation_time_offset=collection['dataset_creation_dt_offset'],
                dataset_measurements_offset=collection['dataset_measurements_offset'],
                dataset_search_fields=self._db.get_dataset_fields(collection),
                storage_unit_search_fields=self._db.get_storage_unit_fields(collection),
            ) for collection in query_result
        )


class DatasetResource(object):
    def __init__(self, db):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        """
        self._db = db

    def get(self, id_):
        raise RuntimeError('TODO: implement')

    def has(self, dataset):
        """
        Have we already indexed this dataset?

        :type dataset: datacube.model.Dataset
        :rtype: bool
        """
        return self._db.contains_dataset(dataset.id)

    def add(self, dataset):
        """
        Ensure a dataset is in the index. Add it if not present.
        :type dataset: datacube.model.Dataset
        :return: dataset id if newly indexed.
        :rtype: uuid.UUID or None
        """
        with self._db.begin() as transaction:
            return _ensure_dataset(self._db, dataset.metadata_doc, path=dataset.metadata_path)

    def get_field(self, name):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        return self._db.get_dataset_field('eo', name)

    def _make(self, query_result):
        """
        :rtype list[datacube.model.Dataset]
        """
        return (Dataset(dataset.metadata_type, dataset.metadata, dataset.metadata_path)
                for dataset in query_result)

    def search(self, *expressions, **query):
        """
        :type query: dict[str,str|float|datacube.model.Range]
        :type expressions: tuple[datacube.index.fields.PgExpression]
        :rtype list[datacube.model.Dataset]
        """
        query_exprs = tuple(to_expressions(self.get_field, **query))
        return self._make(self._db.search_datasets((expressions + query_exprs)))

    def search_eager(self, *expressions, **query):
        """
        :type expressions: list[datacube.index.fields.Expression]
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype list[datacube.model.Dataset]
        """
        return list(self.search(*expressions, **query))
