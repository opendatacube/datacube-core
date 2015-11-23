# coding=utf-8
"""
Access methods for indexing datasets & storage units.
"""
from __future__ import absolute_import

import copy
import logging

from datacube.config import SystemConfig
from datacube.model import Range
from .postgres import PostgresDb


_LOG = logging.getLogger(__name__)


def connect(config=SystemConfig.find()):
    """
    Connect to the index. Default Postgres implementation.
    :type config: datacube.config.SystemConfig
    :rtype: DataIndex
    """
    return DataIndex(
        PostgresDb.from_config(config)
    )


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


def _build_expression(get_field, name, value):
    field = get_field(name)
    if field is None:
        raise RuntimeError('Unknown field %r' % name)

    if isinstance(value, Range):
        return field.between(value.begin, value.end)
    else:
        return field == value


def _build_expressions(get_field, **query):
    return [_build_expression(get_field, name, value) for name, value in query.items()]


class DataIndex(object):
    def __init__(self, db):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        """
        self._db = db

    def ensure_dataset(self, dataset):
        """
        Ensure a dataset is in the index. Add it if not present.
        :type dataset: datacube.model.Dataset
        :return: dataset id if newly indexed.
        :rtype: uuid.UUID or None
        """
        with self._db.begin() as transaction:
            return _ensure_dataset(self._db, dataset.metadata_doc, path=dataset.metadata_path)

    def contains_dataset(self, dataset):
        """
        Have we already indexed this dataset?

        :type dataset: datacube.model.Dataset
        :rtype: bool
        """
        return self._db.contains_dataset(dataset.id)

    def add_storage_units(self, storage_units):
        """
        :type storage_units: list[datacube.model.StorageUnit]
        """
        for unit in storage_units:
            with self._db.begin() as transaction:
                unit_id = self._db.add_storage_unit(
                    unit.path,
                    unit.dataset_ids,
                    unit.descriptor,
                    unit.storage_mapping.id_,
                )
                _LOG.debug('Indexed unit %s @ %s', unit_id, unit.path)

    def add_storage_unit(self, storage_unit):
        """
        :type storage_unit: datacube.model.StorageUnit
        """
        return self.add_storage_units([storage_unit])

    def get_dataset_field(self, name):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        return self._db.get_dataset_field('eo', name)

    def get_storage_field(self, name):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        return self._db.get_storage_field('eo', name)

    def get_storage_field_with_fallback(self, name):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        val = self.get_storage_field(name)
        return val if val is not None else self.get_dataset_field(name)

    def search_datasets(self, *expressions, **query):
        """
        TODO: Return objects
        :type expressions: list[datacube.index.fields.Expression]
        """
        query_exprs = tuple(_build_expressions(self.get_dataset_field, **query))
        return self._db.search_datasets((expressions + query_exprs))

    def search_datasets_eager(self, *expressions, **query):
        """
        :type expressions: list[datacube.index.fields.Expression]
        """
        return list(self.search_datasets(*expressions, **query))

    def search_storage_units(self, *expressions, **query):
        """
        TODO: Return objects
        :type expressions: list[datacube.index.fields.Expression]
        """
        query_exprs = tuple(_build_expressions(self.get_storage_field_with_fallback, **query))
        return self._db.search_storage_units((expressions + query_exprs))

    def search_storage_units_eager(self, *expressions, **query):
        """
        :type expressions: list[datacube.index.fields.Expression]
        """
        return list(self.search_storage_units(*expressions, **query))
