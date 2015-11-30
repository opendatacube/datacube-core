# coding=utf-8
"""
Lower-level database access.
"""
from __future__ import absolute_import

import datetime
import json
import logging

import numpy
from sqlalchemy import create_engine, select, text, bindparam, exists, and_, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.url import URL as EngineUrl
from sqlalchemy.exc import IntegrityError

from datacube.config import LocalConfig
from . import tables
from ._fields import parse_fields, NativeField
from .tables import DATASET, DATASET_SOURCE, STORAGE_TYPE, \
    STORAGE_MAPPING, STORAGE_UNIT, DATASET_STORAGE, COLLECTION

PGCODE_UNIQUE_CONSTRAINT = '23505'

_LOG = logging.getLogger(__name__)


class PostgresDb(object):
    """
    A very thin database access api.

    It exists so that higher level modules are not tied to SQLAlchemy, connections or specifics of database-access.

    (and can be unit tested without any actual databases)
    """

    def __init__(self, engine, connection):
        self._engine = engine
        self._connection = connection

    @classmethod
    def connect(cls, hostname, database, username=None, port=None):
        _engine = create_engine(
            EngineUrl('postgresql', host=hostname, database=database, username=username, port=port),
            echo=False,
            # 'AUTOCOMMIT' here means READ-COMMITTED isolation level with autocommit on.
            # When a transaction is needed we will do an explicit begin/commit.
            isolation_level='AUTOCOMMIT',

            json_serializer=_to_json,
            # json_deserializer=my_deserialize_fn
        )
        _connection = _engine.connect()
        return PostgresDb(_engine, _connection)

    @classmethod
    def from_config(cls, config=LocalConfig.find()):
        return PostgresDb.connect(
            config.db_hostname,
            config.db_database,
            config.db_username,
            config.db_port
        )

    def init(self):
        """
        Init a new database (if not already set up).

        :return: If it was newly created.
        """
        return tables.ensure_db(self._connection, self._engine)

    def begin(self):
        """
        Start a transaction.

        Returns a transaction object. Call commit() or rollback() to complete the
        transaction or use a context manager:

            with db.begin() as transaction:
                db.insert_dataset(...)

        :return: Tranasction object
        """
        return _BegunTransaction(self._connection)

    def insert_dataset(self, metadata_doc, dataset_id, path, metadata_type, collection_id=None):
        """
        Insert dataset if not already indexed.
        :type metadata_doc: dict
        :type dataset_id: str or uuid.UUID
        :type path: pathlib.Path
        :type metadata_type: str
        :return: whether it was inserted
        :rtype: bool
        """
        if collection_id is None:
            collection_result = self.get_collection_for_doc(metadata_doc)
            if not collection_result:
                raise RuntimeError('No collection matches dataset')
            collection_id = collection_result['id']

        try:
            ret = self._connection.execute(
                # Insert if not exists.
                #     (there's still a tiny chance of a race condition: It will throw an integrity error if another
                #      connection inserts the same dataset in the time between the subquery and the main query.
                #      This is ok for our purposes.)
                DATASET.insert().from_select(
                    ['id', 'collection_ref', 'metadata_path', 'metadata'],
                    select([
                        bindparam('id'), bindparam('collection_ref'), bindparam('metadata_path'),
                        bindparam('metadata', type_=JSONB)
                    ]).where(~exists(select([DATASET.c.id]).where(DATASET.c.id == bindparam('id'))))
                ),
                id=dataset_id,
                collection_ref=collection_id,
                # TODO: Does a single path make sense? Or a separate 'locations' table?
                metadata_path=str(path) if path else None,
                metadata=metadata_doc
            )
            return ret.rowcount > 0
        except IntegrityError as e:
            if e.orig.pgcode == PGCODE_UNIQUE_CONSTRAINT:
                _LOG.info('Duplicate dataset, not inserting: %s @ %s', dataset_id, path)
                # We're still going to raise it, because the transaction will have been invalidated.
            raise

    def contains_dataset(self, dataset_id):
        return bool(self._connection.execute(select([DATASET.c.id]).where(DATASET.c.id == dataset_id)).fetchone())

    def insert_dataset_source(self, classifier, dataset_id, source_dataset_id):
        self._connection.execute(
            DATASET_SOURCE.insert(),
            classifier=classifier,
            dataset_ref=dataset_id,
            source_dataset_ref=source_dataset_id
        )

    def ensure_storage_type(self, driver, name, descriptor):
        # TODO: Update them if they already exist. This will do for now.
        self._connection.execute(
            STORAGE_TYPE.insert(),
            driver=driver,
            name=name,
            descriptor=descriptor
        )

    def get_storage_type(self, storage_type_id):
        return self._connection.execute(
            STORAGE_TYPE.select().where(STORAGE_TYPE.c.id == storage_type_id)
        ).first()

    def get_storage_mapping(self, storage_mapping_id):
        return self._connection.execute(
            STORAGE_MAPPING.select().where(STORAGE_MAPPING.c.id == storage_mapping_id)
        ).first()

    def get_storage_mappings(self, dataset_metadata):
        """
        Find any storage mappings that match the given dataset.

        :type dataset_metadata: dict
        :rtype: dict
        """
        # Find any storage mappings whose 'dataset_metadata' document is a subset of the metadata.
        return self._connection.execute(
            STORAGE_MAPPING.select().where(
                STORAGE_MAPPING.c.dataset_metadata.contained_by(dataset_metadata)
            )
        ).fetchall()

    def ensure_storage_mapping(self, storage_type_name, name, location_name, file_path_template,
                               dataset_metadata, data_measurements_key, measurements):
        self._connection.execute(
            STORAGE_MAPPING.insert().values(
                storage_type_ref=select([STORAGE_TYPE.c.id]).where(
                    STORAGE_TYPE.c.name == storage_type_name
                ),
                name=name,
                dataset_metadata=dataset_metadata,
                dataset_measurements_key=data_measurements_key,
                measurements=measurements,
                location_name=location_name,
                file_path_template=file_path_template,
            )
        )

    def add_storage_unit(self, path, dataset_ids, descriptor, storage_mapping_id):
        unit_id = self._connection.execute(
            STORAGE_UNIT.insert().returning(STORAGE_UNIT.c.id),
            storage_mapping_ref=storage_mapping_id,
            descriptor=descriptor,
            path=path
        ).scalar()

        self._connection.execute(
            DATASET_STORAGE.insert(),
            [
                {'dataset_ref': dataset_id, 'storage_unit_ref': unit_id}
                for dataset_id in dataset_ids
                ]
        )
        return unit_id

    def get_storage_units(self):
        return self._connection.execute(STORAGE_UNIT.select()).fetchall()

    _NATIVE_DATASET_FIELDS = {
        'id': NativeField('id', DATASET.c.id),
        'metadata_path': NativeField('metadata_path', DATASET.c.metadata_path)
    }

    _NATIVE_STORAGE_FIELDS = {
        'id': NativeField('id', STORAGE_UNIT.c.id),
        'path': NativeField('path', STORAGE_UNIT.c.path)
    }

    def get_dataset_fields(self, collection_result):
        fields = self._NATIVE_DATASET_FIELDS.copy()
        fields.update(parse_fields(
            collection_result['dataset_search_fields'],
            DATASET.c.metadata
        ))
        return fields

    def get_storage_unit_fields(self, collection_result):
        fields = self._NATIVE_STORAGE_FIELDS.copy()
        fields.update(parse_fields(
            collection_result['storage_unit_search_fields'],
            DATASET.c.metadata
        ))
        return fields

    def search_datasets(self, expressions, select_fields=None):
        """
        :type select_fields: tuple[datacube.index.postgres._fields.PgField]
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: dict
        """
        return self._search_docs(
            expressions,
            select_fields=select_fields,
            select_table=DATASET
        )

    def search_storage_units(self, expressions, select_fields=None):
        """
        :type select_fields: tuple[datacube.index.postgres._fields.PgField]
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: dict
        """
        from_expression = STORAGE_UNIT

        # Join to datasets if we're querying by a dataset field.
        referenced_tables = set([expression.field.alchemy_column.table for expression in expressions])
        _LOG.debug('Searching fields from tables: %s', ', '.join([t.name for t in referenced_tables]))
        if DATASET in referenced_tables:
            from_expression = from_expression.join(DATASET_STORAGE).join(DATASET)

        return self._search_docs(
            expressions,
            select_fields=select_fields,
            select_table=STORAGE_UNIT,
            from_expression=from_expression
        )

    def _search_docs(self, expressions, select_fields=None, select_table=None, from_expression=None):
        select_fields = [f.alchemy_expression for f in select_fields] if select_fields else [select_table]

        if from_expression is None:
            from_expression = select_table

        results = self._connection.execute(
            select(select_fields).select_from(from_expression).where(
                and_(*[expression.alchemy_expression for expression in expressions])
            )
        )
        for result in results:
            yield result

    def get_collection_for_doc(self, metadata_doc):
        """
        :type metadata_doc: dict
        :rtype: dict or None
        """
        return self._connection.execute(
            COLLECTION.select().where(
                COLLECTION.c.dataset_metadata.contained_by(metadata_doc)
            ).order_by(
                COLLECTION.c.match_priority.asc()
            ).limit(1)
        ).first()

    def get_collection(self, id_):
        return self._connection.execute(
            COLLECTION.select().where(COLLECTION.c.id == id_)
        ).first()

    def add_collection(self, name, description,
                       dataset_metadata, match_priority,
                       dataset_id_offset, dataset_label_offset,
                       dataset_creation_dt_offset, dataset_measurements_offset,
                       dataset_search_fields,
                       storage_unit_search_fields):
        res = self._connection.execute(
            COLLECTION.insert().values(
                name=name,
                description=description,
                dataset_metadata=dataset_metadata,
                match_priority=match_priority,
                dataset_id_offset=dataset_id_offset,
                dataset_label_offset=dataset_label_offset,
                dataset_creation_dt_offset=dataset_creation_dt_offset,
                dataset_measurements_offset=dataset_measurements_offset,
                dataset_search_fields=dataset_search_fields,
                storage_unit_search_fields=storage_unit_search_fields
            )
        )

        collection_id = res.inserted_primary_key[0]
        collection_result = self.get_collection(collection_id)

        # Initialise search fields.
        _setup_collection_fields(
            self._engine, name, 'dataset', self.get_dataset_fields(collection_result),
            DATASET.c.collection_ref == collection_id
        )
        _setup_collection_fields(
            self._engine, name, 'storage_unit', self.get_storage_unit_fields(collection_result),
            STORAGE_UNIT.c.collection_ref == collection_id
        )


def _setup_collection_fields(engine, collection_prefix, doc_prefix, fields, where_expression):
    prefix = '{}_{}'.format(collection_prefix.lower(), doc_prefix.lower())

    # Create indexes for the search fields.
    for field in fields.values():
        index_type = field.postgres_index_type
        if index_type:
            _LOG.debug('Creating index: %s', field.name)
            Index(
                'ix_field_{prefix}_{name}'.format(
                    prefix=prefix.lower(),
                    name=field.name.lower(),
                ),
                field.alchemy_expression,
                postgres_where=where_expression,
                postgresql_using=index_type,
                # Don't lock the table (in the future we'll allow indexing new fields...)
                postgresql_concurrently=True
            ).create(engine)

    # Create a view of search fields (for debugging convenience).
    engine.execute(
        tables.View(
            prefix,
            select(
                [field.alchemy_expression.label(field.name) for field in fields.values()]
            ).where(where_expression)
        )
    )


def _to_json(o):
    return json.dumps(o, default=_json_fallback)


def _json_fallback(obj):
    """Fallback json serialiser."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, numpy.dtype):
        return obj.name
    raise TypeError("Type not serializable: {}".format(type(obj)))


class _BegunTransaction(object):
    def __init__(self, connection):
        self._connection = connection
        self.begin()

    def begin(self):
        self._connection.execute(text('BEGIN'))

    def commit(self):
        self._connection.execute(text('COMMIT'))

    def rollback(self):
        self._connection.execute(text('ROLLBACK'))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
