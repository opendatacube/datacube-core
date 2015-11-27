# coding=utf-8
"""
Lower-level database access.
"""
from __future__ import absolute_import

import datetime
import json
import logging
from collections import defaultdict

import numpy
from sqlalchemy import create_engine, select, text, bindparam, exists, and_
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.url import URL as EngineUrl
from sqlalchemy.exc import IntegrityError

from datacube.config import LocalConfig
from . import tables
from ._fields import FieldCollection, DEFAULT_FIELDS_FILE
from .tables import DATASET, DATASET_SOURCE, STORAGE_TYPE, \
    STORAGE_MAPPING, STORAGE_UNIT, DATASET_STORAGE

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

        # These are currently hardcoded and so will not change. We may store them in the DB eventually.
        self._fields = FieldCollection()
        self._fields.load_from_file(DEFAULT_FIELDS_FILE)

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
        is_new = tables.ensure_db(self._connection, self._engine)

        # Index fields within documents.
        # TODO: Support rerunning this on existing databases (ie. check if each index exists first).
        if is_new:

            views = defaultdict(list)
            for metadata_type, doc_type, field in self._fields.items():
                _LOG.debug('Creating index: %s', field.name)
                index = field.as_alchemy_index(prefix=metadata_type + '_' + doc_type)
                if index is not None:
                    index.create(self._engine)

                views['{}_{}'.format(metadata_type, doc_type)].append(field)

            # Create a view of all our search fields (for debugging convenience).
            for view_name, fields in views.items():
                self._engine.execute(
                    tables.View(
                        view_name,
                        select(
                            [field.alchemy_expression.label(field.name) for field in fields]
                        )
                    )
                )
        return is_new

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

    def insert_dataset(self, metadata_doc, dataset_id, path, metadata_type):
        """
        Insert dataset if not already indexed.
        :type metadata_doc: dict
        :type dataset_id: str or uuid.UUID
        :type path: pathlib.Path
        :type metadata_type: str
        :return: whether it was inserted
        :rtype: bool
        """
        try:
            ret = self._connection.execute(
                # Insert if not exists.
                #     (there's still a tiny chance of a race condition: It will throw an integrity error if another
                #      connection inserts the same dataset in the time between the subquery and the main query.
                #      This is ok for our purposes.)
                DATASET.insert().from_select(
                    ['id', 'metadata_type', 'metadata_path', 'metadata'],
                    select([
                        bindparam('id'), bindparam('metadata_type'), bindparam('metadata_path'),
                        bindparam('metadata', type_=JSONB)
                    ]).where(~exists(select([DATASET.c.id]).where(DATASET.c.id == bindparam('id'))))
                ),
                id=dataset_id,
                metadata_type=metadata_type,
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
        ).fetchone()

    def get_storage_mapping(self, storage_mapping_id):
        return self._connection.execute(
            STORAGE_MAPPING.select().where(STORAGE_MAPPING.c.id == storage_mapping_id)
        ).fetchone()

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

    def get_dataset_field(self, metadata_type, name):
        return self._fields.get(metadata_type, 'dataset', name)

    def get_storage_field(self, metadata_type, name):
        return self._fields.get(metadata_type, 'storage_unit', name)

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
