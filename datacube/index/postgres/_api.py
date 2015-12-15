# coding=utf-8
# We often have one-arg-per column, so these checks aren't so useful.
# pylint: disable=too-many-arguments,too-many-public-methods
"""
Lower-level database access.
"""
from __future__ import absolute_import

import datetime
import json
import logging

from functools import reduce as reduce_

import numpy
from sqlalchemy import create_engine, select, text, bindparam, exists, and_, or_, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.url import URL as EngineUrl
from sqlalchemy.exc import IntegrityError

from datacube.config import LocalConfig
from datacube.index.fields import OrExpression
from datacube.index.postgres.tables._core import schema_qualified
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

    def insert_dataset(self, metadata_doc, dataset_id, path=None, collection_id=None):
        """
        Insert dataset if not already indexed.
        :type metadata_doc: dict
        :type dataset_id: str or uuid.UUID
        :type path: pathlib.Path
        :return: whether it was inserted
        :rtype: bool
        """
        if collection_id is None:
            collection_result = self.get_collection_for_doc(metadata_doc)
            if not collection_result:
                _LOG.debug('Attempted failed match on doc %r', metadata_doc)
                raise RuntimeError('No collection matches dataset')
            collection_id = collection_result['id']
            _LOG.debug('Matched collection %r', collection_id)
        else:
            _LOG.debug('Using provided collection %r', collection_id)

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
        res = self._connection.execute(
            DATASET_SOURCE.insert(),
            classifier=classifier,
            dataset_ref=dataset_id,
            source_dataset_ref=source_dataset_id
        )
        return res.inserted_primary_key[0]

    def ensure_storage_type(self, driver, name, descriptor, description=None):
        # TODO: Update them if they already exist. This will do for now.
        res = self._connection.execute(
            STORAGE_TYPE.insert(),
            driver=driver,
            name=name,
            description=description,
            descriptor=descriptor
        )
        return res.inserted_primary_key[0]

    def get_storage_type(self, storage_type_id):
        return self._connection.execute(
            STORAGE_TYPE.select().where(STORAGE_TYPE.c.id == storage_type_id)
        ).first()

    def get_storage_type_by_name(self, name):
        return self._connection.execute(
            STORAGE_TYPE.select().where(STORAGE_TYPE.c.name == name)
        ).first()

    def get_storage_mapping(self, storage_mapping_id):
        return self._connection.execute(
            STORAGE_MAPPING.select().where(STORAGE_MAPPING.c.id == storage_mapping_id)
        ).first()

    def get_dataset(self, dataset_id):
        return self._connection.execute(
            DATASET.select().where(DATASET.c.id == dataset_id)
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

    def ensure_storage_mapping(self, storage_type_name,
                               name, location_name, file_path_template,
                               dataset_metadata, measurements,
                               description=None):
        res = self._connection.execute(
            STORAGE_MAPPING.insert().values(
                storage_type_ref=select([STORAGE_TYPE.c.id]).where(
                    STORAGE_TYPE.c.name == storage_type_name
                ),
                name=name,
                description=description,
                dataset_metadata=dataset_metadata,
                measurements=measurements,
                location_name=location_name,
                file_path_template=file_path_template,
            )
        )
        return res.inserted_primary_key[0]

    def add_storage_unit(self, path, dataset_ids, descriptor, storage_mapping_id):
        if not dataset_ids:
            raise ValueError('Storage unit must be linked to at least one dataset.')

        unit_id = self._connection.execute(
            STORAGE_UNIT.insert().values(
                collection_ref=select([DATASET.c.collection_ref]).where(DATASET.c.id == dataset_ids[0]),
                storage_mapping_ref=storage_mapping_id,
                descriptor=descriptor,
                path=path
            ).returning(STORAGE_UNIT.c.id),
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

    def get_dataset_fields(self, collection_result):
        # Native fields (hard-coded into the schema)
        fields = {
            'id': NativeField(
                'id',
                None,
                None,
                DATASET.c.id
            ),
            'metadata_path': NativeField(
                'metadata_path',
                'Path to metadata file',
                None,
                DATASET.c.metadata_path
            ),
            'collection': NativeField(
                'collection',
                'Name of collection',
                None, COLLECTION.c.name
            )
        }
        # noinspection PyTypeChecker
        fields.update(
            parse_fields(
                collection_result['dataset_search_fields'],
                collection_result['id'],
                DATASET.c.metadata
            )
        )
        return fields

    def get_storage_unit_fields(self, collection_result):
        # Native fields (hard-coded into the schema)
        fields = {
            'id': NativeField(
                'id',
                None,
                collection_result['id'],
                STORAGE_UNIT.c.id
            ),
            'path': NativeField(
                'path',
                'Path to storage file',
                collection_result['id'],
                STORAGE_UNIT.c.path
            )
        }
        # noinspection PyTypeChecker
        fields.update(
            parse_fields(
                collection_result['storage_unit_search_fields'],
                collection_result['id'],
                STORAGE_UNIT.c.descriptor
            )
        )
        return fields

    def search_datasets(self, expressions, select_fields=None):
        """
        :type select_fields: tuple[datacube.index.postgres._fields.PgField]
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: dict
        """
        return self._search_docs(
            expressions,
            primary_table=DATASET,
            select_fields=select_fields,
        )

    def search_storage_units(self, expressions, select_fields=None):
        """
        :type select_fields: tuple[datacube.index.postgres._fields.PgField]
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: dict
        """
        return self._search_docs(
            expressions,
            primary_table=STORAGE_UNIT,
            select_fields=select_fields
        )

    def _search_docs(self, expressions, primary_table, select_fields=None):
        """

        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :type select_fields: tuple[datacube.index.postgres._fields.PgField]
        :param primary_table: SQLAlchemy table
        :return:
        """
        select_fields = [
            f.alchemy_expression.label(f.name)
            for f in select_fields
            ] if select_fields else [primary_table]

        from_expression, raw_expressions = _prepare_expressions(expressions, primary_table)

        results = self._connection.execute(
            select(select_fields).select_from(from_expression).where(
                and_(*raw_expressions)
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

    def get_collection_by_name(self, name):
        return self._connection.execute(
            COLLECTION.select().where(COLLECTION.c.name == name)
        ).first()

    def get_storage_mapping_by_name(self, storage_type_name, name):
        return self._connection.execute(
            select(
                [STORAGE_MAPPING]
            ).select_from(
                STORAGE_MAPPING.join(STORAGE_TYPE)
            ).where(
                and_(
                    STORAGE_MAPPING.c.name == name,
                    STORAGE_TYPE.c.name == storage_type_name
                )
            )
        ).first()

    def add_collection(self,
                       name,
                       dataset_metadata, match_priority,
                       dataset_id_offset, dataset_label_offset,
                       dataset_creation_dt_offset, dataset_measurements_offset,
                       dataset_sources_offset,
                       dataset_search_fields,
                       storage_unit_search_fields,
                       description=None):
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
                dataset_sources_offset=dataset_sources_offset,
                dataset_search_fields=dataset_search_fields,
                storage_unit_search_fields=storage_unit_search_fields
            )
        )

        collection_id = res.inserted_primary_key[0]
        collection_result = self.get_collection(collection_id)

        # Initialise search fields.
        _setup_collection_fields(
            self._connection, name, 'dataset', self.get_dataset_fields(collection_result),
            DATASET.c.collection_ref == collection_id
        )
        _setup_collection_fields(
            self._connection, name, 'storage_unit', self.get_storage_unit_fields(collection_result),
            STORAGE_UNIT.c.collection_ref == collection_id
        )

    def get_all_collections(self):
        return self._connection.execute(COLLECTION.select()).fetchall()


def _pg_exists(conn, name):
    """
    Does a postgres object exist?
    """
    return conn.execute("SELECT to_regclass(%s)", name).scalar() is not None


def _setup_collection_fields(conn, collection_prefix, doc_prefix, fields, where_expression):
    """
    Create indexes and views for a collection's search fields.
    """
    name = '{}_{}'.format(collection_prefix.lower(), doc_prefix.lower())

    # Create indexes for the search fields.
    for field in fields.values():
        index_type = field.postgres_index_type
        if index_type:
            _LOG.debug('Creating index: %s', field.name)
            index_name = 'ix_field_{prefix}_{field_name}'.format(
                prefix=name.lower(),
                field_name=field.name.lower()
            )

            if not _pg_exists(conn, schema_qualified(index_name)):
                Index(
                    index_name,
                    field.alchemy_expression,
                    postgres_where=where_expression,
                    postgresql_using=index_type,
                    # Don't lock the table (in the future we'll allow indexing new fields...)
                    postgresql_concurrently=True
                ).create(conn)

    # Create a view of search fields (for debugging convenience).
    view_name = schema_qualified(name)
    if not _pg_exists(conn, view_name):
        conn.execute(
            tables.View(
                view_name,
                select(
                    [field.alchemy_expression.label(field.name) for field in fields.values()]
                ).where(where_expression)
            )
        )


_JOIN_REQUIREMENTS = {
    # To join dataset to storage unit, use this table.
    (DATASET, STORAGE_UNIT): DATASET_STORAGE,
    (STORAGE_UNIT, DATASET): DATASET_STORAGE
}


def _prepare_expressions(expressions, primary_table):
    """
    :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
    :param primary_table: SQLAlchemy table
    """
    # We currently only allow one collection to be queried (our indexes are per-collection)
    collection_references = set()
    join_tables = set()

    def tables_referenced(expression):
        if isinstance(expression, OrExpression):
            return reduce_(lambda a, b: a | b, (tables_referenced(expr) for expr in expression.exprs), set())

        field = expression.field
        table = field.alchemy_column.table
        collection_id = field.collection_id
        return {(table, collection_id)}

    for table, collection_id in reduce_(lambda a, b: a | b, (tables_referenced(expr) for expr in expressions), set()):
        if table != primary_table:
            join_tables.add(table)
        if collection_id:
            collection_references.add((table, collection_id))

    unique_collections = set([c[1] for c in collection_references])
    if len(unique_collections) > 1:
        raise ValueError(
            'Currently only one collection can be queried at a time. (Tried %r)' % collection_references
        )

    def raw_expr(expression):
        if isinstance(expression, OrExpression):
            return or_(raw_expr(expr) for expr in expression.exprs)
        return expression.alchemy_expression

    raw_expressions = [raw_expr(expression) for expression in expressions]

    # We may have multiple references: storage.collection_ref and dataset.collection_ref.
    # We want to include all, to ensure the indexes are used.
    for from_table, queried_collection in collection_references:
        raw_expressions.insert(0, from_table.c.collection_ref == queried_collection)

    from_expression = primary_table
    for table in join_tables:
        # Do we need any middle-men tables to join our tables?
        join_requirement = _JOIN_REQUIREMENTS.get((primary_table, table), None)
        if join_requirement is not None:
            from_expression = from_expression.join(join_requirement)
        from_expression = from_expression.join(table)

    return from_expression, raw_expressions


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
