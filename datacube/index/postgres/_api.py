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
from sqlalchemy import create_engine, select, text, bindparam, exists, and_, or_, Index, func, alias
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.url import URL as EngineUrl
from sqlalchemy.exc import IntegrityError

from datacube.config import LocalConfig
from datacube.index.fields import OrExpression
from datacube.index.postgres.tables._core import schema_qualified
from datacube.index.postgres.tables._dataset import DATASET_LOCATION, METADATA_TYPE
from . import tables
from ._fields import parse_fields, NativeField
from .tables import DATASET, DATASET_SOURCE, STORAGE_TYPE, STORAGE_UNIT, DATASET_STORAGE, COLLECTION

DATASET_URI_FIELD = DATASET_LOCATION.c.uri_scheme + ':' + DATASET_LOCATION.c.uri_body
_DATASET_SELECT_FIELDS = (
    DATASET,
    # The most recent file uri. We may want more advanced path selection in the future...
    select([
        DATASET_URI_FIELD
    ]).where(
        and_(
            DATASET_LOCATION.c.dataset_ref == DATASET.c.id,
            DATASET_LOCATION.c.uri_scheme == 'file'
        )
    ).order_by(
        DATASET_LOCATION.c.added.desc()
    ).limit(1).label('local_uri')
)

PGCODE_UNIQUE_CONSTRAINT = '23505'

_LOG = logging.getLogger(__name__)


def _split_uri(uri):
    """
    Split the scheme and the remainder of the URI.

    >>> _split_uri('http://test.com/something.txt')
    ('http', '//test.com/something.txt')
    >>> _split_uri('eods:LS7_ETM_SYS_P31_GALPGS01-002_101_065_20160127')
    ('eods', 'LS7_ETM_SYS_P31_GALPGS01-002_101_065_20160127')
    >>> _split_uri('file://rhe-test-dev.prod.lan/data/fromASA/LANDSAT-7.89274.S4A2C1D3R3')
    ('file', '//rhe-test-dev.prod.lan/data/fromASA/LANDSAT-7.89274.S4A2C1D3R3')
    """
    comp = uri.split(':')
    scheme = comp[0]
    body = ':'.join(comp[1:])
    return scheme, body


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
    def connect(cls, hostname, database, username=None, password=None, port=None):
        _engine = create_engine(
            EngineUrl(
                'postgresql',
                host=hostname, database=database, port=port,
                username=username, password=password,
            ),
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
            config.db_password,
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

    def insert_dataset(self, metadata_doc, dataset_id, collection_id=None):
        """
        Insert dataset if not already indexed.
        :type metadata_doc: dict
        :type dataset_id: str or uuid.UUID
        :type collection_id: int
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
            collection_ref = bindparam('collection_ref')
            ret = self._connection.execute(
                # Insert if not exists.
                #     (there's still a tiny chance of a race condition: It will throw an integrity error if another
                #      connection inserts the same dataset in the time between the subquery and the main query.
                #      This is ok for our purposes.)
                DATASET.insert().from_select(
                    ['id', 'collection_ref', 'metadata_type_ref', 'metadata'],
                    select([
                        bindparam('id'), collection_ref,
                        select([
                            COLLECTION.c.metadata_type_ref
                        ]).where(
                            COLLECTION.c.id == collection_ref
                        ).label('metadata_type_ref'),
                        bindparam('metadata', type_=JSONB)
                    ]).where(~exists(select([DATASET.c.id]).where(DATASET.c.id == bindparam('id'))))
                ),
                id=dataset_id,
                collection_ref=collection_id,
                metadata=metadata_doc
            )
            return ret.rowcount > 0
        except IntegrityError as e:
            if e.orig.pgcode == PGCODE_UNIQUE_CONSTRAINT:
                _LOG.info('Duplicate dataset, not inserting: %s', dataset_id)
                # We're still going to raise it, because the transaction will have been invalidated.
            raise

    def ensure_dataset_location(self, dataset_id, uri):
        """
        Add a location to a dataset if it is not already recorded.
        :type dataset_id: str or uuid.UUID
        :type uri: str
        """
        scheme, body = _split_uri(uri)
        # Insert if not exists.
        #     (there's still a tiny chance of a race condition: It will throw an integrity error if another
        #      connection inserts the same location in the time between the subquery and the main query.
        #      This is ok for our purposes.)
        self._connection.execute(
            DATASET_LOCATION.insert().from_select(
                ['dataset_ref', 'uri_scheme', 'uri_body'],
                select([
                    bindparam('dataset_ref'), bindparam('uri_scheme'), bindparam('uri_body'),
                ]).where(
                    ~exists(select([DATASET_LOCATION.c.id]).where(
                        and_(
                            DATASET_LOCATION.c.dataset_ref == bindparam('dataset_ref'),
                            DATASET_LOCATION.c.uri_scheme == bindparam('uri_scheme'),
                            DATASET_LOCATION.c.uri_body == bindparam('uri_body'),
                        ),
                    ))
                )
            ),
            dataset_ref=dataset_id,
            uri_scheme=scheme,
            uri_body=body,
        )

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

    def get_storage_type(self, storage_type_id):
        return self._connection.execute(
            STORAGE_TYPE.select().where(STORAGE_TYPE.c.id == storage_type_id)
        ).first()

    def get_dataset(self, dataset_id):
        return self._connection.execute(
            select(_DATASET_SELECT_FIELDS).where(DATASET.c.id == dataset_id)
        ).first()

    def get_storage_types(self, dataset_metadata):
        """
        Find any storage types that match the given dataset.

        :type dataset_metadata: dict
        :rtype: dict
        """
        # Find any storage types whose 'dataset_metadata' document is a subset of the metadata.
        return self._connection.execute(
            STORAGE_TYPE.select().where(
                STORAGE_TYPE.c.dataset_metadata.contained_by(dataset_metadata)
            )
        ).fetchall()

    def get_all_storage_types(self):
        return self._connection.execute(
            STORAGE_TYPE.select()
        ).fetchall()

    def ensure_storage_type(self,
                            name,
                            dataset_metadata,
                            definition):
        res = self._connection.execute(
            STORAGE_TYPE.insert().values(
                name=name,
                dataset_metadata=dataset_metadata,
                definition=definition
            )
        )
        storage_type_id = res.inserted_primary_key[0]
        cube_sql_str = self._storage_unit_cube_sql_str(definition['storage']['dimension_order'])
        constraint = """alter table agdc.storage_unit add exclude using gist (%s with &&)
                        where (storage_type_ref = %s)""" % (cube_sql_str, storage_type_id)
        # TODO: must enforce cube extension somehow before we can do this
        # self._connection.execute(constraint)
        return storage_type_id

    def archive_storage_unit(self, storage_unit_id):
        self._connection.execute(DATASET_STORAGE.delete().where(DATASET_STORAGE.c.storage_unit_ref == storage_unit_id))
        self._connection.execute(STORAGE_UNIT.delete().where(STORAGE_UNIT.c.id == storage_unit_id))

    def _storage_unit_cube_sql_str(self, dimensions):
        def _array_str(p):
            return 'ARRAY[' + ','.join("CAST(descriptor #>> '{coordinates,%s,%s}' as numeric)" % (c, p)
                                       for c in dimensions) + ']'

        return "cube(" + ','.join(_array_str(p) for p in ['begin', 'end']) + ")"

    def get_storage_unit_overlap(self, storage_type):
        wild_sql_appears = self._storage_unit_cube_sql_str(storage_type.dimensions) + ' as cube'

        su1 = select([
            STORAGE_UNIT.c.id,
            text(wild_sql_appears)
        ]).where(STORAGE_UNIT.c.storage_type_ref == storage_type.id)
        su1 = alias(su1, name='su1')
        su2 = alias(su1, name='su2')

        overlaps = select([su1.c.id]).where(
            exists(
                select([1]).select_from(su2).where(
                    and_(
                        su1.c.id != su2.c.id,
                        text("su1.cube && su2.cube")
                    )
                )
            )
        )

        return self._connection.execute(overlaps).fetchall()

    def add_storage_unit(self, path, dataset_ids, descriptor, storage_type_id, size_bytes):
        if not dataset_ids:
            raise ValueError('Storage unit must be linked to at least one dataset.')

        # Get the collection/metadata-type for this storage unit.
        # We assume all datasets are of the same collection. (TODO: Revise when 'product type' concept is added)
        matched_collection = select([
            DATASET.c.collection_ref, DATASET.c.metadata_type_ref
        ]).where(
            DATASET.c.id == dataset_ids[0]
        ).cte('matched_collection')

        # Add the storage unit
        unit_id = self._connection.execute(
            STORAGE_UNIT.insert().values(
                collection_ref=select([matched_collection.c.collection_ref]),
                metadata_type_ref=select([matched_collection.c.metadata_type_ref]),
                storage_type_ref=storage_type_id,
                descriptor=descriptor,
                path=path,
                size_bytes=size_bytes
            ).returning(STORAGE_UNIT.c.id),
        ).scalar()

        # Link the storage unit to the datasets.
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
            'collection': NativeField(
                'collection',
                'Name of collection',
                None, COLLECTION.c.name
            )
        }
        dataset_search_fields = collection_result['definition']['dataset']['search_fields']

        # noinspection PyTypeChecker
        fields.update(
            parse_fields(
                dataset_search_fields,
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
            'type': NativeField(
                'type',
                'Storage type id',
                collection_result['id'],
                STORAGE_UNIT.c.storage_type_ref
            ),
            'path': NativeField(
                'path',
                'Path to storage file',
                collection_result['id'],
                STORAGE_UNIT.c.path
            )
        }
        storage_unit_def = collection_result['definition'].get('storage_unit')
        if storage_unit_def and 'search_fields' in storage_unit_def:
            unit_search_fields = storage_unit_def['search_fields']

            # noinspection PyTypeChecker
            fields.update(
                parse_fields(
                    unit_search_fields,
                    collection_result['id'],
                    STORAGE_UNIT.c.descriptor
                )
            )

        return fields

    def search_datasets_by_metadata(self, metadata):
        """
        Find any datasets that have the given metadata.

        :type metadata: dict
        :rtype: dict
        """
        # Find any storage types whose 'dataset_metadata' document is a subset of the metadata.
        return self._connection.execute(
            select(_DATASET_SELECT_FIELDS).where(DATASET.c.metadata.contains(metadata))
        ).fetchall()

    def search_datasets(self, expressions, select_fields=None):
        """
        :type select_fields: tuple[datacube.index.postgres._fields.PgField]
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: dict
        """
        select_fields = [
            f.alchemy_expression.label(f.name)
            for f in select_fields
            ] if select_fields else _DATASET_SELECT_FIELDS

        return self._search_docs(
            expressions,
            primary_table=DATASET,
            select_fields=select_fields,
        )

    def get_dataset_ids_for_storage_unit(self, storage_unit_id):
        return self._connection.execute(
            select([DATASET_STORAGE.c.dataset_ref]).where(DATASET_STORAGE.c.storage_unit_ref == storage_unit_id)
        ).fetchall()

    def search_storage_units(self, expressions, select_fields=None):
        """
        :type select_fields: tuple[datacube.index.postgres._fields.PgField]
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: dict
        """

        if select_fields:
            select_fields = [
                f.alchemy_expression.label(f.name)
                for f in select_fields
                ]
            group_by_fields = None
            required_tables = None
        else:
            # We include a list of dataset ids alongside each storage unit.
            select_fields = [
                STORAGE_UNIT,
                func.array_agg(
                    DATASET_STORAGE.c.dataset_ref
                ).label('dataset_refs')
            ]
            group_by_fields = (STORAGE_UNIT.c.id,)
            required_tables = (DATASET_STORAGE,)

        return self._search_docs(
            expressions,
            primary_table=STORAGE_UNIT,
            select_fields=select_fields,
            group_by_fields=group_by_fields,
            required_tables=required_tables
        )

    def _search_docs(self, expressions, primary_table, select_fields=None, group_by_fields=None, required_tables=None):
        """

        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :param primary_table: SQLAlchemy table
        :return:
        """
        from_expression, raw_expressions = _prepare_expressions(
            expressions, primary_table,
            required_tables=required_tables
        )

        select_query = select(select_fields).select_from(from_expression).where(and_(*raw_expressions))

        if group_by_fields:
            select_query = select_query.group_by(*group_by_fields)

        results = self._connection.execute(select_query)
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

    def get_metadata_type(self, id_):
        return self._connection.execute(
            METADATA_TYPE.select().where(METADATA_TYPE.c.id == id_)
        ).first()

    def get_collection_by_name(self, name):
        return self._connection.execute(
            COLLECTION.select().where(COLLECTION.c.name == name)
        ).first()

    def get_metadata_type_by_name(self, name):
        return self._connection.execute(
            METADATA_TYPE.select().where(METADATA_TYPE.c.name == name)
        ).first()

    def get_storage_type_by_name(self, name):
        return self._connection.execute(
            STORAGE_TYPE.select().where(STORAGE_TYPE.c.name == name)
        ).first()

    def add_collection(self,
                       name,
                       dataset_metadata,
                       match_priority,
                       metadata_type_id,
                       definition):
        res = self._connection.execute(
            COLLECTION.insert().values(
                name=name,
                dataset_metadata=dataset_metadata,
                metadata_type_ref=metadata_type_id,
                match_priority=match_priority,
                definition=definition
            )
        )
        return res.inserted_primary_key[0]

    def add_metadata_type(self, name, definition):
        res = self._connection.execute(
            METADATA_TYPE.insert().values(
                name=name,
                definition=definition
            )
        )
        type_id = res.inserted_primary_key[0]
        record = self.get_metadata_type(type_id)

        # Initialise search fields.
        _setup_collection_fields(
            self._connection, name, 'dataset', self.get_dataset_fields(record),
            DATASET.c.metadata_type_ref == type_id
        )
        _setup_collection_fields(
            self._connection, name, 'storage_unit', self.get_storage_unit_fields(record),
            STORAGE_UNIT.c.metadata_type_ref == type_id
        )

    def get_all_collections(self):
        return self._connection.execute(COLLECTION.select()).fetchall()

    def count_storage_types(self):
        return self._connection.execute(select([func.count()]).select_from(STORAGE_TYPE)).scalar()

    def get_locations(self, dataset_id):
        return [
            record[0]
            for record in self._connection.execute(
                select([
                    DATASET_URI_FIELD
                ]).where(
                    DATASET_LOCATION.c.dataset_ref == dataset_id
                ).order_by(
                    DATASET_LOCATION.c.added.desc()
                )
            ).fetchall()
            ]

    def __repr__(self):
        return "PostgresDb<engine={!r}>".format(self._engine)


def _pg_exists(conn, name):
    """
    Does a postgres object exist?
    :rtype bool
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
                    postgresql_where=where_expression,
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


def _prepare_expressions(expressions, primary_table, required_tables=None):
    """
    :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
    :param primary_table: SQLAlchemy table
    """
    # We currently only allow one metadata to be queried at a time (our indexes are per-type)
    metadata_type_references = set()
    join_tables = set(required_tables) if required_tables else set()

    def tables_referenced(expression):
        if isinstance(expression, OrExpression):
            return reduce_(lambda a, b: a | b, (tables_referenced(expr) for expr in expression.exprs), set())

        #: :type: datacube.index.postgres._fields.PgField
        field = expression.field
        table = field.alchemy_column.table
        metadata_type_id = field.metadata_type_id
        return {(table, metadata_type_id)}

    for table, metadata_type_id in reduce_(lambda a, b: a | b, (tables_referenced(expr) for expr in expressions),
                                           set()):
        if table != primary_table:
            join_tables.add(table)
        if metadata_type_id:
            metadata_type_references.add((table, metadata_type_id))

    unique_metadata_types = set([c[1] for c in metadata_type_references])
    if len(unique_metadata_types) > 1:
        raise ValueError(
            'Currently only one metadata type can be queried at a time. (Tried %r)' % metadata_type_references
        )

    def raw_expr(expression):
        if isinstance(expression, OrExpression):
            return or_(raw_expr(expr) for expr in expression.exprs)
        return expression.alchemy_expression

    raw_expressions = [raw_expr(expression) for expression in expressions]

    # We may have multiple references: storage.metadata_type_ref and dataset.metadata_type_ref.
    # We want to include all, to ensure the indexes are used.
    for from_table, queried_metadata_type in metadata_type_references:
        raw_expressions.insert(0, from_table.c.metadata_type_ref == queried_metadata_type)

    from_expression = _prepare_from_expression(primary_table, join_tables)

    return from_expression, raw_expressions


def _prepare_from_expression(primary_table, join_tables):
    """
    Calculate an SQLAlchemy from expression to join the given table to other required tables.
    """
    from_expression = primary_table
    middleman_tables = set(_JOIN_REQUIREMENTS.get((primary_table, table), None) for table in join_tables)
    for table in join_tables:
        # If this table will be used to join another, we can skip it.
        if table in middleman_tables:
            continue
        # Do we need any middle-men tables to join our tables?
        join_requirement = _JOIN_REQUIREMENTS.get((primary_table, table), None)
        if join_requirement is not None:
            from_expression = from_expression.join(join_requirement)
        from_expression = from_expression.join(table)
    return from_expression


def transform_object_tree(o, f):
    if isinstance(o, dict):
        return {k: transform_object_tree(v, f) for k, v in o.items()}
    if isinstance(o, list):
        return [transform_object_tree(v, f) for v in o]
    if isinstance(o, tuple):
        return tuple(transform_object_tree(v, f) for v in o)
    return f(o)


def _to_json(o):
    # Postgres <=9.5 doesn't support NaN and Infinity
    def fixup_value(v):
        if isinstance(v, float):
            if v != v:
                return "NaN"
            if v == float("inf"):
                return "Infinity"
            if v == float("-inf"):
                return "-Infinity"
        if isinstance(v, (datetime.datetime, datetime.date)):
            return v.isoformat()
        if isinstance(v, numpy.dtype):
            return v.name
        return v

    fixedup = transform_object_tree(o, fixup_value)

    return json.dumps(fixedup, default=_json_fallback)


def _json_fallback(obj):
    """Fallback json serialiser."""
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
