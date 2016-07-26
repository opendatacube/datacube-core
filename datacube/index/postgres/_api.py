# coding=utf-8

# We often have one-arg-per column, so these checks aren't so useful.
# pylint: disable=too-many-arguments,too-many-public-methods

# SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
# pylint: disable=singleton-comparison

"""
Lower-level database access.
"""
from __future__ import absolute_import

import json
import logging
import re

from sqlalchemy import create_engine, select, text, bindparam, and_, or_, Index, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.url import URL as EngineUrl
from sqlalchemy.exc import IntegrityError

import datacube
from datacube.config import LocalConfig
from datacube.index.exceptions import DuplicateRecordError
from datacube.index.fields import OrExpression
from datacube.utils import jsonify_document
from . import tables
from ._fields import parse_fields, NativeField
from .tables import DATASET, DATASET_SOURCE, METADATA_TYPE, DATASET_LOCATION, DATASET_TYPE

_LIB_ID = 'agdc-' + str(datacube.__version__)
APP_NAME_PATTERN = re.compile('^[a-zA-Z0-9-]+$')

DATASET_URI_FIELD = DATASET_LOCATION.c.uri_scheme + ':' + DATASET_LOCATION.c.uri_body
_DATASET_SELECT_FIELDS = (
    DATASET,
    # The most recent file uri. We may want more advanced path selection in the future...
    select([
        DATASET_URI_FIELD,
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


class IndexSetupError(Exception):
    pass


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
    def connect(cls, hostname, database, username=None, password=None, port=None, application_name=None, validate=True):
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
            connect_args={'application_name': application_name}
        )
        if validate:
            if not tables.database_exists(_engine):
                raise IndexSetupError('\n\nNo DB schema exists. Have you run init?\n\t{init_command}'.format(
                    init_command='datacube system init'
                ))

            if not tables.schema_is_latest(_engine):
                raise IndexSetupError(
                    '\n\nDB schema is out of date. '
                    'An administrator must run init:\n\t{init_command}'.format(
                        init_command='datacube -v system init'
                    ))

        _connection = _engine.connect()
        return PostgresDb(_engine, _connection)

    @classmethod
    def from_config(cls, config=LocalConfig.find(), application_name=None, validate_db=True):
        app_name = cls._expand_app_name(application_name)

        return PostgresDb.connect(
            config.db_hostname,
            config.db_database,
            config.db_username,
            config.db_password,
            config.db_port,
            application_name=app_name,
            validate=validate_db
        )

    @classmethod
    def _expand_app_name(cls, application_name):
        """
        >>> PostgresDb._expand_app_name(None) #doctest: +ELLIPSIS
        'agdc-...'
        >>> PostgresDb._expand_app_name('cli') #doctest: +ELLIPSIS
        'cli agdc-...'
        >>> PostgresDb._expand_app_name('not valid')
        Traceback (most recent call last):
        ...
        ValueError: Invalid application name 'not valid': Must be alphanumeric with dashes.
        >>> PostgresDb._expand_app_name('') #doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        ValueError: Invalid application name '': Must be alphanumeric with dashes.
        """
        full_name = _LIB_ID
        if application_name is not None:
            if not APP_NAME_PATTERN.match(application_name):
                raise ValueError('Invalid application name %r: Must be alphanumeric with dashes.' % application_name)

            full_name = application_name + ' ' + _LIB_ID

        if len(full_name) > 64:
            raise ValueError('Application name is too long: Maximum %s chars' % (64 - len(_LIB_ID)))
        return full_name

    def init(self, with_permissions=True):
        """
        Init a new database (if not already set up).

        :return: If it was newly created.
        """
        is_new = tables.ensure_db(self._engine, with_permissions=with_permissions)
        if not is_new:
            tables.update_schema(self._engine)

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

    def insert_dataset(self, metadata_doc, dataset_id, dataset_type_id):
        """
        Insert dataset if not already indexed.
        :type metadata_doc: dict
        :type dataset_id: str or uuid.UUID
        :type dataset_type_id: int
        :return: whether it was inserted
        :rtype: bool
        """
        try:
            dataset_type_ref = bindparam('dataset_type_ref')
            ret = self._connection.execute(
                DATASET.insert().from_select(
                    ['id', 'dataset_type_ref', 'metadata_type_ref', 'metadata'],
                    select([
                        bindparam('id'), dataset_type_ref,
                        select([
                            DATASET_TYPE.c.metadata_type_ref
                        ]).where(
                            DATASET_TYPE.c.id == dataset_type_ref
                        ).label('metadata_type_ref'),
                        bindparam('metadata', type_=JSONB)
                    ])
                ),
                id=dataset_id,
                dataset_type_ref=dataset_type_id,
                metadata=metadata_doc
            )
            return ret.rowcount > 0
        except IntegrityError as e:
            if e.orig.pgcode == PGCODE_UNIQUE_CONSTRAINT:
                raise DuplicateRecordError('Duplicate dataset, not inserting: %s' % dataset_id)
            raise

    def ensure_dataset_location(self, dataset_id, uri):
        """
        Add a location to a dataset if it is not already recorded.
        :type dataset_id: str or uuid.UUID
        :type uri: str
        """
        scheme, body = _split_uri(uri)

        try:
            self._connection.execute(
                DATASET_LOCATION.insert(),
                dataset_ref=dataset_id,
                uri_scheme=scheme,
                uri_body=body,
            )
        except IntegrityError as e:
            if e.orig.pgcode == PGCODE_UNIQUE_CONSTRAINT:
                raise DuplicateRecordError('Location already exists: %s' % uri)
            raise

    def contains_dataset(self, dataset_id):
        return bool(self._connection.execute(select([DATASET.c.id]).where(DATASET.c.id == dataset_id)).fetchone())

    def insert_dataset_source(self, classifier, dataset_id, source_dataset_id):
        try:
            self._connection.execute(
                DATASET_SOURCE.insert(),
                classifier=classifier,
                dataset_ref=dataset_id,
                source_dataset_ref=source_dataset_id
            )
        except IntegrityError as e:
            if e.orig.pgcode == PGCODE_UNIQUE_CONSTRAINT:
                raise DuplicateRecordError('Source already exists')
            raise

    def get_dataset(self, dataset_id):
        return self._connection.execute(
            select(_DATASET_SELECT_FIELDS).where(DATASET.c.id == dataset_id)
        ).first()

    def get_derived_datasets(self, dataset_id):
        return self._connection.execute(
            select(
                _DATASET_SELECT_FIELDS
            ).select_from(
                DATASET.join(DATASET_SOURCE, DATASET.c.id == DATASET_SOURCE.c.dataset_ref)
            ).where(
                DATASET_SOURCE.c.source_dataset_ref == dataset_id
            )
        ).fetchall()

    def get_dataset_sources(self, dataset_id):
        # recursively build the list of (dataset_ref, source_dataset_ref) pairs starting from dataset_id
        # include (dataset_ref, NULL) [hence the left join]
        sources = select(
            [DATASET.c.id.label('dataset_ref'),
             DATASET_SOURCE.c.source_dataset_ref,
             DATASET_SOURCE.c.classifier]
        ).select_from(
            DATASET.join(DATASET_SOURCE,
                         DATASET.c.id == DATASET_SOURCE.c.dataset_ref,
                         isouter=True)
        ).where(
            DATASET.c.id == dataset_id
        ).cte(name="sources", recursive=True)

        sources = sources.union_all(
            select(
                [sources.c.source_dataset_ref.label('dataset_ref'),
                 DATASET_SOURCE.c.source_dataset_ref,
                 DATASET_SOURCE.c.classifier]
            ).select_from(
                sources.join(DATASET_SOURCE,
                             sources.c.source_dataset_ref == DATASET_SOURCE.c.dataset_ref,
                             isouter=True)
            ).where(sources.c.source_dataset_ref != None))

        # turn the list of pairs into adjacency list (dataset_ref, [source_dataset_ref, ...])
        # some source_dataset_ref's will be NULL
        aggd = select(
            [sources.c.dataset_ref,
             func.array_agg(sources.c.source_dataset_ref).label('sources'),
             func.array_agg(sources.c.classifier).label('classes')]
        ).group_by(sources.c.dataset_ref).alias('aggd')

        # join the adjacency list with datasets table
        query = select(
            _DATASET_SELECT_FIELDS + (aggd.c.sources, aggd.c.classes)
        ).select_from(aggd.join(DATASET, DATASET.c.id == aggd.c.dataset_ref))

        return self._connection.execute(query).fetchall()

    def get_dataset_fields(self, metadata_type_result):
        # Native fields (hard-coded into the schema)
        fields = {
            'id': NativeField(
                'id',
                None,
                None,
                DATASET.c.id
            ),
            'product': NativeField(
                'product',
                'Dataset type name',
                None,
                DATASET_TYPE.c.name
            ),
            'dataset_type_id': NativeField(
                'dataset_type_id',
                'ID of a dataset type',
                None,
                DATASET.c.dataset_type_ref
            ),
            'metadata_type': NativeField(
                'metadata_type',
                'Metadata type of dataset',
                None,
                METADATA_TYPE.c.name
            ),
            'metadata_type_id': NativeField(
                'metadata_type_id',
                'ID of a metadata type',
                None,
                DATASET.c.metadata_type_ref
            ),
        }
        dataset_search_fields = metadata_type_result['definition']['dataset']['search_fields']

        # noinspection PyTypeChecker
        fields.update(
            parse_fields(
                dataset_search_fields,
                metadata_type_result['id'],
                DATASET.c.metadata
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

    def search_datasets(self, expressions, select_fields=None, with_source_ids=False):
        """
        :type with_source_ids: bool
        :type select_fields: tuple[datacube.index.postgres._fields.PgField]
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: dict
        """

        if select_fields:
            select_columns = tuple(
                f.alchemy_expression.label(f.name)
                for f in select_fields
            )
        else:
            select_columns = _DATASET_SELECT_FIELDS

        if with_source_ids:
            # Include the IDs of source datasets
            select_columns += (
                select(
                    (func.array_agg(DATASET_SOURCE.c.source_dataset_ref),)
                ).select_from(
                    DATASET_SOURCE
                ).where(
                    DATASET_SOURCE.c.dataset_ref == DATASET.c.id
                ).group_by(
                    DATASET_SOURCE.c.dataset_ref
                ).label('dataset_refs'),
            )

        def raw_expr(expression):
            if isinstance(expression, OrExpression):
                return or_(raw_expr(expr) for expr in expression.exprs)
            return expression.alchemy_expression

        raw_expressions = [raw_expr(expression) for expression in expressions]

        select_query = (
            select(
                select_columns
            ).select_from(
                self._from_expression(DATASET, expressions)
            ).where(
                and_(DATASET.c.archived == None, *raw_expressions)
            )
        )

        results = self._connection.execute(select_query)
        for result in results:
            yield result

    def count_datasets(self, expressions):
        """
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: int
        """

        def raw_expr(expression):
            if isinstance(expression, OrExpression):
                return or_(raw_expr(expr) for expr in expression.exprs)
            return expression.alchemy_expression

        raw_expressions = [raw_expr(expression) for expression in expressions]

        select_query = (
            select(
                [func.count('*')]
            ).select_from(
                self._from_expression(DATASET, expressions)
            ).where(
                and_(DATASET.c.archived == None, *raw_expressions)
            )
        )

        return self._connection.scalar(select_query)

    def _from_expression(self, source_table, expressions):
        join_tables = set([expression.field.required_alchemy_table for expression in expressions])
        from_expression = source_table
        for table in join_tables:
            if table != source_table:
                from_expression = from_expression.join(table)
        return from_expression

    def get_dataset_type(self, id_):
        return self._connection.execute(
            DATASET_TYPE.select().where(DATASET_TYPE.c.id == id_)
        ).first()

    def get_metadata_type(self, id_):
        return self._connection.execute(
            METADATA_TYPE.select().where(METADATA_TYPE.c.id == id_)
        ).first()

    def get_dataset_type_by_name(self, name):
        return self._connection.execute(
            DATASET_TYPE.select().where(DATASET_TYPE.c.name == name)
        ).first()

    def get_metadata_type_by_name(self, name):
        return self._connection.execute(
            METADATA_TYPE.select().where(METADATA_TYPE.c.name == name)
        ).first()

    def add_dataset_type(self,
                         name,
                         metadata,
                         metadata_type_id,
                         definition, concurrently=False):

        metadata_type_record = self.get_metadata_type(metadata_type_id)

        res = self._connection.execute(
            DATASET_TYPE.insert().values(
                name=name,
                metadata=metadata,
                metadata_type_ref=metadata_type_id,
                definition=definition
            )
        )

        type_id = res.inserted_primary_key[0]

        # Initialise search fields.
        _setup_collection_fields(
            self._connection, name, self.get_dataset_fields(metadata_type_record),
            where_expression=and_(DATASET.c.archived == None, DATASET.c.dataset_type_ref == type_id),
            concurrently=concurrently
        )
        return type_id

    def add_metadata_type(self, name, definition, concurrently=False):
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
            self._connection, name, self.get_dataset_fields(record),
            where_expression=and_(DATASET.c.archived == None, DATASET.c.metadata_type_ref == type_id),
            concurrently=concurrently
        )

    def check_dynamic_fields(self, concurrently=False, rebuild_all=False):
        _LOG.info('Checking dynamic views/indexes. (rebuild all = %s)', rebuild_all)
        for metadata_type in self.get_all_metadata_types():
            _setup_collection_fields(
                self._connection, metadata_type['name'], self.get_dataset_fields(metadata_type),
                where_expression=and_(DATASET.c.archived == None, DATASET.c.metadata_type_ref == metadata_type['id']),
                concurrently=concurrently,
                replace_existing=rebuild_all
            )

        for dataset_type in self.get_all_dataset_types():
            _setup_collection_fields(
                self._connection, dataset_type['name'],
                self.get_dataset_fields(self.get_metadata_type(dataset_type['metadata_type_ref'])),
                where_expression=and_(DATASET.c.archived == None, DATASET.c.dataset_type_ref == dataset_type['id']),
                concurrently=concurrently,
                replace_existing=rebuild_all
            )

    def get_all_dataset_types(self):
        return self._connection.execute(DATASET_TYPE.select().order_by(DATASET_TYPE.c.name.asc())).fetchall()

    def get_all_metadata_types(self):
        return self._connection.execute(METADATA_TYPE.select().order_by(METADATA_TYPE.c.name.asc())).fetchall()

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

    def list_users(self):
        result = self._connection.execute("""
            select
                group_role.rolname as role_name,
                user_role.rolname as user_name,
                pg_catalog.shobj_description(user_role.oid, 'pg_authid') as description
            from pg_roles group_role
            inner join pg_auth_members am on am.roleid = group_role.oid
            inner join pg_roles user_role on am.member = user_role.oid
            where (group_role.rolname like 'agdc_%%') and not (user_role.rolname like 'agdc_%%')
            order by group_role.oid asc, user_role.oid asc;
        """)
        for row in result:
            yield _from_pg_role(row['role_name']), row['user_name'], row['description']

    def create_user(self, username, password, role):
        pg_role = _to_pg_role(role)
        tables.create_user(self._engine, username, password, pg_role)

    def grant_role(self, role, users):
        """
        Grant a role to a user.
        """
        pg_role = _to_pg_role(role)

        for user in users:
            if not tables.has_role(self._engine, user):
                raise ValueError('Unknown user %r' % user)

        tables.grant_role(self._engine, pg_role, users)


def _to_pg_role(role):
    """
    >>> _to_pg_role('ingest')
    'agdc_ingest'
    >>> _to_pg_role('fake')
    Traceback (most recent call last):
    ...
    ValueError: Unknown role 'fake'. Expected one of ...
    """
    pg_role = 'agdc_' + role.lower()
    if pg_role not in tables.USER_ROLES:
        raise ValueError(
            'Unknown role %r. Expected one of %r' %
            (role, [r.split('_')[1] for r in tables.USER_ROLES])
        )
    return pg_role


def _from_pg_role(pg_role):
    """
    >>> _from_pg_role('agdc_admin')
    'admin'
    >>> _from_pg_role('fake')
    Traceback (most recent call last):
    ...
    ValueError: Not a pg role: 'fake'. Expected one of ...
    """
    if pg_role not in tables.USER_ROLES:
        raise ValueError('Not a pg role: %r. Expected one of %r' % (pg_role, tables.USER_ROLES))

    return pg_role.split('_')[1]


def _pg_exists(conn, name):
    """
    Does a postgres object exist?
    :rtype bool
    """
    return conn.execute("SELECT to_regclass(%s)", name).scalar() is not None


def _setup_collection_fields(conn, collection_prefix, fields, where_expression,
                             concurrently=False, replace_existing=False):
    """
    Create indexes and views for a collection's search fields.
    """
    name = collection_prefix.lower()

    # Create indexes for the search fields.
    for field in fields.values():
        index_type = field.postgres_index_type
        if index_type:
            # Our normal indexes start with "ix_", dynamic indexes with "dix_"
            index_name = 'dix_{prefix}_{field_name}'.format(
                prefix=name.lower(),
                field_name=field.name.lower()
            )
            # Previous naming scheme
            legacy_name = 'dix_field_{prefix}_dataset_{field_name}'.format(
                prefix=name.lower(),
                field_name=field.name.lower()
            )
            index = Index(
                index_name,
                field.alchemy_expression,
                postgresql_where=where_expression,
                postgresql_using=index_type,
                # Don't lock the table (in the future we'll allow indexing new fields...)
                postgresql_concurrently=concurrently
            )
            exists = _pg_exists(conn, tables.schema_qualified(index_name))
            legacy_exists = _pg_exists(conn, tables.schema_qualified(legacy_name))

            # This currently leaves a window of time without indexes: it's primarily intended for development.
            if replace_existing:
                if exists:
                    _LOG.debug('Dropping index: %s (replace=%r)', index_name, replace_existing)
                    index.drop(conn)
                    exists = False
                if legacy_exists:
                    _LOG.debug('Dropping legacy index: %s (replace=%r)', legacy_name, replace_existing)
                    Index(legacy_name, field.alchemy_expression).drop(conn)
                    legacy_exists = False

            if not (exists or legacy_exists):
                _LOG.debug('Creating index: %s', index_name)
                index.create(conn)
            else:
                _LOG.debug('Index exists: %s  (replace=%r)', index_name, replace_existing)

    # Create a view of search fields (for debugging convenience).
    # 'dv_' prefix: dynamic view. To distinguish from views that are created as part of the schema itself.
    view_name = tables.schema_qualified('dv_{}_dataset'.format(name))
    exists = _pg_exists(conn, view_name)

    # This currently leaves a window of time without the views: it's primarily intended for development.
    if exists and replace_existing:
        _LOG.debug('Dropping view: %s (replace=%r)', view_name, replace_existing)
        conn.execute('drop view %s' % view_name)
        exists = False

    if not exists:
        _LOG.debug('Creating view: %s', view_name)
        conn.execute(
            tables.CreateView(
                view_name,
                select(
                    [field.alchemy_expression.label(field.name) for field in fields.values()]
                ).select_from(
                    DATASET
                ).where(where_expression)
            )
        )
    else:
        _LOG.debug('View exists: %s (replace=%r)', view_name, replace_existing)

    legacy_name = tables.schema_qualified('{}_dataset'.format(name))
    if _pg_exists(conn, legacy_name):
        _LOG.debug('Dropping legacy view: %s', legacy_name)
        conn.execute('drop view %s' % legacy_name)


def _to_json(o):
    # Postgres <=9.5 doesn't support NaN and Infinity
    fixedup = jsonify_document(o)
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
