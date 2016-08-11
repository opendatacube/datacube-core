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

from sqlalchemy import cast
from sqlalchemy import create_engine, select, text, bindparam, and_, or_, Index, func
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.url import URL as EngineUrl
from sqlalchemy.exc import IntegrityError

import datacube
from datacube.config import LocalConfig
from datacube.index.exceptions import DuplicateRecordError
from datacube.index.fields import OrExpression
from datacube.model import Range
from datacube.utils import jsonify_document
from datacube.compat import string_types
from . import tables
from ._fields import parse_fields, NativeField
from .tables import DATASET, DATASET_SOURCE, METADATA_TYPE, DATASET_LOCATION, DATASET_TYPE

_LIB_ID = 'agdc-' + str(datacube.__version__)

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

    Thread safe: the only shared state is the (thread-safe) sqlalchemy connection pool.

    But not multiprocess safe once the first connections are made! A connection must not be shared between multiple
    processes. You can call close() before forking if you know no other threads currently hold connections,
    or else use a separate instance of this class in each process.
    """

    def __init__(self, engine):
        # We don't recommend using this constructor directly as it may change.
        # Use static methods PostgresDb.create() or PostgresDb.from_config()
        self._engine = engine

    @classmethod
    def create(cls, hostname, database, username=None, password=None, port=None, application_name=None, validate=True):
        engine = create_engine(
            EngineUrl(
                'postgresql',
                host=hostname, database=database, port=port,
                username=username, password=password,
            ),
            echo=False,
            echo_pool=False,

            # 'AUTOCOMMIT' here means READ-COMMITTED isolation level with autocommit on.
            # When a transaction is needed we will do an explicit begin/commit.
            isolation_level='AUTOCOMMIT',

            json_serializer=_to_json,
            connect_args={'application_name': application_name}
        )
        if validate:
            if not tables.database_exists(engine):
                raise IndexSetupError('\n\nNo DB schema exists. Have you run init?\n\t{init_command}'.format(
                    init_command='datacube system init'
                ))

            if not tables.schema_is_latest(engine):
                raise IndexSetupError(
                    '\n\nDB schema is out of date. '
                    'An administrator must run init:\n\t{init_command}'.format(
                        init_command='datacube -v system init'
                    ))
        return PostgresDb(engine)

    @classmethod
    def from_config(cls, config=LocalConfig.find(), application_name=None, validate_connection=True):
        app_name = cls._expand_app_name(application_name)

        return PostgresDb.create(
            config.db_hostname,
            config.db_database,
            config.db_username,
            config.db_password,
            config.db_port,
            application_name=app_name,
            validate=validate_connection
        )

    @property
    def _connection(self):
        """
        Borrow a connection from the pool.
        """
        return self._engine.connect()

    def close(self):
        """
        Close any idle connections in the pool.

        This is good practice if you are keeping this object in scope
        but wont be using it for a while.

        Connections should not be shared between processes, so this should be called
        before forking if the same instance will be used.

        (connections are normally closed automatically when this object is
         garbage collected)
        """
        self._engine.dispose()

    @classmethod
    def _expand_app_name(cls, application_name):
        """
        >>> PostgresDb._expand_app_name(None) #doctest: +ELLIPSIS
        'agdc-...'
        >>> PostgresDb._expand_app_name('') #doctest: +ELLIPSIS
        'agdc-...'
        >>> PostgresDb._expand_app_name('cli') #doctest: +ELLIPSIS
        'cli agdc-...'
        >>> PostgresDb._expand_app_name('a b.c/d')
        'a-b-c-d agdc-...'
        >>> PostgresDb._expand_app_name(5)
        Traceback (most recent call last):
        ...
        TypeError: Application name must be a string
        """
        full_name = _LIB_ID
        if application_name:
            if not isinstance(application_name, string_types):
                raise TypeError('Application name must be a string')

            full_name = re.sub('[^0-9a-zA-Z]+', '-', application_name) + ' ' + full_name

        if len(full_name) > 64:
            _LOG.warning('Application name is too long: Truncating to %s chars', (64 - len(_LIB_ID) - 1))
        return full_name[-64:]

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

        Returns an instance that will maintain a single connection in a transaction.

        Call commit() or rollback() to complete the transaction or use a context manager:

            with db.begin() as trans:
                trans.insert_dataset(...)

        :rtype: _PostgresDbInTransaction
        """
        return _PostgresDbInTransaction(self._engine)

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

    def archive_dataset(self, dataset_id):
        self._connection.execute(
            DATASET.update().where(
                DATASET.c.id == dataset_id
            ).where(
                DATASET.c.archived == None
            ).values(
                archived=func.now()
            )
        )

    def restore_dataset(self, dataset_id):
        self._connection.execute(
            DATASET.update().where(
                DATASET.c.id == dataset_id
            ).values(
                archived=None
            )
        )

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

    @staticmethod
    def _alchemify_expressions(expressions):
        def raw_expr(expression):
            if isinstance(expression, OrExpression):
                return or_(raw_expr(expr) for expr in expression.exprs)
            return expression.alchemy_expression

        return [raw_expr(expression) for expression in expressions]

    @staticmethod
    def search_datasets_query(expressions, select_fields=None, with_source_ids=False):
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

        raw_expressions = PostgresDb._alchemify_expressions(expressions)

        select_query = (
            select(
                select_columns
            ).select_from(
                PostgresDb._from_expression(DATASET, expressions, select_fields)
            ).where(
                and_(DATASET.c.archived == None, *raw_expressions)
            )
        )

        return select_query

    def search_datasets(self, expressions, select_fields=None, with_source_ids=False):
        """
        :type with_source_ids: bool
        :type select_fields: tuple[datacube.index.postgres._fields.PgField]
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: dict
        """
        select_query = self.search_datasets_query(expressions, select_fields, with_source_ids)
        results = self._connection.execute(select_query)
        for result in results:
            yield result

    def count_datasets(self, expressions):
        """
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: int
        """

        raw_expressions = self._alchemify_expressions(expressions)

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

    def count_datasets_through_time(self, start, end, period, time_field, expressions):
        """
        :type period: str
        :type start: datetime.datetime
        :type end: datetime.datetime
        :type expressions: tuple[datacube.index.postgres._fields.PgExpression]
        :rtype: list[((datetime.datetime, datetime.datetime), int)]
        """

        raw_expressions = self._alchemify_expressions(expressions)

        start_times = select((
            func.generate_series(start, end, cast(period, INTERVAL)).label('start_time'),
        )).alias('start_times')

        time_range_select = (
            select((
                func.tstzrange(
                    start_times.c.start_time,
                    func.lead(start_times.c.start_time).over()
                ).label('time_period'),
            ))
        ).alias('all_time_ranges')

        # Exclude the trailing (end time to infinite) row. Is there a simpler way?
        time_ranges = (
            select((
                time_range_select,
            )).where(
                ~func.upper_inf(time_range_select.c.time_period)
            )
        ).alias('time_ranges')

        count_query = (
            select(
                (func.count('*'),)
            ).select_from(
                self._from_expression(DATASET, expressions)
            ).where(
                and_(
                    time_field.alchemy_expression.overlaps(time_ranges.c.time_period),
                    DATASET.c.archived == None,
                    *raw_expressions
                )
            )
        )

        results = self._connection.execute(select((
            time_ranges.c.time_period,
            count_query.label('dataset_count')
        )))

        for time_period, dataset_count in results:
            # if not time_period.upper_inf:
            yield Range(time_period.lower, time_period.upper), dataset_count

    @staticmethod
    def _from_expression(source_table, expressions=None, fields=None):
        join_tables = set()
        if expressions:
            join_tables.update(expression.field.required_alchemy_table for expression in expressions)
        if fields:
            join_tables.update(field.required_alchemy_table for field in fields)
        join_tables.discard(source_table)

        table_order_hack = [DATASET_SOURCE, DATASET_LOCATION, DATASET, DATASET_TYPE, METADATA_TYPE]

        from_expression = source_table
        for table in table_order_hack:
            if table in join_tables:
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
        self._setup_dataset_type_fields(type_id, name, metadata_type_id, definition['metadata'])
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

        self._setup_metadata_type_fields(
            type_id, name, record, concurrently=concurrently
        )

    def check_dynamic_fields(self, concurrently=False, rebuild_all=False):
        _LOG.info('Checking dynamic views/indexes. (rebuild all = %s)', rebuild_all)
        for metadata_type in self.get_all_metadata_types():
            self._setup_metadata_type_fields(
                metadata_type['id'],
                metadata_type['name'],
                metadata_type,
                rebuild_all, concurrently
            )

        for dataset_type in self.get_all_dataset_types():
            self._setup_dataset_type_fields(
                dataset_type['id'],
                dataset_type['name'],
                dataset_type['metadata_type_ref'],
                dataset_type['definition']['metadata'],
                rebuild_all,
                concurrently
            )

    def _setup_metadata_type_fields(self, id_, name, record, rebuild_all=False, concurrently=True):
        fields = self.get_dataset_fields(record)
        dataset_filter = and_(DATASET.c.archived == None, DATASET.c.metadata_type_ref == id_)
        _check_dynamic_fields(self._connection, concurrently, dataset_filter,
                              (), fields, name, rebuild_all)

    def _setup_dataset_type_fields(self, id_, name, metadata_type_id, metadata_doc,
                                   rebuild_all=False, concurrently=True):
        fields = self.get_dataset_fields(self.get_metadata_type(metadata_type_id))
        dataset_filter = and_(DATASET.c.archived == None, DATASET.c.dataset_type_ref == id_)
        excluded_field_names = tuple(self._get_active_field_names(metadata_type_id, metadata_doc))

        _check_dynamic_fields(self._connection, concurrently, dataset_filter,
                              excluded_field_names, fields, name, rebuild_all)

    def _get_active_field_names(self, metadata_type_id, metadata_doc):
        fields = self.get_dataset_fields(self.get_metadata_type(metadata_type_id))
        for field in fields.values():
            if hasattr(field, 'extract'):
                try:
                    value = field.extract(metadata_doc)
                    if value is not None:
                        yield field.name
                except (AttributeError, KeyError, ValueError):
                    continue

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

    def drop_user(self, username):
        tables.drop_user(self._engine, username)

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


def contains_all(d_, *keys):
    """
    Does the dictionary have values for all of the given keys?

    >>> contains_all({'a': 4}, 'a')
    True
    >>> contains_all({'a': 4, 'b': 5}, 'a', 'b')
    True
    >>> contains_all({'b': 5}, 'a')
    False
    """
    return all([d_.get(key) for key in keys])


def _check_dynamic_fields(conn, concurrently, dataset_filter, excluded_field_names, fields, name, rebuild_all):
    """
    Check that we have expected indexes and views for the given fields
    """

    # If this type has time/space fields, create composite indexes (as they are often searched together)
    # We will probably move these into product configuration in the future.
    composite_indexes = (
        ('lat', 'lon', 'time'),
        ('time', 'lat', 'lon'),
        ('sat_path', 'sat_row', 'time')
    )

    for field_composite in composite_indexes:
        # If all of the fields are available in this product, we'll create a composite index
        # for them instead of individual indexes.
        if contains_all(fields, *field_composite):
            excluded_field_names += field_composite
            _check_field_index(
                conn,
                [fields.get(f) for f in field_composite],
                name, dataset_filter,
                concurrently=concurrently,
                replace_existing=rebuild_all,
                index_type='gist'
            )

    # Create indexes for the individual fields.
    for field in fields.values():
        if not field.postgres_index_type:
            continue
        _check_field_index(
            conn, [field],
            name, dataset_filter,
            should_exist=(field.name not in excluded_field_names),
            concurrently=concurrently,
            replace_existing=rebuild_all,
        )
    # A view of all fields
    _ensure_view(conn, fields, name, rebuild_all, dataset_filter)


def _check_field_index(conn, fields, name_prefix, filter_expression,
                       should_exist=True, concurrently=False,
                       replace_existing=False, index_type=None):
    """
    Check the status of a given index: add or remove it as needed
    """
    if index_type is None:
        if len(fields) > 1:
            raise ValueError('Must specify index type for composite indexes.')
        index_type = fields[0].postgres_index_type

    field_name = '_'.join([f.name.lower() for f in fields])
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    index_name = 'dix_{prefix}_{field_name}'.format(
        prefix=name_prefix.lower(),
        field_name=field_name
    )
    # Previous naming scheme
    legacy_name = 'dix_field_{prefix}_dataset_{field_name}'.format(
        prefix=name_prefix.lower(),
        field_name=field_name,
    )
    indexed_expressions = [f.alchemy_expression for f in fields]
    index = Index(
        index_name,
        *indexed_expressions,
        postgresql_where=filter_expression,
        postgresql_using=index_type,
        # Don't lock the table (in the future we'll allow indexing new fields...)
        postgresql_concurrently=concurrently
    )
    exists = _pg_exists(conn, tables.schema_qualified(index_name))
    legacy_exists = _pg_exists(conn, tables.schema_qualified(legacy_name))

    # This currently leaves a window of time without indexes: it's primarily intended for development.
    if replace_existing or (not should_exist):
        if exists:
            _LOG.debug('Dropping index: %s (replace=%r)', index_name, replace_existing)
            index.drop(conn)
            exists = False
        if legacy_exists:
            _LOG.debug('Dropping legacy index: %s (replace=%r)', legacy_name, replace_existing)
            Index(legacy_name, *indexed_expressions).drop(conn)
            legacy_exists = False

    if should_exist:
        if not (exists or legacy_exists):
            _LOG.info('Creating index: %s', index_name)
            index.create(conn)
        else:
            _LOG.debug('Index exists: %s  (replace=%r)', index_name, replace_existing)


def _ensure_view(conn, fields, name, replace_existing, where_expression):
    """
    Ensure a view exists for the given fields
    """
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
                    DATASET.join(DATASET_TYPE).join(METADATA_TYPE)
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


class _PostgresDbInTransaction(PostgresDb):
    """
    Identical to PostgresDb class, but all operations
    are run against a single connection in a transaction.

    Call commit() or rollback() to complete the transaction or use a context manager:

        with db.begin() as transaction:
            transaction.insert_dataset(...)

    (Don't share an instance between threads)
    """

    def __init__(self, engine):
        super(_PostgresDbInTransaction, self).__init__(engine)
        self.__connection = engine.connect()
        self.begin()

    @property
    def _connection(self):
        # Override parent so that we use the same connection in transaction
        return self.__connection

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
