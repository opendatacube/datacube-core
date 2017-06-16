# coding=utf-8

# We often have one-arg-per column, so these checks aren't so useful.
# pylint: disable=too-many-arguments,too-many-public-methods

# SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
# pylint: disable=singleton-comparison

"""
Postgres connection and setup
"""
from __future__ import absolute_import

import json
import logging
import re

from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL as EngineUrl

import datacube
from datacube.compat import string_types
from datacube.config import LocalConfig
from datacube.utils import jsonify_document
from . import tables, _api

_LIB_ID = 'agdc-' + str(datacube.__version__)

_LOG = logging.getLogger(__name__)


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

    def __getstate__(self):
        _LOG.warning("Serializing PostgresDb engine %s", self.url)
        return {'url': self.url}

    def __setstate__(self, state):
        self.__init__(self._create_engine(state['url']))

    @property
    def url(self):
        return self._engine.url

    @staticmethod
    def _create_engine(url, application_name=None, pool_timeout=60):
        return create_engine(
            url,
            echo=False,
            echo_pool=False,

            # 'AUTOCOMMIT' here means READ-COMMITTED isolation level with autocommit on.
            # When a transaction is needed we will do an explicit begin/commit.
            isolation_level='AUTOCOMMIT',

            json_serializer=_to_json,
            # If a connection is idle for this many seconds, SQLAlchemy will renew it rather
            # than assuming it's still open. Allows servers to close idle connections without clients
            # getting errors.
            pool_recycle=pool_timeout,
            connect_args={'application_name': application_name}
        )

    @classmethod
    def create(cls, hostname, database, username=None, password=None, port=None,
               application_name=None, validate=True, pool_timeout=60):
        engine = cls._create_engine(
            EngineUrl(
                'postgresql',
                host=hostname, database=database, port=port,
                username=username, password=password,
            ),
            application_name=application_name,
            pool_timeout=pool_timeout)
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
            validate=validate_connection,
            pool_timeout=config.db_connection_timeout
        )

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

    def connect(self):
        """
        Borrow a connection from the pool.
        """
        return _PostgresDbConnection(self._engine)

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

    def get_dataset_fields(self, search_fields_definition):
        return _api.get_dataset_fields(search_fields_definition)

    def __repr__(self):
        return "PostgresDb<engine={!r}>".format(self._engine)


class _PostgresDbConnection(object):
    def __init__(self, engine):
        self._engine = engine
        self._connection = None

    def __enter__(self):
        self._connection = self._engine.connect()
        return _api.PostgresDbAPI(self._connection)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()
        self._connection = None


class _PostgresDbInTransaction(object):
    """
    Identical to PostgresDb class, but all operations
    are run against a single connection in a transaction.

    Call commit() or rollback() to complete the transaction or use a context manager:

        with db.begin() as transaction:
            transaction.insert_dataset(...)

    (Don't share an instance between threads)
    """

    def __init__(self, engine):
        self._engine = engine
        self._connection = None

    def __enter__(self):
        self._connection = self._engine.connect()
        self._connection.execute(text('BEGIN'))
        return _api.PostgresDbAPI(self._connection)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._connection.execute(text('ROLLBACK'))
        else:
            self._connection.execute(text('COMMIT'))
        self._connection.close()
        self._connection = None


def _to_json(o):
    # Postgres <=9.5 doesn't support NaN and Infinity
    fixedup = jsonify_document(o)
    return json.dumps(fixedup, default=_json_fallback)


def _json_fallback(obj):
    """Fallback json serialiser."""
    raise TypeError("Type not serializable: {}".format(type(obj)))
