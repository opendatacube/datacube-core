# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

# We often have one-arg-per column, so these checks aren't so useful.
# pylint: disable=too-many-arguments,too-many-public-methods

# SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
# pylint: disable=singleton-comparison

"""
Postgres connection and setup
"""
import json
import logging
import os
import re
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Mapping, Optional, Union, Type

from sqlalchemy import event, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL as EngineUrl  # noqa: N811

import datacube
from datacube.index.exceptions import IndexSetupError
from datacube.utils import jsonify_document
from datacube.utils.geometry import CRS

from . import _api
from . import _core
from ._spatial import ensure_spindex, spindexes, spindex_for_crs
from ._schema import SpatialIndex

_LIB_ID = 'odc-' + str(datacube.__version__)

_LOG = logging.getLogger(__name__)

try:
    import pwd

    DEFAULT_DB_USER = pwd.getpwuid(os.geteuid()).pw_name  # type: Optional[str]
except (ImportError, KeyError):
    # No default on Windows and some other systems
    DEFAULT_DB_USER = None
DEFAULT_DB_PORT = 5432
DEFAULT_IAM_AUTH = False
DEFAULT_IAM_TIMEOUT = 600


class PostGisDb(object):
    """
    A thin database access api.

    It exists so that higher level modules are not tied to SQLAlchemy, connections or specifics of database-access.

    (and can be unit tested without any actual databases)

    Thread safe: the only shared state is the (thread-safe) sqlalchemy connection pool.

    But not multiprocess safe once the first connections are made! A connection must not be shared between multiple
    processes. You can call close() before forking if you know no other threads currently hold connections,
    or else use a separate instance of this class in each process.
    """

    driver_name = 'postgis'  # Mostly to support parametised tests

    def __init__(self, engine):
        # We don't recommend using this constructor directly as it may change.
        # Use static methods PostGisDb.create() or PostGisDb.from_config()
        self._engine = engine
        self._spindexes: Optional[Mapping[CRS, Any]] = None

    @classmethod
    def from_config(cls, config, application_name=None, validate_connection=True):
        app_name = cls._expand_app_name(application_name)

        return PostGisDb.create(
            config['db_hostname'],
            config['db_database'],
            config.get('db_username', DEFAULT_DB_USER),
            config.get('db_password', None),
            config.get('db_port', DEFAULT_DB_PORT),
            application_name=app_name,
            validate=validate_connection,
            iam_rds_auth=bool(config.get("db_iam_authentication", DEFAULT_IAM_AUTH)),
            iam_rds_timeout=int(config.get("db_iam_timeout", DEFAULT_IAM_TIMEOUT)),
            pool_timeout=int(config.get('db_connection_timeout', 60)),
            # pass config?
        )

    @classmethod
    def create(cls, hostname, database, username=None, password=None, port=None,
               application_name=None, validate=True,
               iam_rds_auth=False, iam_rds_timeout=600,
               # pass config?
               pool_timeout=60):
        mk_url = getattr(EngineUrl, 'create', EngineUrl)
        engine = cls._create_engine(
            mk_url(
                'postgresql',
                host=hostname, database=database, port=port,
                username=username, password=password,
            ),
            application_name=application_name,
            iam_rds_auth=iam_rds_auth,
            iam_rds_timeout=iam_rds_timeout,
            pool_timeout=pool_timeout)
        if validate:
            if not _core.database_exists(engine):
                raise IndexSetupError('\n\nNo DB schema exists. Have you run init?\n\t{init_command}'.format(
                    init_command='datacube system init'
                ))

            if not _core.schema_is_latest(engine):
                raise IndexSetupError(
                    '\n\nDB schema is out of date. '
                    'An administrator must run init:\n\t{init_command}'.format(
                        init_command='datacube -v system init'
                    ))
        return PostGisDb(engine)

    @staticmethod
    def _create_engine(url, application_name=None, iam_rds_auth=False, iam_rds_timeout=600, pool_timeout=60):
        engine = create_engine(
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
            connect_args={'application_name': application_name},
        )

        if iam_rds_auth:
            from datacube.utils.aws import obtain_new_iam_auth_token
            handle_dynamic_token_authentication(engine, obtain_new_iam_auth_token, timeout=iam_rds_timeout, url=url)

        return engine

    @property
    def url(self) -> EngineUrl:
        return self._engine.url

    @staticmethod
    def get_db_username(config):
        try:
            return config['db_username']
        except KeyError:
            return DEFAULT_DB_USER

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
        >>> PostGisDb._expand_app_name(None) #doctest: +ELLIPSIS
        'odc-...'
        >>> PostGisDb._expand_app_name('') #doctest: +ELLIPSIS
        'odc-...'
        >>> PostGisDb._expand_app_name('cli') #doctest: +ELLIPSIS
        'cli odc-...'
        >>> PostGisDb._expand_app_name('a b.c/d')
        'a-b-c-d odc-...'
        >>> PostGisDb._expand_app_name(5)
        Traceback (most recent call last):
        ...
        TypeError: Application name must be a string
        """
        full_name = _LIB_ID
        if application_name:
            if not isinstance(application_name, str):
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
        is_new = _core.ensure_db(self._engine, with_permissions=with_permissions)
        if not is_new:
            _core.update_schema(self._engine)

        return is_new

    def _refresh_spindexes(self):
        self._spindexes = spindexes(self._engine)

    @property
    def spindexes(self) -> Mapping[CRS, Type[SpatialIndex]]:
        if self._spindexes is None:
            self._refresh_spindexes()
        return self._spindexes

    def create_spatial_index(self, crs: "datacube.utils.geometry.CRS") -> Optional[Type[SpatialIndex]]:
        """
        Create a spatial index across the database, for the named CRS.

        :param crs_str:
        :return:
        """
        spidx = self.spindexes.get(crs)
        if spidx is None:
            spidx = spindex_for_crs(crs)
            if spidx is None:
                _LOG.warning("Could not dynamically model an index for CRS %s", crs._str)
                return None
            ensure_spindex(self._engine, spidx)
            self.spindexes[crs] = spidx
        return spidx

    def spatial_index(self, crs: CRS) -> Optional[Type[SpatialIndex]]:
        return self.spindexes.get(crs)

    def spatial_indexes(self, refresh=False) -> Iterable[CRS]:
        if refresh:
            self._refresh_spindexes()
        return list(self.spindexes.keys())

    @contextmanager
    def _connect(self):
        """
        Borrow a connection from the pool.

        The name connect() is misleading: it will not create a new connection if one is already available in the pool.

        Callers should minimise the amount of time they hold onto their connections. If they're doing anything between
        calls to the DB (such as opening files, or waiting on user input), it's better to return the connection
        to the pool beforehand.

        The connection can raise errors if not following this advice ("server closed the connection unexpectedly"),
        as some servers will aggressively close idle connections (eg. DEA's NCI servers). It also prevents the
        connection from being reused while borrowed.

        Low level context manager, use <index_resource>._db_connection instead
        """
        with self._engine.connect() as connection:
            try:
                yield _api.PostgisDbAPI(self, connection)
            finally:
                connection.close()

    def give_me_a_connection(self):
        return self._engine.connect()

    @classmethod
    def get_dataset_fields(cls, metadata_type_definition):
        return _api.get_dataset_fields(metadata_type_definition)

    def __repr__(self):
        return "PostgresDb<engine={!r}>".format(self._engine)


def handle_dynamic_token_authentication(engine: Engine,
                                        new_token: Callable[..., str],
                                        timeout: Union[float, int] = 600,
                                        **kwargs) -> None:
    last_token = [None]
    last_token_time = [0.0]

    @event.listens_for(engine, "do_connect")
    def override_new_connection(dialect, conn_rec, cargs, cparams):
        # Handle IAM authentication
        # Importing here because the function `clock_gettime` is not available on Windows
        # which shouldn't be a problem, because boto3 auth is mostly used on AWS.
        from time import clock_gettime, CLOCK_REALTIME

        now = clock_gettime(CLOCK_REALTIME)
        if now - last_token_time[0] > timeout:
            last_token[0] = new_token(**kwargs)
            last_token_time[0] = now
        cparams["password"] = last_token[0]


def _to_json(o):
    # Postgres <=9.5 doesn't support NaN and Infinity
    fixedup = jsonify_document(o)
    return json.dumps(fixedup, default=_json_fallback)


def _json_fallback(obj):
    """Fallback json serialiser."""
    raise TypeError("Type not serializable: {}".format(type(obj)))
