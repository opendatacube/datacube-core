# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
from contextlib import contextmanager
from typing import Iterable, Type

from deprecat import deprecat
from datacube.cfg.opt import ODCOptionHandler, config_options_for_psql_driver
from datacube.cfg.api import ODCEnvironment
from datacube.drivers.postgres import PostgresDb, PostgresDbAPI
from datacube.index.postgres._transaction import PostgresTransaction
from datacube.index.postgres._datasets import DatasetResource  # type: ignore
from datacube.index.postgres._lineage import LineageResource
from datacube.index.postgres._metadata_types import MetadataTypeResource
from datacube.index.postgres._products import ProductResource
from datacube.index.postgres._users import UserResource
from datacube.index.abstract import AbstractIndex, AbstractIndexDriver, AbstractTransaction, \
    default_metadata_type_docs
from datacube.model import MetadataType
from datacube.migration import ODC2DeprecationWarning

_LOG = logging.getLogger(__name__)


class Index(AbstractIndex):
    """
    Access to the datacube index.

    DON'T INITIALISE THIS DIRECTLY (it will break in the future). Use `datacube.index.index_connect()` or
    access property ``.index`` on your existing :class:`datacube.api.core.Datacube`.

    These are thread safe. But not multiprocess safe once a connection is made (db connections cannot be shared
    between processes) You can close idle connections before forking by calling close(), provided you know no
    other connections are active. Or else use a separate instance of this class in each process.

    :ivar datacube.index._datasets.DatasetResource datasets: store and retrieve :class:`datacube.model.Dataset`
    :ivar datacube.index._products.ProductResource products: store and retrieve :class:`datacube.model.Product`\
    (should really be called Product)
    :ivar datacube.index._metadata_types.MetadataTypeResource metadata_types: store and retrieve \
    :class:`datacube.model.MetadataType`
    :ivar UserResource users: user management

    :type users: datacube.index._users.UserResource
    :type datasets: datacube.index._datasets.DatasetResource
    :type products: datacube.index._products.ProductResource
    :type metadata_types: datacube.index._metadata_types.MetadataTypeResource
    """
    #   Metadata type support flags
    supports_legacy = True
    supports_eo3 = True
    supports_nongeo = True

    #   Database/storage feature support flags
    supports_write = True
    supports_persistance = True
    supports_transactions = True

    #   User managment support flags
    supports_users = True

    #   Lineage support flags
    supports_lineage = True

    def __init__(self, db: PostgresDb, env: ODCEnvironment) -> None:
        self._db = db
        self._env = env
        self._users = UserResource(db, self)
        self._metadata_types = MetadataTypeResource(db, self)
        self._products = ProductResource(db, self)
        self._lineage = LineageResource(db, self)
        self._datasets = DatasetResource(db, self)

    @property
    def environment(self) -> ODCEnvironment:
        return self._env

    @property
    def users(self) -> UserResource:
        return self._users

    @property
    def metadata_types(self) -> MetadataTypeResource:
        return self._metadata_types

    @property
    def products(self) -> ProductResource:
        return self._products

    @property
    def lineage(self) -> LineageResource:
        return self._lineage

    @property
    def datasets(self) -> DatasetResource:
        return self._datasets

    @property
    def url(self) -> str:
        return str(self._db.url)

    @classmethod
    def from_config(cls,
                    config_env: ODCEnvironment,
                    application_name: str | None = None,
                    validate_connection: bool = True):
        db = PostgresDb.from_config(config_env, application_name=application_name,
                                    validate_connection=validate_connection)
        return cls(db, config_env)

    @classmethod
    def get_dataset_fields(cls, doc):
        return PostgresDb.get_dataset_fields(doc)

    def init_db(self, with_default_types=True, with_permissions=True):
        is_new = self._db.init(with_permissions=with_permissions)

        if is_new and with_default_types:
            _LOG.info('Adding default metadata types.')
            for doc in default_metadata_type_docs():
                self.metadata_types.add(self.metadata_types.from_doc(doc), allow_table_lock=True)

        return is_new

    def close(self):
        """
        Close any idle connections database connections.

        This is good practice if you are keeping the Index instance in scope
        but won't be using it for a while.

        (Connections are normally closed automatically when this object is deleted: ie. no references exist)
        """
        self._db.close()

    @property
    def index_id(self) -> str:
        return f"legacy_{self.url}"

    def transaction(self) -> AbstractTransaction:
        return PostgresTransaction(self._db, self.index_id)

    def __repr__(self):
        return "Index<db={!r}>".format(self._db)

    @contextmanager
    def _active_connection(self, transaction: bool = False) -> PostgresDbAPI:
        """
        Context manager representing a database connection.

        If there is an active transaction for this index in the current thread, the connection object from that
        transaction is returned, with the active transaction remaining in control of commit and rollback.

        If there is no active transaction and the transaction argument is True, a new transactionised connection
        is returned, with this context manager handling commit and rollback.

        If there is no active transaction and the transaction argument is False (the default), a new connection
        is returned with autocommit semantics.

        Note that autocommit behaviour is NOT available if there is an active transaction for the index
        and the active thread.

        :param transaction: Use a transaction if one is not already active for the thread.
        :return: A PostgresDbAPI object, with the specified transaction semantics.
        """
        trans = self.thread_transaction()
        closing = False
        if trans is not None:
            # Use active transaction
            yield trans._connection
        elif transaction:
            closing = True
            with self._db._connect() as conn:
                conn.begin()
                try:
                    yield conn
                    conn.commit()
                except Exception:  # pylint: disable=broad-except
                    conn.rollback()
                    raise
        else:
            closing = True
            # Autocommit behaviour:
            with self._db._connect() as conn:
                yield conn


class DefaultIndexDriver(AbstractIndexDriver):
    aliases = ['postgres', 'legacy']

    @classmethod
    def index_class(cls) -> Type[AbstractIndex]:
        return Index

    @staticmethod
    @deprecat(
        reason="The 'metadata_type_from_doc' static method has been deprecated. "
               "Please use the 'index.metadata_type.from_doc()' instead.",
        version='1.9.0',
        category=ODC2DeprecationWarning)
    def metadata_type_from_doc(definition: dict) -> MetadataType:
        """
        :param definition:
        """
        MetadataType.validate(definition)  # type: ignore
        return MetadataType(definition,
                            dataset_search_fields=Index.get_dataset_fields(definition))

    @staticmethod
    def get_config_option_handlers(env: ODCEnvironment) -> Iterable[ODCOptionHandler]:
        return config_options_for_psql_driver(env)


def index_driver_init():
    return DefaultIndexDriver()
