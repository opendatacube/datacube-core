# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
from contextlib import contextmanager

from datacube.drivers.postgres import PostgresDb, PostgresDbAPI
from datacube.index.postgres._transaction import PostgresTransaction
from datacube.index.postgres._datasets import DatasetResource  # type: ignore
from datacube.index.postgres._metadata_types import MetadataTypeResource
from datacube.index.postgres._products import ProductResource
from datacube.index.postgres._users import UserResource
from datacube.index.abstract import AbstractIndex, AbstractIndexDriver, default_metadata_type_docs, AbstractTransaction
from datacube.model import MetadataType
from datacube.utils.geometry import CRS

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
    :ivar datacube.index._products.ProductResource products: store and retrieve :class:`datacube.model.DatasetType`\
    (should really be called Product)
    :ivar datacube.index._metadata_types.MetadataTypeResource metadata_types: store and retrieve \
    :class:`datacube.model.MetadataType`
    :ivar UserResource users: user management

    :type users: datacube.index._users.UserResource
    :type datasets: datacube.index._datasets.DatasetResource
    :type products: datacube.index._products.ProductResource
    :type metadata_types: datacube.index._metadata_types.MetadataTypeResource
    """

    supports_transactions = True

    def __init__(self, db: PostgresDb) -> None:
        self._db = db

        self._users = UserResource(db, self)
        self._metadata_types = MetadataTypeResource(db, self)
        self._products = ProductResource(db, self)
        self._datasets = DatasetResource(db, self)

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
    def datasets(self) -> DatasetResource:
        return self._datasets

    @property
    def url(self) -> str:
        return str(self._db.url)

    @classmethod
    def from_config(cls, config, application_name=None, validate_connection=True):
        db = PostgresDb.from_config(config, application_name=application_name,
                                    validate_connection=validate_connection)
        return cls(db)

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
        but wont be using it for a while.

        (Connections are normally closed automatically when this object is deleted: ie. no references exist)
        """
        self._db.close()

    @property
    def index_id(self) -> str:
        return f"legacy_{self.url}"

    def transaction(self) -> AbstractTransaction:
        return PostgresTransaction(self._db, self.index_id)

    def create_spatial_index(self, crs: CRS) -> None:
        _LOG.warning("postgres driver does not support spatio-temporal indexes")

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
    aliases = ['postgres']

    @staticmethod
    def connect_to_index(config, application_name=None, validate_connection=True):
        return Index.from_config(config, application_name, validate_connection)

    @staticmethod
    def metadata_type_from_doc(definition: dict) -> MetadataType:
        """
        :param definition:
        """
        MetadataType.validate(definition)  # type: ignore
        return MetadataType(definition,
                            dataset_search_fields=Index.get_dataset_fields(definition))


def index_driver_init():
    return DefaultIndexDriver()
