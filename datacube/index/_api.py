# coding=utf-8
"""
Access methods for indexing datasets & products.
"""
from __future__ import absolute_import

import logging
from pathlib import Path

import datacube.utils
from datacube.config import LocalConfig
from ._datasets import DatasetResource, ProductResource, MetadataTypeResource
from .postgres import PostgresDb

_LOG = logging.getLogger(__name__)

_DEFAULT_METADATA_TYPES_PATH = Path(__file__).parent.joinpath('default-metadata-types.yaml')


class Index(object):
    """
    Access to the datacube index.

    Thread safe. But not multiprocess safe once a connection is made (db connections cannot be shared between processes)
    You can close idle connections before forking by calling close(), provided you know no other connections are active.
    Or else use a separate instance of this class in each process.

    :ivar datacube.index._datasets.DatasetResource datasets: store and retrieve :class:`datacube.model.Dataset`
    :ivar datacube.index._datasets.DatasetTypeResource products: store and retrieve :class:`datacube.model.DatasetType`\
    (should really be called Product)
    :ivar datacube.index._datasets.MetadataTypeResource metadata_types: store and retrieve \
    :class:`datacube.model.MetadataType`
    :ivar UserResource users: user management

    :type users: UserResource
    :type datasets: datacube.index._datasets.DatasetResource
    :type products: datacube.index._datasets.DatasetTypeResource
    :type metadata_types: datacube.index._datasets.MetadataTypeResource
    """
    def __init__(self, driver_manager, db):
        # type: (PostgresDb) -> None
        self._db = db

        self.users = UserResource(db)
        self.metadata_types = MetadataTypeResource(db)
        self.products = ProductResource(db, self.metadata_types)
        self.datasets = DatasetResource(driver_manager, db, self.products)

    @property
    def url(self):
        return self._db.url

    def init_db(self, with_default_types=True, with_permissions=True, with_s3_tables=False):
        is_new = self._db.init(with_permissions=with_permissions, with_s3_tables=with_s3_tables)

        if is_new and with_default_types:
            _LOG.info('Adding default metadata types.')
            for _, doc in datacube.utils.read_documents(_DEFAULT_METADATA_TYPES_PATH):
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

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

    def __repr__(self):
        return "Index<db={!r}>".format(self._db)


class UserResource(object):
    def __init__(self, db):
        # type: (PostgresDb) -> None
        self._db = db

    def grant_role(self, role, *usernames):
        """
        Grant a role to users
        """
        with self._db.connect() as connection:
            connection.grant_role(role, usernames)

    def create_user(self, username, password, role, description=None):
        """
        Create a new user.
        """
        with self._db.connect() as connection:
            connection.create_user(username, password, role, description=description)

    def delete_user(self, *usernames):
        """
        Delete a user
        """
        with self._db.connect() as connection:
            connection.drop_users(usernames)

    def list_users(self):
        """
        :return: list of (role, user, description)
        :rtype: list[(str, str, str)]
        """
        with self._db.connect() as connection:
            for role, user, description in connection.list_users():
                yield role, user, description
