# coding=utf-8
"""
Database access.
"""
from __future__ import absolute_import

import datetime
import json
import logging

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

from .tables import ensure_db, DATASET, DATASET_SOURCE

PGCODE_UNIQUE_CONSTRAINT = '23505'

_LOG = logging.getLogger(__name__)


def _connection_string(host=None, database=None):
    """
    >>> _connection_string(database='agdc')
    'postgresql:///agdc'
    >>> _connection_string(host='postgres.dev.lan', database='agdc')
    'postgresql://postgres.dev.lan/agdc'
    """
    return 'postgresql://{host}/{database}'.format(
        host=host or '',
        database=database or ''
    )


class Db(object):
    """
    A very thin database access api.

    It exists so that higher level modules are not tied to SQLAlchemy, connections or specifics of database-access.

    (and can be unit tested without any actual databases)
    """

    def __init__(self, engine, connection):
        self._engine = engine
        self._connection = connection

    @classmethod
    def connect(cls, hostname, database):
        connection_string = _connection_string(hostname, database)
        _engine = create_engine(
            connection_string,
            echo=True,
            # 'AUTOCOMMIT' here means READ-COMMITTED isolation level with autocommit on.
            # When a transaction is needed we will do an explicit begin/commit.
            isolation_level='AUTOCOMMIT'
        )
        _connection = _engine.connect()
        ensure_db(_connection, _engine)
        return Db(_engine, _connection)

    def begin(self):
        """
        Start a transaction.

        Returns a transaction object. Call commit() or rollback() to complete the
        transaction or use a context manager:

            with db.begin() as transaction:
                db.insert_dataset(...)

        :return: Tranasction object
        """
        return self._connection.begin()

    def _execute(self, eow):
        return self._connection.execute(eow)

    def insert_dataset(self, dataset_doc, dataset_id, path, product_type, ignore_duplicates=True):
        try:
            self._execute(
                DATASET.insert().values(
                    id=dataset_id,
                    type=product_type,
                    # TODO: Does a single path make sense? Or a separate 'locations' table?
                    metadata_path=str(path) if path else None,
                    # We convert to JSON ourselves so we can specify our own serialiser (for date conversion etc)
                    metadata=json.dumps(dataset_doc, default=_json_serialiser)
                )
            )
            return True
        except IntegrityError as e:
            # Unique constraint error: either UUID or path name.
            # We are often inserting datasets
            if (e.orig.pgcode == PGCODE_UNIQUE_CONSTRAINT) and ignore_duplicates:
                if ignore_duplicates:
                    _LOG.info('Duplicate dataset, not inserting: %s @ %s', dataset_id, path)
                    return False
            raise

    def insert_dataset_source(self, classifier, dataset_id, source_dataset_id):
        self._execute(
            DATASET_SOURCE.insert().values(
                classifier=classifier,
                dataset_ref=dataset_id,
                source_dataset_ref=source_dataset_id
            )
        )


def _json_serialiser(obj):
    """Fallback json serialiser."""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type not serializable: {}".format(type(obj)))
