# coding=utf-8
"""
Database access.
"""
from __future__ import absolute_import

import datetime
import json
import logging

from sqlalchemy import create_engine, select, text, bindparam, exists
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
        return _BegunTransaction(self._connection)

    def insert_dataset(self, dataset_doc, dataset_id, path, product_type):
        """
        Insert dataset if not already indexed.
        :type dataset_doc: dict
        :type dataset_id: str or uuid.UUID
        :type path: pathlib.Path
        :type product_type: str
        :return: whether it was inserted
        :rtype: bool
        """
        try:
            ret = self._connection.execute(
                # Insert if not exists.
                #     (there's still a tiny chance of a race condition: It will throw an integrity error if another
                #      connection inserts the same dataset in the time between the subquery and the main query.
                #      This is ok for our purposes.)
                DATASET.insert().from_select(
                    ['id', 'type', 'metadata_path', 'metadata'],
                    select([
                        bindparam('id'), bindparam('type'), bindparam('metadata_path'), bindparam('metadata')
                    ]).where(~exists(select([DATASET.c.id]).where(DATASET.c.id == bindparam('id'))))
                ),
                id=dataset_id,
                type=product_type,
                # TODO: Does a single path make sense? Or a separate 'locations' table?
                metadata_path=str(path) if path else None,
                # We convert to JSON ourselves so we can specify our own serialiser (for date conversion etc)
                metadata=json.dumps(dataset_doc, default=_json_serialiser)
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
        self._connection.execute(
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
