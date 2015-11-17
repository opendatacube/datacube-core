# coding=utf-8
"""
Lower-level database access.
"""
from __future__ import absolute_import

import datetime
import json
import logging
import numpy

from sqlalchemy import create_engine, select, text, bindparam, exists, and_
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.url import URL as engine_url
from sqlalchemy.exc import IntegrityError

from . import tables, _fields
from .tables import DATASET, DATASET_SOURCE, STORAGE_TYPE, \
    STORAGE_MAPPING, STORAGE_UNIT, DATASET_STORAGE

PGCODE_UNIQUE_CONSTRAINT = '23505'

_LOG = logging.getLogger(__name__)


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
    def connect(cls, hostname, database, username=None, port=None):
        _engine = create_engine(
            engine_url('postgresql', host=hostname, database=database, username=username, port=port),
            echo=False,
            # 'AUTOCOMMIT' here means READ-COMMITTED isolation level with autocommit on.
            # When a transaction is needed we will do an explicit begin/commit.
            isolation_level='AUTOCOMMIT',

            json_serializer=_to_json,
            # json_deserializer=my_deserialize_fn
        )
        _connection = _engine.connect()
        is_new = tables.ensure_db(_connection, _engine)

        # Index any important document fields.
        if is_new:
            for field in _fields.parse_doc(tables.DATASET_QUERY_FIELDS['eo']).values():
                _LOG.debug('Creating index: %s', field.name)
                field.alchemy_index.create(_engine)

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
                        bindparam('id'), bindparam('type'), bindparam('metadata_path'),
                        bindparam('metadata', type_=JSONB)
                    ]).where(~exists(select([DATASET.c.id]).where(DATASET.c.id == bindparam('id'))))
                ),
                id=dataset_id,
                type=product_type,
                # TODO: Does a single path make sense? Or a separate 'locations' table?
                metadata_path=str(path) if path else None,
                metadata=dataset_doc
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
            DATASET_SOURCE.insert(),
            classifier=classifier,
            dataset_ref=dataset_id,
            source_dataset_ref=source_dataset_id
        )

    def ensure_storage_type(self, driver, name, descriptor):
        # TODO: Update them if they already exist. This will do for now.
        self._connection.execute(
            STORAGE_TYPE.insert(),
            driver=driver,
            name=name,
            descriptor=descriptor
        )

    def get_storage_type(self, storage_type_id):
        return self._connection.execute(
            STORAGE_TYPE.select().where(STORAGE_TYPE.c.id == storage_type_id)
        ).fetchone()

    def get_storage_mapping(self, storage_mapping_id):
        return self._connection.execute(
            STORAGE_MAPPING.select().where(STORAGE_MAPPING.c.id == storage_mapping_id)
        ).fetchone()

    def get_storage_mappings(self, dataset_metadata):
        """
        Find any storage mappings that match the given dataset.

        :type dataset_metadata: dict
        :rtype: dict
        """
        # Find any storage mappings whose 'dataset_metadata' document is a subset of the metadata.
        return self._connection.execute(
            STORAGE_MAPPING.select().where(
                STORAGE_MAPPING.c.dataset_metadata.contained_by(dataset_metadata)
            )
        ).fetchall()

    def ensure_storage_mapping(self, driver, storage_type_name, name, location_name, location_offset,
                               dataset_metadata, data_measurements_key, measurements):
        self._connection.execute(
            STORAGE_MAPPING.insert().values(
                storage_type_ref=select([STORAGE_TYPE.c.id]).where(
                    and_(STORAGE_TYPE.c.driver == driver,
                         STORAGE_TYPE.c.name == storage_type_name)
                ),
                name=name,
                dataset_metadata=dataset_metadata,
                dataset_measurements_key=data_measurements_key,
                measurements=measurements,
                location_name=location_name,
                location_offset=location_offset,
            )
        )

    def add_storage_unit(self, path, dataset_ids, descriptor, storage_mapping_id):
        unit_id = self._connection.execute(
            STORAGE_UNIT.insert().returning(STORAGE_UNIT.c.id),
            storage_mapping_ref=storage_mapping_id,
            descriptor=descriptor,
            path=path
        ).scalar()

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


def _to_json(o):
    return json.dumps(o, default=_json_fallback)


def _json_fallback(obj):
    """Fallback json serialiser."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, numpy.dtype):
        return obj.name
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
