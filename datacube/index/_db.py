# coding=utf-8
"""
Database access.
"""
from __future__ import absolute_import

import datetime
import json

from . import tables


class Db(object):
    """
    A very thin database access api.

    It exists so that higher level modules are not tied to SQLAlchemy, connections or specifics of database-access.

    (and can be unit tested without any actual databases)
    """
    def __init__(self, engine):
        self._engine = engine
        self._connection = None

    def _execute(self, eow):
        if not self._connection:
            self._connection = self._engine.connect()

            tables.ensure_db(self._connection, self._engine)

        self._connection.execute(eow)

    def insert_dataset(self, dataset_doc, dataset_id, path, product_type):
        self._execute(
            tables.DATASET.insert().values(
                id=dataset_id,
                type=product_type,
                # TODO: Does a single path make sense? Or a separate 'locations' table?
                metadata_path=str(path),
                # We convert to JSON ourselves so we can specify our own serialiser (for date conversion etc)
                metadata=json.dumps(dataset_doc, default=_json_serialiser)
            )
        )

    def insert_dataset_source(self, classifier, dataset_id, source_dataset_id):
        self._execute(
            tables.DATASET_SOURCE.insert().values(
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
