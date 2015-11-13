# coding=utf-8
"""
Common methods for index integration tests.
"""
from __future__ import absolute_import

from datacube.index._core_db import Db
from datacube.index.tables._core import METADATA, ensure_db


def connect_db():
    # Defaults for running tests.
    return Db.connect('localhost', 'agdcintegration')


def init_db():
    db = connect_db()
    # Drop and recreate tables so our tests have a clean db.
    METADATA.drop_all(db._engine)
    ensure_db(db._connection, db._engine)
    return db
