# coding=utf-8
"""
Common methods for index integration tests.
"""
from __future__ import absolute_import

import os

from datacube.config import SystemConfig
from datacube.index.postgres import PostgresDb
from datacube.index.postgres.tables._core import METADATA, ensure_db


def connect_db():
    default = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'agdcintegration.conf')
    user = os.path.expanduser('~/.datacube_integration.conf')
    config = SystemConfig.find([default, user])
    return PostgresDb.connect(
            config.db_hostname,
            config.db_database,
            config.db_username,
            config.db_port)


def init_db():
    db = connect_db()
    # Drop and recreate tables so our tests have a clean db.
    METADATA.drop_all(db._engine)
    ensure_db(db._connection, db._engine)
    return db
