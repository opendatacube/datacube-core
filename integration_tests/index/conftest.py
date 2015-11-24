# coding=utf-8
"""
Common methods for index integration tests.
"""
from __future__ import absolute_import

import os

import pytest

from datacube.config import LocalConfig
from datacube.index._api import Index
from datacube.index.postgres import PostgresDb
from datacube.index.postgres.tables._core import METADATA, ensure_db


@pytest.fixture
def local_config():
    default = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'agdcintegration.conf')
    user = os.path.expanduser('~/.datacube_integration.conf')
    config = LocalConfig.find([default, user])
    return config


@pytest.fixture
def db(local_config):
    db = PostgresDb.from_config(local_config)
    # Drop and recreate tables so our tests have a clean db.
    METADATA.drop_all(db._engine)
    ensure_db(db._connection, db._engine)
    return db


@pytest.fixture
def index(db, local_config):
    return Index(db, local_config)
