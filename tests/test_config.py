# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from datacube.config import LocalConfig
from tests import util


def test_find_defaults():
    config = LocalConfig.find(paths=[])
    assert config.db_hostname == ''
    assert config.db_database == 'datacube'


def test_find_config():
    files = util.write_files({
        'base.conf': """[datacube]
db_hostname: fakehost.test.lan
        """,
        'override.conf': """[datacube]
db_hostname: overridden.test.lan
db_database: overridden_db
        """
    })

    # One config file
    config = LocalConfig.find(paths=[str(files.joinpath('base.conf'))])
    assert config.db_hostname == 'fakehost.test.lan'
    # Not set: uses default
    assert config.db_database == 'datacube'

    # Now two config files, with the latter overriding earlier options.
    config = LocalConfig.find(paths=[str(files.joinpath('base.conf')),
                                     str(files.joinpath('override.conf'))])
    assert config.db_hostname == 'overridden.test.lan'
    assert config.db_database == 'overridden_db'
