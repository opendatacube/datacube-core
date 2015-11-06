# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from datacube.config import UserConfig
from tests import util


def test_find_defaults():
    config = UserConfig.find(paths=[])
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

    config_paths = [str(files.joinpath('base.conf'))]

    # One config file
    config = UserConfig.find(paths=config_paths)
    assert config.db_hostname == 'fakehost.test.lan'
    # Not set: uses default
    assert config.db_database == 'datacube'

    # Now two config files, with the latter overriding earlier options.
    config_paths.append(str(files.joinpath('override.conf')))

    config = UserConfig.find(paths=config_paths)
    assert config.db_hostname == 'overridden.test.lan'
    assert config.db_database == 'overridden_db'
