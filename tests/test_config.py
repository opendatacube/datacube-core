# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import configparser
from textwrap import dedent

from datacube.config import LocalConfig
from datacube.testutils import write_files


def test_find_config():
    files = write_files({
        'base.conf': dedent("""\
            [datacube]
            db_hostname: fakehost.test.lan
        """),
        'override.conf': dedent("""\
            [datacube]
            db_hostname: overridden.test.lan
            db_database: overridden_db
        """)
    })

    # One config file
    config = LocalConfig.find(paths=[str(files.joinpath('base.conf'))])
    assert config['db_hostname'] == 'fakehost.test.lan'
    # Not set: uses default
    assert config['db_database'] == 'datacube'

    # Now two config files, with the latter overriding earlier options.
    config = LocalConfig.find(paths=[str(files.joinpath('base.conf')),
                                     str(files.joinpath('override.conf'))])
    assert config['db_hostname'] == 'overridden.test.lan'
    assert config['db_database'] == 'overridden_db'


config_sample = """
[default]
db_database: datacube

# A blank host will use a local socket. Specify a hostname (such as localhost) to use TCP.
db_hostname:

# Credentials are optional: you might have other Postgres authentication configured.
# The default username is the current user id
# db_username:
# A blank password will fall back to default postgres driver authentication, such as reading your ~/.pgpass file.
# db_password:
index_driver: pg


## Development environment ##
[dev]
# These fields are all the defaults, so they could be omitted, but are here for reference

db_database: datacube

# A blank host will use a local socket. Specify a hostname (such as localhost) to use TCP.
db_hostname:

# Credentials are optional: you might have other Postgres authentication configured.
# The default username is the current user id
# db_username:
# A blank password will fall back to default postgres driver authentication, such as reading your ~/.pgpass file.
# db_password:

## Staging environment ##
[staging]
db_hostname: staging.dea.ga.gov.au

[s3_test]
db_hostname: staging.dea.ga.gov.au
index_driver: s3aio"""


def test_using_configparser(tmpdir):
    sample_config = tmpdir.join('datacube.conf')
    sample_config.write(config_sample)

    config = configparser.ConfigParser()
    config.read(str(sample_config))


def test_empty_configfile(tmpdir):
    default_only = """[default]"""
    sample_file = tmpdir.join('datacube.conf')
    sample_file.write(default_only)
    config = configparser.ConfigParser()
    config.read(str(sample_file))
