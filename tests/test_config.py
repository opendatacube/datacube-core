# coding=utf-8
"""
Module
"""

import configparser
from textwrap import dedent

from datacube.config import LocalConfig, parse_connect_url, parse_env_params
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


def test_parse_db_url():

    assert parse_connect_url('postgresql:///db') == dict(database='db', hostname='')
    assert parse_connect_url('postgresql://some.tld/db') == dict(database='db', hostname='some.tld')
    assert parse_connect_url('postgresql://some.tld:3344/db') == dict(
        database='db',
        hostname='some.tld',
        port='3344')
    assert parse_connect_url('postgresql://user@some.tld:3344/db') == dict(
        username='user',
        database='db',
        hostname='some.tld',
        port='3344')
    assert parse_connect_url('postgresql://user:pass@some.tld:3344/db') == dict(
        password='pass',
        username='user',
        database='db',
        hostname='some.tld',
        port='3344')

    # check urlencode is reversed for password field
    assert parse_connect_url('postgresql://user:pass%40@some.tld:3344/db') == dict(
        password='pass@',
        username='user',
        database='db',
        hostname='some.tld',
        port='3344')


def test_parse_env(monkeypatch):
    def set_env(**kw):
        for e in ('DATACUBE_DB_URL',
                  'DB_HOSTNAME',
                  'DB_PORT',
                  'DB_USERNAME',
                  'DB_PASSWORD'):
            monkeypatch.delenv(e, raising=False)
        for e, v in kw.items():
            monkeypatch.setenv(e, v)

    def check_env(**kw):
        set_env(**kw)
        return parse_env_params()

    assert check_env() == {}
    assert check_env(DATACUBE_DB_URL='postgresql:///db') == dict(
        hostname='',
        database='db'
    )
    assert check_env(DATACUBE_DB_URL='postgresql://uu:%20pass%40@host.tld:3344/db') == dict(
        username='uu',
        password=' pass@',
        hostname='host.tld',
        port='3344',
        database='db'
    )
    assert check_env(DB_DATABASE='db') == dict(
        database='db'
    )
    assert check_env(DB_DATABASE='db', DB_HOSTNAME='host.tld') == dict(
        database='db',
        hostname='host.tld'
    )
    assert check_env(DB_DATABASE='db',
                     DB_HOSTNAME='host.tld',
                     DB_USERNAME='user',
                     DB_PASSWORD='pass@') == dict(
                         database='db',
                         hostname='host.tld',
                         username='user',
                         password='pass@')
