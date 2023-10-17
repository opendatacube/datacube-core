# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest


def test_smells_like_ini():
    from datacube.cfg.utils import smells_like_ini
    assert smells_like_ini("""[an_ini_file]
key: value
other-key: 12

""")
    assert smells_like_ini("""
; This is an ini comment
[an_ini_file]
key: value
other-key: 12

""")
    assert not smells_like_ini("""# This is a YAML comment

spagoots:
  four: score
  and: 7
  years: ago
""")
    assert not smells_like_ini("""{
"spagoots":{
  "four": "score",
  "and": 7,
  "years": "ago",
  # no comments in json!
  }
}
""")
    assert smells_like_ini("""[
    Looks ini-ish but
    :::is:::
    GIBBER-ish ..;';'@#$%^@#$%^#$^&$
\n  \n    \n \n  \t  \t  \n   \n
aasdfer\\faw043[]]][""")

    # Pure white space
    assert not smells_like_ini("   \n  \n    \n \n  \t  \t  \n   \n")


@pytest.fixture
def simple_valid_ini():
    return """[foo]
bar: bell
bat: 7
baz: luhrmann
[goo]
bar: beque
bat: 2
baz: mckenzie
"""


@pytest.fixture
def simple_valid_yaml():
    return """foo:
    bar: bell
    bat: 7
    baz: luhrmann
goo:
    bar: beque
    bat: 2
    baz: mckenzie
"""


def test_parse_text(simple_valid_ini, simple_valid_yaml):
    from datacube.cfg.api import ConfigException
    from datacube.cfg.cfg import parse_text
    from datacube.cfg.cfg import CfgFormat
    ini = parse_text(simple_valid_ini)
    yaml = parse_text(simple_valid_yaml)
    assert ini["foo"]["bar"] == yaml["foo"]["bar"]
    assert ini["goo"]["baz"] == yaml["goo"]["baz"]
    assert int(ini["foo"]["bat"]) == int(yaml["foo"]["bat"])

    with pytest.raises(ConfigException) as e:
        ini_as_yaml = parse_text(simple_valid_ini, fmt=CfgFormat.YAML)

    with pytest.raises(ConfigException) as e:
        yaml_as_ini = parse_text(simple_valid_yaml, fmt=CfgFormat.INI)


@pytest.fixture
def single_env_config():
    return """# Simple single environment config
experimental:
   index_driver: postgis
   db_url: postgresql://foo:bar@server.subdomain.domain/mytestdb
   db_iam_authentication: yes
"""


@pytest.fixture
def simple_config():
    return """# Simple but thorough test config
default:
   alias: legacy
legacy:
   index_driver: default
   db_username: foo
   db_password: bar
   db_hostname: server.subdomain.domain
   db_port: 5433
   db_database: mytestdb
   db_connection_timeout: 20
experimental:
   index_driver: postgis
   db_url: postgresql://foo:bar@server.subdomain.domain/mytestdb
   db_iam_authentication: yes
postgis:
   alias: experimental
memory:
   index_driver: memory
   db_url: '@nota?valid:url//foo&bar%%%'
exp2:
   index_driver: postgis
   db_url: postgresql://foo:bar@server.subdomain.domain/mytestdb
   db_database: not_read
   db_port: ignored
   db_iam_authentication: yes
   db_iam_timeout: 300
"""


def test_single_env(single_env_config):
    from datacube.cfg.api import ODCConfig
    cfg = ODCConfig(text=single_env_config)

    assert cfg['experimental'].index_driver == "postgis"
    assert cfg['experimental'].db_url == "postgresql://foo:bar@server.subdomain.domain/mytestdb"
    with pytest.raises(AttributeError):
        assert cfg['experimental'].db_username
    assert cfg['experimental']['db_iam_authentication']
    assert cfg['experimental'].db_iam_timeout == 600
    assert cfg['experimental']['db_connection_timeout'] == 60


def test_aliases(simple_config):
    from datacube.cfg.api import ODCConfig
    cfg = ODCConfig(text=simple_config)
    assert cfg['default']._name == 'legacy'
    assert cfg['legacy']._name == 'legacy'
    assert cfg['postgis']._name == 'experimental'
    assert cfg['experimental']._name == 'experimental'
    assert cfg['memory']._name == 'memory'
    assert cfg['exp2']._name == 'exp2'
    assert cfg['dynamic']._name == 'dynamic'
    assert cfg[None]._name == 'legacy'


def test_options(simple_config):
    from datacube.cfg.api import ODCConfig
    cfg = ODCConfig(text=simple_config)

    assert cfg['default']['index_driver'] == 'default'
    assert cfg['default'].db_username == 'foo'
    assert not cfg['default']['db_iam_authentication']
    with pytest.raises(KeyError):
        assert cfg['default']["db_iam_timeout"]

    assert cfg['exp2'].db_url == "postgresql://foo:bar@server.subdomain.domain/mytestdb"
    with pytest.raises(AttributeError):
        assert cfg['exp2'].db_username
    assert cfg['exp2']['db_iam_authentication']
    assert cfg['exp2'].db_iam_timeout == 300
    assert cfg['exp2']['db_connection_timeout'] == 60
