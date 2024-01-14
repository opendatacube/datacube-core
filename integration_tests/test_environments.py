# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from datacube import Datacube
from datacube.cfg import ODCConfig


def test_multiple_environment_config(tmpdir):
    raw_config = """
[DEFAULT]
db_username: test_user
index_driver: default

[default]
db_hostname: db.opendatacube.test

[testalt]
db_hostname: alt-db.opendatacube.test
    """

    cfg = ODCConfig(text=raw_config)
    cfg_env = cfg[None]
    assert cfg_env.db_hostname == 'db.opendatacube.test'
    alt_env = cfg['testalt']
    assert alt_env.db_hostname == 'alt-db.opendatacube.test'
    assert cfg_env.index_driver == 'default'

    # Make sure the correct config is passed through the API
    # Parsed config:
    db_url = f'postgresql://{cfg_env.db_username}@db.opendatacube.test:5432/datacube'
    alt_db_url = f'postgresql://{alt_env.db_username}@alt-db.opendatacube.test:5432/datacube'

    with Datacube(env=cfg_env, validate_connection=False) as dc:
        assert str(dc.index.url) == db_url

    # When none specified, default environment is loaded
    with Datacube(config=cfg, validate_connection=False) as dc:
        assert str(dc.index.url) == db_url
    # When specific environment is loaded
    with Datacube(config=cfg, env='testalt', validate_connection=False) as dc:
        assert str(dc.index.url) == alt_db_url
