# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest

from datacube import Datacube
from datacube.cfg import ODCConfig


@pytest.mark.parametrize("psycopg_version", [2, 3])
def test_multiple_environment_config(tmpdir, psycopg_version: int) -> None:
    raw_config = f"""
[DEFAULT]
db_username: test_user
index_driver: default
psycopg_version: {psycopg_version}

[default]
db_hostname: db.opendatacube.test

[testalt]
db_hostname: alt-db.opendatacube.test
    """
    cfg = ODCConfig(text=raw_config)
    cfg_env = cfg[None]
    assert cfg_env.db_hostname == "db.opendatacube.test"
    alt_env = cfg["testalt"]
    assert alt_env.db_hostname == "alt-db.opendatacube.test"
    assert cfg_env.index_driver == "default"

    # Make sure the correct config is passed through the API
    # Parsed config:
    prefix = f"postgresql+psycopg{'' if psycopg_version == 3 else '2'}"
    db_url = f"{prefix}://{cfg_env.db_username}@db.opendatacube.test/datacube"
    alt_db_url = f"{prefix}://{alt_env.db_username}@alt-db.opendatacube.test/datacube"

    with Datacube(env=cfg_env, validate_connection=False) as dc:
        assert str(dc.index.url) == db_url

    # When none specified, default environment is loaded
    with Datacube(config=cfg, validate_connection=False) as dc:
        assert str(dc.index.url) == db_url
    # When specific environment is loaded
    with Datacube(config=cfg, env="testalt", validate_connection=False) as dc:
        assert str(dc.index.url) == alt_db_url
