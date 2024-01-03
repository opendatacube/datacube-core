# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest

from datacube.index.postgres.index import Index


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_with_standard_index(uninitialised_postgres_db, cfg_env):
    index = Index(uninitialised_postgres_db, cfg_env)
    index.init_db()


def test_system_init(uninitialised_postgres_db, clirunner):
    result = clirunner(['system', 'init'], catch_exceptions=False)

    # Question: Should the Index be able to be specified on the command line, or should it come from the config file?

    if result.exit_code != 0:
        print(result.output)
        assert False
