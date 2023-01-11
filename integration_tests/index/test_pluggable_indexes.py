# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest

from configparser import ConfigParser

from datacube.index.postgres.index import Index


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_with_standard_index(uninitialised_postgres_db):
    index = Index(uninitialised_postgres_db)
    index.init_db()


def create_sample_config():
    parser = ConfigParser()
    parser.add_section('test_env')
    parser.set('test_env', 'index_driver', 'default')


def test_system_init(uninitialised_postgres_db, clirunner):
    result = clirunner(['system', 'init'], catch_exceptions=False)

    # Question: Should the Index be able to be specified on the command line, or should it come from the config file?

    if result.exit_code != 0:
        print(result.output)
        assert False
