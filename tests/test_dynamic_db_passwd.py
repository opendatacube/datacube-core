# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine.url import URL

from datacube.drivers.postgres._connections import PostgresDb, handle_dynamic_token_authentication


counter = [0]
last_base = [None]


def next_token(base):
    counter[0] = counter[0] + 1
    last_base[0] = base
    return f"{base}{counter[0]}"


def test_dynamic_password():
    url = URL.create('postgresql',
                     host="fake_host", database="fake_database", port=6543,
                     username="fake_username", password="fake_password")
    engine = PostgresDb._create_engine(url)
    counter[0] = 0
    last_base[0] = None
    handle_dynamic_token_authentication(engine, next_token, base="password")
    with pytest.raises(OperationalError):
        conn = engine.connect()
    assert counter[0] == 1
    assert last_base[0] == "password"
