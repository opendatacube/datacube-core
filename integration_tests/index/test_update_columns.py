# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Test creation of added/updated columns during
`datacube system init`
"""
import pytest

from datacube.drivers.postgres.sql import SCHEMA_NAME
from datacube.drivers.postgres import _schema


COLUMN_PRESENCE = """
select exists (select 1 from information_schema.columns
where table_schema='{schema}' and table_name='{table}' and column_name='{column}');
"""

DROP_COLUMN = """
alter table {schema}.{table} drop column {column}
"""

TRIGGER_PRESENCE = """
select tgname
from pg_trigger
where not tgisinternal
and tgrelid = '{schema}.{table}'::regclass;
"""


def check_column(conn, table_name: str, column_name: str) -> bool:
    column_result = conn.execute(
        COLUMN_PRESENCE.format(
            schema=SCHEMA_NAME, table=table_name, column=column_name
        )
    ).fetchone()
    return column_result == (True,)


def check_trigger(conn, table_name: str) -> bool:
    trigger_result = conn.execute(
        TRIGGER_PRESENCE.format(schema=SCHEMA_NAME, table=table_name)
    ).fetchone()
    if trigger_result is None:
        return False
    return 'row_update_time' in trigger_result[0]


def drop_column(conn, table: str, column: str):
    conn.execute(DROP_COLUMN.format(
        schema=SCHEMA_NAME, table=table, column=column))


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_added_column(clirunner, uninitialised_postgres_db):
    # Run on an empty database.
    result = clirunner(["system", "init"])
    assert "Created." in result.output

    with uninitialised_postgres_db._connect() as connection:
        assert check_column(connection, _schema.METADATA_TYPE.name, "updated")
        assert not check_column(connection, _schema.METADATA_TYPE.name, "fake_column")
        assert check_column(connection, _schema.PRODUCT.name, "updated")
        assert not check_column(connection, _schema.PRODUCT.name, "fake_column")
        assert check_column(connection, _schema.DATASET.name, "updated")
        assert not check_column(connection, _schema.DATASET.name, "fake_column")
        assert check_column(connection, _schema.DATASET_LOCATION.name, "added")
        assert not check_column(connection, _schema.DATASET_LOCATION.name, "fake_column")

        # Check for triggers
        assert check_trigger(connection, _schema.METADATA_TYPE.name)
        assert check_trigger(connection, _schema.PRODUCT.name)
        assert check_trigger(connection, _schema.DATASET.name)
        assert not check_trigger(connection, _schema.DATASET_LOCATION.name)


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_readd_column(clirunner, uninitialised_postgres_db):
    # Run on an empty database. drop columns and readd
    result = clirunner(["system", "init"])
    assert "Created." in result.output

    with uninitialised_postgres_db._connect() as connection:
        # Drop all the columns for an init rerun
        drop_column(connection, _schema.METADATA_TYPE.name, "updated")
        drop_column(connection, _schema.PRODUCT.name, "updated")
        drop_column(connection, _schema.DATASET.name, "updated")
        drop_column(connection, _schema.DATASET_LOCATION.name, "added")

        assert not check_column(connection, _schema.METADATA_TYPE.name, "updated")
        assert not check_column(connection, _schema.PRODUCT.name, "updated")
        assert not check_column(connection, _schema.DATASET.name, "updated")
        assert not check_column(connection, _schema.DATASET_LOCATION.name, "added")

    result = clirunner(["system", "init"])

    with uninitialised_postgres_db._connect() as connection:
        assert check_column(connection, _schema.METADATA_TYPE.name, "updated")
        assert check_column(connection, _schema.PRODUCT.name, "updated")
        assert check_column(connection, _schema.DATASET.name, "updated")
        assert check_column(connection, _schema.DATASET_LOCATION.name, "added")
