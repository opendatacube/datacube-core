# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Test creation of added/updated columns during
`datacube system init`
"""

import pytest
from sqlalchemy import text

from datacube.drivers.postgres import _schema
from datacube.drivers.postgres.sql import SCHEMA_NAME, pg_column_exists

DROP_COLUMN = """
alter table {schema}.{table} drop column {column}
"""

TRIGGER_PRESENCE = """
select tgname
from pg_trigger
where not tgisinternal
and tgrelid = '{schema}.{table}'::regclass;
"""


def check_trigger(conn, table_name: str) -> bool:
    trigger_result = conn.execute(
        text(TRIGGER_PRESENCE.format(schema=SCHEMA_NAME, table=table_name))
    ).fetchone()
    if trigger_result is None:
        return False
    return "row_update_time" in trigger_result[0]


def drop_column(conn, table: str, column: str) -> None:
    conn.execute(
        text(DROP_COLUMN.format(schema=SCHEMA_NAME, table=table, column=column))
    )


@pytest.mark.parametrize("uninitialised_postgres_db", ("UTC",), indirect=True)
@pytest.mark.parametrize("datacube_env_name", ("datacube", "datacube3"))
def test_added_column(clirunner, uninitialised_postgres_db) -> None:
    # Run on an empty database.
    result = clirunner(["--env", "datacube", "system", "init"])
    assert "Created." in result.output

    with uninitialised_postgres_db._connect() as connection:
        c = connection._connection
        assert pg_column_exists(c, _schema.METADATA_TYPE.name, "updated")
        assert pg_column_exists(c, _schema.PRODUCT.name, "updated")
        assert pg_column_exists(c, _schema.DATASET.name, "updated")
        assert pg_column_exists(c, _schema.DATASET_LOCATION.name, "added")

        # Check for triggers
        assert check_trigger(connection, _schema.METADATA_TYPE.name)
        assert check_trigger(connection, _schema.PRODUCT.name)
        assert check_trigger(connection, _schema.DATASET.name)
        assert not check_trigger(connection, _schema.DATASET_LOCATION.name)


@pytest.mark.parametrize("uninitialised_postgres_db", ("UTC",), indirect=True)
@pytest.mark.parametrize("datacube_env_name", ("datacube", "datacube3"))
def test_readd_column(clirunner, uninitialised_postgres_db) -> None:
    # Run on an empty database. drop columns and re-add
    result = clirunner(["--env", "datacube", "system", "init"])
    assert "Created." in result.output

    with uninitialised_postgres_db._connect() as connection:
        c = connection._connection
        # Drop all the columns for an init rerun
        drop_column(connection, _schema.METADATA_TYPE.name, "updated")
        drop_column(connection, _schema.PRODUCT.name, "updated")
        drop_column(connection, _schema.DATASET.name, "updated")
        drop_column(connection, _schema.DATASET_LOCATION.name, "added")

        assert not pg_column_exists(c, _schema.METADATA_TYPE.name, "updated")
        assert not pg_column_exists(c, _schema.PRODUCT.name, "updated")
        assert not pg_column_exists(c, _schema.DATASET.name, "updated")
        assert not pg_column_exists(c, _schema.DATASET_LOCATION.name, "added")

    result = clirunner(["--env", "datacube", "system", "init"])

    with uninitialised_postgres_db._connect() as connection:
        c = connection._connection
        assert pg_column_exists(c, _schema.METADATA_TYPE.name, "updated")
        assert pg_column_exists(c, _schema.PRODUCT.name, "updated")
        assert pg_column_exists(c, _schema.DATASET.name, "updated")
        assert pg_column_exists(c, _schema.DATASET_LOCATION.name, "added")
