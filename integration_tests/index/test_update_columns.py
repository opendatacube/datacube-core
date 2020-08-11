"""
Test creation of added/updated columns during
`datacube system init`
"""
from datacube.drivers.postgres.sql import SCHEMA_NAME
from datacube.drivers.postgres import _schema


COLUMN_PRESENCE = """
SELECT EXISTS (SELECT 1 FROM information_schema.columns
WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='{column}');
"""

DROP_COLUMN = """
ALTER TABLE {schema}.{table} DROP COLUMN {column}
"""


def check_column(conn, table_name: str, column_name: str) -> bool:
    column_result = conn.execute(
        COLUMN_PRESENCE.format(
            schema=SCHEMA_NAME, table=table_name, column=column_name
        )
    ).fetchone()
    return column_result == (True,)


def drop_column(conn, table, column):
    conn.execute(DROP_COLUMN.format(
        schema=SCHEMA_NAME, table=table, column=column))


def test_added_column(clirunner, uninitialised_postgres_db):
    # Run on an empty database.
    result = clirunner(["system", "init"])
    assert "Created." in result.output

    with uninitialised_postgres_db.connect() as connection:
        assert check_column(connection, _schema.METADATA_TYPE.name, "updated")
        assert not check_column(connection, _schema.METADATA_TYPE.name, "fake_column")
        assert check_column(connection, _schema.PRODUCT.name, "updated")
        assert not check_column(connection, _schema.PRODUCT.name, "fake_column")
        assert check_column(connection, _schema.DATASET.name, "updated")
        assert not check_column(connection, _schema.DATASET.name, "fake_column")
        assert check_column(connection, _schema.DATASET_LOCATION.name, "added")
        assert not check_column(connection, _schema.DATASET_LOCATION.name, "fake_column")


def test_readd_column(clirunner, uninitialised_postgres_db):
    # Run on an empty database. drop columns and readd
    result = clirunner(["system", "init"])
    assert "Created." in result.output

    with uninitialised_postgres_db.connect() as connection:
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

    with uninitialised_postgres_db.connect() as connection:
        assert check_column(connection, _schema.METADATA_TYPE.name, "updated")
        assert check_column(connection, _schema.PRODUCT.name, "updated")
        assert check_column(connection, _schema.DATASET.name, "updated")
        assert check_column(connection, _schema.DATASET_LOCATION.name, "added")
