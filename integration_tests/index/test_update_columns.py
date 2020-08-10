"""
Test creation of added/updated columns during
`datacube system init`
"""
from datacube.drivers.postgres.sql import pg_column_exists
from datacube.drivers.postgres import _schema

COLUMN_PRESENCE = """
SELECT EXISTS (SELECT 1 FROM information_schema.columns
WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='{column}');
"""


def test_added_column(clirunner, uninitialised_postgres_db):
    # Run on an empty database.
    result = clirunner(["system", "init"])
    assert "Created." in result.output

    with uninitialised_postgres_db.connect() as connection:
        assert pg_column_exists(connection, _schema.METADATA_TYPE, "updated")
        assert pg_column_exists(connection, _schema.PRODUCT, "updated")
        assert pg_column_exists(connection, _schema.DATASET, "updated")
        assert pg_column_exists(connection, _schema.DATASET_LOCATION, "added")
