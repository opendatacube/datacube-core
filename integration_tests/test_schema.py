from datacube.drivers.postgres.sql import pg_exists
from datacube.drivers.postgres._core import schema_qualified, __schema_version__


def test_schema_version(initialised_postgres_db):
    with initialised_postgres_db.connect() as dbapi:
        assert pg_exists(dbapi._connection.engine, schema_qualified('schema_version'))
        assert dbapi.get_schema_version_info()['schema_version'] == __schema_version__
