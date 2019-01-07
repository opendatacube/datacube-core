from configparser import ConfigParser

from datacube.drivers.s3aio_index import S3AIOIndex
from datacube.index.index import Index


def test_can_create_s3_index(uninitialised_postgres_db):
    s3index = S3AIOIndex(uninitialised_postgres_db)

    assert not s3index.connected_to_s3_database()

    s3index.init_db()

    assert s3index.connected_to_s3_database()


def test_with_standard_index(uninitialised_postgres_db):
    index = Index(uninitialised_postgres_db)
    index.init_db()


def create_sample_config():
    parser = ConfigParser()
    parser.add_section('test_env')
    parser.set('test_env', 'index_driver', 'default')
    parser.set('test_env', 'index_driver', 'S3AIOIndex')


def test_system_init(uninitialised_postgres_db, clirunner):
    result = clirunner(['system', 'init'], catch_exceptions=False)

    # Question: Should the Index be able to be specified on the command line, or should it come from the config file?

    if result.exit_code != 0:
        print(result.output)
        assert False
