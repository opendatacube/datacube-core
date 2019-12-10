# coding=utf-8
"""
Module
"""
import logging
import random
import re
from pathlib import Path

import pytest

from datacube.drivers.postgres import _dynamic
from datacube.drivers.postgres._core import drop_db, has_schema, SCHEMA_NAME

EXAMPLE_DATASET_TYPE_DOCS = map(str, Path(__file__).parent.parent.
                                joinpath('docs', 'config_samples', 'dataset_types').glob('**/*.yaml'))

# Documents that shouldn't be accepted as mapping docs.
INVALID_MAPPING_DOCS = map(str, Path(__file__).parent.parent.joinpath('docs').glob('*'))


def _dataset_type_count(db):
    with db.connect() as connection:
        return len(list(connection.get_all_products()))


def test_add_example_dataset_types(clirunner, initialised_postgres_db, default_metadata_type):
    """
    Add example mapping docs, to ensure they're valid and up-to-date.

    We add them all to a single database to check for things like duplicate ids.

    :type initialised_postgres_db: datacube.drivers.postgres._connections.PostgresDb
    """
    existing_mappings = _dataset_type_count(initialised_postgres_db)

    print('{} mappings'.format(existing_mappings))
    for mapping_path in EXAMPLE_DATASET_TYPE_DOCS:
        print('Adding mapping {}'.format(mapping_path))

        result = clirunner(['-v', 'product', 'add', mapping_path])
        assert result.exit_code == 0

        mappings_count = _dataset_type_count(initialised_postgres_db)
        assert mappings_count > existing_mappings, "Mapping document was not added: " + str(mapping_path)
        existing_mappings = mappings_count

    result = clirunner(['-v', 'metadata', 'list'])
    assert result.exit_code == 0

    result = clirunner(['-v', 'metadata', 'show', '-f', 'json', 'eo'],
                       expect_success=True)
    assert result.exit_code == 0

    result = clirunner(['-v', 'metadata', 'show'],
                       expect_success=True)
    assert result.exit_code == 0

    result = clirunner(['-v', 'product', 'list'])
    assert result.exit_code == 0

    expect_result = 0 if existing_mappings > 0 else 1
    result = clirunner(['-v', 'product', 'show'],
                       expect_success=(expect_result == 0))
    assert result.exit_code == expect_result

    if existing_mappings > 1:
        result = clirunner(['-v', 'product', 'show', '-f', 'json'],
                           expect_success=False)
        assert result.exit_code == 1

        result = clirunner(['-v', 'product', 'show', '-f', 'json', 'ls8_level1_usgs'],
                           expect_success=False)
        assert result.exit_code == 0

        result = clirunner(['-v', 'product', 'show', '-f', 'yaml', 'ls8_level1_usgs'],
                           expect_success=False)
        assert result.exit_code == 0


def test_error_returned_on_invalid(clirunner, initialised_postgres_db):
    """
    :type initialised_postgres_db: datacube.drivers.postgres._connections.PostgresDb
    """
    assert _dataset_type_count(initialised_postgres_db) == 0

    for mapping_path in INVALID_MAPPING_DOCS:
        result = clirunner(
            [
                'product', 'add', mapping_path
            ],
            # TODO: Make this false when the cli is updated to print errors (rather than uncaught exceptions).
            catch_exceptions=True,
            expect_success=False
        )
        assert result.exit_code != 0, "Success return code for invalid document."
        assert _dataset_type_count(initialised_postgres_db) == 0, "Invalid document was added to DB"


def test_config_check(clirunner, initialised_postgres_db, local_config):
    """
    :type local_config: datacube.config.LocalConfig
    """

    # This is not a very thorough check, we just check to see that
    # it prints something vaguely related and does not error-out.
    result = clirunner(
        [
            'system', 'check'
        ]
    )

    host_regex = re.compile(r'.*Host:\s+{}.*'.format(local_config['db_hostname']),
                            flags=re.DOTALL)  # Match across newlines
    user_regex = re.compile(r'.*User:\s+{}.*'.format(local_config['db_username']),
                            flags=re.DOTALL)
    assert host_regex.match(result.output)
    assert user_regex.match(result.output)


def test_list_users_does_not_fail(clirunner, local_config, initialised_postgres_db):
    """
    :type local_config: datacube.config.LocalConfig
    """
    # We don't want to make assumptions about available users during test runs.
    # (They are host-global, not specific to the database)
    # So we're just checking that it doesn't fail (and the SQL etc is well formed)
    result = clirunner(
        [
            'user', 'list'
        ]
    )
    assert result.exit_code == 0


def test_db_init_noop(clirunner, local_config, ls5_telem_type):
    # Run on an existing database.
    result = clirunner(
        [
            '-v', 'system', 'init'
        ]
    )
    assert 'Updated.' in result.output
    # It should not rebuild indexes by default
    assert 'Dropping index: dix_{}'.format(ls5_telem_type.name) not in result.output


def test_db_init_rebuild(clirunner, local_config, ls5_telem_type):
    # We set the field creation logging to debug, as we assert its logging output below.
    _dynamic._LOG.setLevel(logging.DEBUG)

    # Run on an existing database.
    result = clirunner(
        [
            '-v', 'system', 'init', '--rebuild'
        ]
    )
    assert 'Updated.' in result.output
    # It should have recreated views and indexes.
    assert 'Dropping index: dix_{}'.format(ls5_telem_type.name) in result.output
    assert 'Creating index: dix_{}'.format(ls5_telem_type.name) in result.output
    assert 'Dropping view: {schema}.dv_{name}_dataset'.format(
        schema=SCHEMA_NAME, name=ls5_telem_type.name
    ) in result.output
    assert 'Creating view: {schema}.dv_{name}_dataset'.format(
        schema=SCHEMA_NAME, name=ls5_telem_type.name
    ) in result.output


def test_db_init(clirunner, initialised_postgres_db):
    with initialised_postgres_db.connect() as connection:
        drop_db(connection._connection)

        assert not has_schema(initialised_postgres_db._engine, connection._connection)

    # Run on an empty database.
    result = clirunner(['system', 'init'])
    assert 'Created.' in result.output

    with initialised_postgres_db.connect() as connection:
        assert has_schema(initialised_postgres_db._engine, connection._connection)


def test_add_no_such_product(clirunner, initialised_postgres_db):
    result = clirunner(['dataset', 'add', '--dtype', 'no_such_product'], expect_success=False)
    assert result.exit_code != 0
    assert "DEPRECATED option detected" in result.output
    assert "ERROR Supplied product name" in result.output


@pytest.fixture(params=[
    ('test_"user"_{n}', None),
    ('test_"user"_{n}', 'Test user description'),
    # Test that names are escaped
    ('test_user_"invalid+_chars_{n}', None),
    ('test_user_invalid_desc_{n}', 'Invalid "\' chars in description')])
def example_user(clirunner, initialised_postgres_db, request):
    username, description = request.param

    username = username.format(n=random.randint(111111, 999999))

    # test_roles = (user_name for role_name, user_name, desc in roles if user_name.startswith('test_'))
    with initialised_postgres_db.connect() as connection:
        users = (user_name for role_name, user_name, desc in connection.list_users())
        if username in users:
            connection.drop_users([username])

    # No user exists.
    assert_no_user(clirunner, username)

    yield username, description

    with initialised_postgres_db.connect() as connection:
        users = (user_name for role_name, user_name, desc in connection.list_users())
        if username in users:
            connection.drop_users([username])


def test_user_creation(clirunner, example_user):
    """
    Add a user, grant them, delete them.

    This test requires role creation privileges on the PostgreSQL instance used for testing...

    :type db: datacube.drivers.postgres._connections.PostgresDb
    """
    username, user_description = example_user

    # Create them
    args = ['user', 'create', 'ingest', username]
    if user_description:
        args.extend(['--description', user_description])
    clirunner(args)
    assert_user_with_role(clirunner, 'ingest', username)

    # Grant them 'manage' permission
    clirunner(['user', 'grant', 'manage', username])
    assert_user_with_role(clirunner, 'manage', username)

    # Delete them
    clirunner(['user', 'delete', username])
    assert_no_user(clirunner, username)


def assert_user_with_role(clirunner, role, user_name):
    result = clirunner(['user', 'list'])
    assert '{}{}'.format('user: ', user_name) in result.output


def assert_no_user(clirunner, username):
    result = clirunner(['user', 'list'])
    assert username not in result.output
