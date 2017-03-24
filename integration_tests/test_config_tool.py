# coding=utf-8
"""
Module
"""
from __future__ import absolute_import, print_function

import logging
import random
from pathlib import Path

import pytest
from click.testing import CliRunner

import datacube.scripts.cli_app
from datacube.index.postgres import _dynamic
from datacube.index.postgres.tables._core import drop_db, has_schema, SCHEMA_NAME

_LOG = logging.getLogger(__name__)

EXAMPLE_STORAGE_TYPE_DOCS = Path(__file__).parent.parent. \
    joinpath('docs', 'config_samples', 'storage_types').glob('**/*.yaml')

EXAMPLE_DATASET_TYPE_DOCS = Path(__file__).parent.parent. \
    joinpath('docs', 'config_samples', 'dataset_types').glob('**/*.yaml')

# Documents that shouldn't be accepted as mapping docs.
INVALID_MAPPING_DOCS = Path(__file__).parent.parent. \
    joinpath('docs').glob('*')


def _run_cli(global_integration_cli_args, cli_method, opts, catch_exceptions=False, expect_success=True):
    exe_opts = list(global_integration_cli_args)
    exe_opts.extend(opts)
    runner = CliRunner()
    result = runner.invoke(
        cli_method,
        exe_opts,
        catch_exceptions=catch_exceptions
    )
    if expect_success:
        assert result.exit_code == 0, "Error for %r. output: %r" % (opts, result.output)
    return result


def _dataset_type_count(db):
    with db.connect() as connection:
        return len(list(connection.get_all_dataset_types()))


def test_add_example_dataset_types(global_integration_cli_args, db, default_metadata_type):
    """
    Add example mapping docs, to ensure they're valid and up-to-date.

    We add them all to a single database to check for things like duplicate ids.

    :type global_integration_cli_args: tuple[str]
    :type db: datacube.index.postgres._api.PostgresDb
    """
    existing_mappings = _dataset_type_count(db)

    print('{} mappings'.format(existing_mappings))
    for mapping_path in EXAMPLE_DATASET_TYPE_DOCS:
        print('Adding mapping {}'.format(mapping_path))
        result = _run_cli(
            global_integration_cli_args,
            datacube.scripts.cli_app.cli,
            [
                '-v', 'product', 'add',
                str(mapping_path)
            ]
        )
        assert result.exit_code == 0, "Error for %r. output: %r" % (str(mapping_path), result.output)
        mappings_count = _dataset_type_count(db)
        assert mappings_count > existing_mappings, "Mapping document was not added: " + str(mapping_path)
        existing_mappings = mappings_count


def test_error_returned_on_invalid(global_integration_cli_args, db):
    """
    :type global_integration_cli_args: tuple[str]
    :type db: datacube.index.postgres._api.PostgresDb
    """
    assert _dataset_type_count(db) == 0

    for mapping_path in INVALID_MAPPING_DOCS:
        result = _run_cli(
            global_integration_cli_args,
            datacube.scripts.cli_app.cli,
            [
                '-v', 'product', 'add',
                str(mapping_path)
            ],
            # TODO: Make this false when the cli is updated to print errors (rather than uncaught exceptions).
            catch_exceptions=True,
            expect_success=False
        )
        assert result.exit_code != 0, "Success return code for invalid document."
        assert _dataset_type_count(db) == 0, "Invalid document was added to DB"


def test_config_check(global_integration_cli_args, local_config):
    """
    :type global_integration_cli_args: tuple[str]
    :type local_config: datacube.config.LocalConfig
    """

    # This is not a very thorough check, we just check to see that
    # it prints something vaguely related and does not error-out.
    result = _run_cli(
        global_integration_cli_args,
        datacube.scripts.cli_app.cli,
        [
            '-v', 'system', 'check'
        ]
    )
    assert result.exit_code == 0
    host_line = 'Host: {}'.format(local_config.db_hostname)
    assert host_line in result.output
    user_line = 'User: {}'.format(local_config.db_username)
    assert user_line in result.output


def test_list_users_does_not_fail(global_integration_cli_args, local_config):
    """
    :type global_integration_cli_args: tuple[str]
    :type local_config: datacube.config.LocalConfig
    """
    # We don't want to make assumptions about available users during test runs.
    # (They are host-global, not specific to the database)
    # So we're just checking that it doesn't fail (and the SQL etc is well formed)
    result = _run_cli(
        global_integration_cli_args,
        datacube.scripts.cli_app.cli,
        [
            '-v', 'user', 'list'
        ]
    )
    assert result.exit_code == 0


def test_db_init_noop(global_integration_cli_args, local_config, ls5_telem_type):
    # Run on an existing database.
    result = _run_cli(
        global_integration_cli_args,
        datacube.scripts.cli_app.cli,
        [
            '-vv', 'system', 'init'
        ]
    )
    assert result.exit_code == 0
    assert 'Updated.' in result.output
    # It should not rebuild indexes by default
    assert 'Dropping index: dix_{}'.format(ls5_telem_type.name) not in result.output


def test_db_init_rebuild(global_integration_cli_args, local_config, ls5_telem_type):
    # We set the field creation logging to debug, as we assert its logging output below.
    _dynamic._LOG.setLevel(logging.DEBUG)

    # Run on an existing database.
    result = _run_cli(
        global_integration_cli_args,
        datacube.scripts.cli_app.cli,
        [
            '-vv', 'system', 'init', '--rebuild'
        ]
    )
    assert result.exit_code == 0
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


def test_db_init(global_integration_cli_args, db, local_config):
    with db.connect() as connection:
        drop_db(connection._connection)

        assert not has_schema(db._engine, connection._connection)

    # Run on an empty database.
    cli_method = datacube.scripts.cli_app.cli
    result = _run_cli(global_integration_cli_args, cli_method, [
        '-v', 'system', 'init'
    ])
    assert result.exit_code == 0
    assert 'Created.' in result.output

    with db.connect() as connection:
        assert has_schema(db._engine, connection._connection)


@pytest.mark.parametrize("username, user_description", [
    ('test_"user"_{n}', None),
    ('test_"user"_{n}', 'Test user description'),
    # Test that names are escaped
    ('user_"invalid+_chars_{n}', None),
    ('user_invalid_desc_{n}', 'Invalid "\' chars in description'),
])
def test_user_creation(global_integration_cli_args, db, username, user_description, default_metadata_type):
    """
    Add a user, grant them, delete them.

    :type global_integration_cli_args: tuple[str]
    :type db: datacube.index.postgres._api.PostgresDb
    """
    existing_mappings = _dataset_type_count(db)

    print('{} mappings'.format(existing_mappings))

    username = username.format(n=random.randint(111111, 999999))

    # No user exists.
    assert_no_user(global_integration_cli_args, username)

    # Create them
    args = ['-v', 'user', 'create', 'ingest', username]
    if user_description:
        args.extend(['--description', user_description])
    _run_cli(
        global_integration_cli_args,
        datacube.scripts.cli_app.cli,
        args
    )
    assert_user_with_role(global_integration_cli_args, 'ingest', username)

    # Grant them 'manage' permission
    _run_cli(
        global_integration_cli_args,
        datacube.scripts.cli_app.cli,
        [
            '-v', 'user', 'grant', 'manage', username
        ]
    )
    assert_user_with_role(global_integration_cli_args, 'manage', username)

    # Delete them
    _run_cli(
        global_integration_cli_args,
        datacube.scripts.cli_app.cli,
        [
            '-v', 'user', 'delete', username
        ]
    )
    assert_no_user(global_integration_cli_args, username)


def assert_user_with_role(global_integration_cli_args, role, user_name):
    result = _run_cli(
        global_integration_cli_args,
        datacube.scripts.cli_app.cli,
        [
            '-v', 'user', 'list'
        ]
    )
    assert '{}\t{}'.format(role, user_name) in result.output


def assert_no_user(global_integration_cli_args, username):
    result = _run_cli(
        global_integration_cli_args,
        datacube.scripts.cli_app.cli,
        [
            '-v', 'user', 'list'
        ]
    )
    assert username not in result.output
