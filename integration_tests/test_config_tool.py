# coding=utf-8
"""
Module
"""
from __future__ import absolute_import, print_function

import logging
from pathlib import Path

from click.testing import CliRunner

import datacube.scripts.cli_app
from datacube.index.postgres.tables._core import drop_db, has_schema, SCHEMA_NAME

_LOG = logging.getLogger(__name__)

EXAMPLE_STORAGE_TYPE_DOCS = Path(__file__).parent.parent. \
    joinpath('docs', 'config_samples', 'storage_types').glob('**/*.yaml')

EXAMPLE_DATASET_TYPE_DOCS = Path(__file__).parent.parent. \
    joinpath('docs', 'config_samples', 'dataset_types').glob('**/*.yaml')

# Documents that shouldn't be accepted as mapping docs.
INVALID_MAPPING_DOCS = Path(__file__).parent.parent. \
    joinpath('docs').glob('*')


def _run_cli(cli_method, opts, catch_exceptions=False):
    runner = CliRunner()
    result = runner.invoke(
        cli_method,
        opts,
        catch_exceptions=catch_exceptions
    )
    return result


def _dataset_type_count(db):
    return len(list(db.get_all_dataset_types()))


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
        opts = list(global_integration_cli_args)
        opts.extend(
            [
                '-v', 'product', 'add',
                str(mapping_path)
            ]
        )
        result = _run_cli(
            datacube.scripts.cli_app.cli,
            opts
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
        opts = list(global_integration_cli_args)
        opts.extend(
            [
                '-v', 'product', 'add',
                str(mapping_path)
            ]
        )

        result = _run_cli(
            datacube.scripts.cli_app.cli,
            opts,
            # TODO: Make this false when the cli is updated to print errors (rather than uncaught exceptions).
            catch_exceptions=True
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
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-v', 'system', 'check'
        ]
    )
    result = _run_cli(
        datacube.scripts.cli_app.cli,
        opts
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
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-v', 'user', 'list'
        ]
    )
    result = _run_cli(
        datacube.scripts.cli_app.cli,
        opts
    )
    assert result.exit_code == 0


def test_db_init_noop(global_integration_cli_args, local_config, default_metadata_type):
    # Run on an existing database.
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-vv', 'system', 'init'
        ]
    )
    result = _run_cli(
        datacube.scripts.cli_app.cli,
        opts
    )
    assert result.exit_code == 0
    assert 'Updated.' in result.output
    # It should not rebuild indexes by default
    assert 'Dropping index: dix_{}'.format(default_metadata_type.name) not in result.output


def test_db_init_rebuild(global_integration_cli_args, local_config, default_metadata_type):
    # Run on an existing database.
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-vv', 'system', 'init', '--rebuild'
        ]
    )
    result = _run_cli(
        datacube.scripts.cli_app.cli,
        opts
    )
    assert result.exit_code == 0
    assert 'Updated.' in result.output
    # It should have recreated views and indexes.
    assert 'Dropping index: dix_{}'.format(default_metadata_type.name) in result.output
    assert 'Creating index: dix_{}'.format(default_metadata_type.name) in result.output
    assert 'Dropping view: {schema}.dv_{name}_dataset'.format(
        schema=SCHEMA_NAME, name=default_metadata_type.name
    ) in result.output
    assert 'Creating view: {schema}.dv_{name}_dataset'.format(
        schema=SCHEMA_NAME, name=default_metadata_type.name
    ) in result.output


def test_db_init(global_integration_cli_args, db, local_config):
    drop_db(db._connection)

    assert not has_schema(db._engine, db._connection)

    # Run on an empty database.
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-v', 'system', 'init'
        ]
    )
    cli_method = datacube.scripts.cli_app.cli
    result = _run_cli(cli_method, opts)
    assert result.exit_code == 0
    assert 'Created.' in result.output

    assert has_schema(db._engine, db._connection)
