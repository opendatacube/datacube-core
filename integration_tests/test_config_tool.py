# coding=utf-8
"""
Module
"""
from __future__ import absolute_import, print_function

import logging
from pathlib import Path

from click.testing import CliRunner

import datacube.scripts.config_tool
from datacube.index.postgres.tables._core import drop_db, has_schema

_LOG = logging.getLogger(__name__)

EXAMPLE_STORAGE_TYPE_DOCS = Path(__file__).parent.parent. \
    joinpath('docs', 'config_samples').glob('**/*.yaml')

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


def test_add_example_storage_types(global_integration_cli_args, db):
    """
    Add example mapping docs, to ensure they're valid and up-to-date.

    We add them all to a single database to check for things like duplicate ids.

    :type global_integration_cli_args: tuple[str]
    :type db: datacube.index.postgres._api.PostgresDb
    """
    existing_mappings = db.count_storage_types()
    print('{} mappings'.format(existing_mappings))
    for mapping_path in EXAMPLE_STORAGE_TYPE_DOCS:
        print('Adding mapping {}'.format(mapping_path))
        opts = list(global_integration_cli_args)
        opts.extend(
            [
                '-v', 'storage', 'add',
                str(mapping_path)
            ]
        )
        result = _run_cli(
            datacube.scripts.config_tool.cli,
            opts
        )
        assert result.exit_code == 0, "Error for %r. output: %r" % (str(mapping_path), result.output)
        mappings_count = db.count_storage_types()
        assert mappings_count > existing_mappings, "Mapping document was not added: " + str(mapping_path)
        existing_mappings = mappings_count


def test_error_returned_on_invalid(global_integration_cli_args, db):
    """
    :type global_integration_cli_args: tuple[str]
    :type db: datacube.index.postgres._api.PostgresDb
    """
    assert db.count_storage_types() == 0

    for mapping_path in INVALID_MAPPING_DOCS:
        opts = list(global_integration_cli_args)
        opts.extend(
            [
                '-v', 'storage', 'add',
                str(mapping_path)
            ]
        )

        result = _run_cli(
            datacube.scripts.config_tool.cli,
            opts,
            # TODO: Make this false when the cli is updated to print errors (rather than uncaught exceptions).
            catch_exceptions=True
        )
        assert result.exit_code != 0, "Success return code for invalid document."
        assert db.count_storage_types() == 0, "Invalid document was added to DB"


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
            '-v', 'check'
        ]
    )
    result = _run_cli(
        datacube.scripts.config_tool.cli,
        opts
    )
    assert result.exit_code == 0
    host_line = 'Host: {}'.format(local_config.db_hostname)
    assert host_line in result.output
    user_line = 'User: {}'.format(local_config.db_username)
    assert user_line in result.output


def test_db_init_noop(global_integration_cli_args, local_config):
    # Run on an existing database.
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-v', 'database', 'init'
        ]
    )
    result = _run_cli(
        datacube.scripts.config_tool.cli,
        opts
    )
    assert result.exit_code == 0
    assert 'Nothing to do.' in result.output


def test_db_init(global_integration_cli_args, db, local_config):
    drop_db(db._connection)

    assert not has_schema(db._engine, db._connection)

    # Run on an empty database.
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-v', 'database', 'init'
        ]
    )
    cli_method = datacube.scripts.config_tool.cli
    result = _run_cli(cli_method, opts)
    assert result.exit_code == 0
    assert 'Done.' in result.output

    assert has_schema(db._engine, db._connection)
