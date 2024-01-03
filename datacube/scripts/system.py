# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging

import click
from click import echo, style
from sqlalchemy.exc import OperationalError

import datacube
from datacube.cfg import ODCEnvironment, psql_url_from_config
from datacube.index import index_connect
from datacube.drivers.postgres._connections import IndexSetupError
from datacube.ui import click as ui
from datacube.ui.click import cli, handle_exception

_LOG = logging.getLogger('datacube-system')


@cli.group(name='system', help='System commands')
def system():
    pass


@system.command('init', help='Initialise the database')
@click.option(
    '--default-types/--no-default-types', is_flag=True, default=True,
    help="Add default types? (default: true)"
)
@click.option(
    '--init-users/--no-init-users', is_flag=True, default=True,
    help="Include user roles and grants. (default: true)"
)
@click.option(
    '--recreate-views/--no-recreate-views', is_flag=True, default=True,
    help="Recreate dynamic views"
)
@click.option(
    '--rebuild/--no-rebuild', is_flag=True, default=False,
    help="Rebuild all dynamic fields (caution: slow)"
)
@click.option(
    '--lock-table/--no-lock-table', is_flag=True, default=False,
    help="Allow table to be locked (eg. while creating missing indexes)"
)
@ui.pass_index(expect_initialised=False)
def database_init(index, default_types, init_users, recreate_views, rebuild, lock_table):
    echo('Initialising database...')

    was_created = index.init_db(with_default_types=default_types,
                                with_permissions=init_users)

    if was_created:
        echo(style('Created.', bold=True))
    else:
        echo(style('Updated.', bold=True))

    echo('Checking indexes/views.')
    index.metadata_types.check_field_indexes(
        allow_table_lock=lock_table,
        rebuild_indexes=rebuild,
        rebuild_views=recreate_views or rebuild,
    )
    echo('Done.')


@system.command('check', help='Check and display current configuration')
@ui.pass_config
def check(cfg_env: ODCEnvironment):
    """
    Verify & view current configuration
    """

    def echo_field(name, value):
        echo('{:<15}'.format(name + ':') + style(str(value), bold=True))

    echo_field('Version', datacube.__version__)
    echo_field('Index Driver', cfg_env.index_driver)
    db_url = psql_url_from_config(cfg_env)
    echo_field('Database URL:', db_url)

    echo()
    echo('Valid connection:\t', nl=False)
    try:
        index = index_connect(config_env=cfg_env)
        echo(style('YES', bold=True))
        if index.url_parts.username:
            for role, user, description in index.users.list_users():
                if user == index.url_parts.username:
                    echo('You have %s privileges.' % style(role.upper(), bold=True))
    except OperationalError as e:
        handle_exception('Error Connecting to Database: %s', e)
    except IndexSetupError as e:
        handle_exception('Database not initialised: %s', e)


@system.command('clone', help='Clone an existing ODC index into a new, empty index')
@click.option('--batch-size',
              help='Size of batches for bulk-adding to the new index',
              type=int,
              default=1000)
@click.option(
    '--skip-lineage/--no-skip-lineage', is_flag=True, default=False,
    help="Clone lineage data where possible. (default: true)"
)
@click.option(
    '--lineage-only/--no-lineage-only', is_flag=True, default=False,
    help="Clone lineage data only. (default: false)"
)
@click.argument('source-env', type=str, nargs=1)
@ui.pass_config
def clone(env: ODCEnvironment, batch_size: int, skip_lineage: bool, lineage_only: bool, source_env: str):
    if skip_lineage and lineage_only:
        echo("Cannot set both lineage-only and skip-lineage")
        exit(1)
    try:
        destination_index = index_connect(env, validate_connection=True)
    except OperationalError as e:
        handle_exception('Error Connecting to Destination Database: %s', e)
        exit(1)
    except IndexSetupError as e:
        handle_exception('Destination database not initialised: %s', e)
        exit(1)

    try:
        source_config = env._cfg[source_env]
    except KeyError:
        raise click.ClickException("No datacube config found for '{}'".format(source_env))
        exit(1)

    try:
        src_index = index_connect(source_config, validate_connection=True)
    except OperationalError as e:
        handle_exception('Error Connecting to Source Database: %s', e)
        exit(1)
    except IndexSetupError as e:
        handle_exception('Source database not initialised: %s', e)
        exit(1)
    # Any errors will have be logged.
    destination_index.clone(src_index, batch_size=batch_size, skip_lineage=skip_lineage, lineage_only=lineage_only)
    exit(0)
