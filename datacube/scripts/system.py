from __future__ import absolute_import

import logging

import click
from click import echo
from sqlalchemy.exc import OperationalError

import datacube
from datacube.index import index_connect
from datacube.index.postgres._api import IndexSetupError
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
    '--rebuild/--no-rebuild', is_flag=True, default=False,
    help="Rebuild dynamic indexes & views (caution: slow)"
)
@click.option(
    '--lock-table/--no-lock-table', is_flag=True, default=False,
    help="Allow table to be locked (eg. while creating missing indexes)"
)
@ui.pass_index(expect_initialised=False)
def database_init(index, default_types, init_users, rebuild, lock_table):
    echo('Initialising database...')

    was_created = index.init_db(with_default_types=default_types,
                                with_permissions=init_users)

    if was_created:
        echo('Created.')
    else:
        echo('Updated.')

    echo('Checking indexes/views.')
    index.metadata_types.check_field_indexes(
        allow_table_lock=lock_table,
        rebuild_all=rebuild
    )
    echo('Done.')


@system.command('check', help='Initialise the system')
@ui.pass_config
def check(config_file):
    """
    Verify & view current configuration
    """
    echo('Version: %s' % datacube.__version__)
    echo('Read configurations files from: %s' % config_file.files_loaded)
    echo('Host: {}:{}'.format(config_file.db_hostname or 'localhost', config_file.db_port or '5432'))
    echo('Database: {}'.format(config_file.db_database))
    echo('User: {}'.format(config_file.db_username))

    echo('\n')
    echo('Attempting connect')
    try:
        index = index_connect(local_config=config_file)
        echo('Success.')
        for role, user, description in index.users.list_users():
            if user == config_file.db_username:
                echo('You have %s privileges.' % role.upper())
    except OperationalError as e:
        handle_exception('Error Connecting to Database: %s', e)
    except IndexSetupError as e:
        handle_exception('Database not initialised: %s', e)
