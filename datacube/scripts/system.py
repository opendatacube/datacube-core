from __future__ import absolute_import

import logging
import click
from click import echo

from datacube.index import index_connect
from datacube.ui import click as ui
from datacube.ui.click import cli, handle_exception
from datacube.index.postgres._api import IndexSetupError
from sqlalchemy.exc import OperationalError


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
@ui.pass_index(expect_initialised=False)
def database_init(index, default_types, init_users):
    echo('Initialising database...')

    was_created = index.init_db(with_default_types=default_types,
                                with_permissions=init_users)

    if was_created:
        echo('Done.')
    else:
        echo('Updated.')


@system.command('check', help='Initialise the system')
@ui.pass_config
def check(config_file):
    """
    Verify & view current configuration
    """
    echo('Read configurations files from: %s' % config_file.files_loaded)
    echo('Host: {}:{}'.format(config_file.db_hostname or 'localhost', config_file.db_port or '5432'))
    echo('Database: {}'.format(config_file.db_database))
    echo('User: {}'.format(config_file.db_username))

    echo('\n')
    echo('Attempting connect')
    try:
        index_connect(local_config=config_file)
        echo('Success.')
    except OperationalError as e:
        handle_exception('Error Connecting to Database: %s', e)
    except IndexSetupError as e:
        handle_exception('Database not initialised: %s', e)

