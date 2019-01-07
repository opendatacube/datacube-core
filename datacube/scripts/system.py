import logging

import click
from click import echo, style
from sqlalchemy.exc import OperationalError

import datacube
from datacube.index import index_connect
from datacube.drivers.postgres._connections import IndexSetupError
from datacube.ui import click as ui
from datacube.ui.click import cli, handle_exception
from datacube.config import LocalConfig

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
# TODO: Need to be able to specify the type of index. In our current case, whether to create s3aio specific tables
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
def check(local_config: LocalConfig):
    """
    Verify & view current configuration
    """

    def echo_field(name, value):
        echo('{:<15}'.format(name + ':') + style(str(value), bold=True))

    echo_field('Version', datacube.__version__)
    echo_field('Config files', ','.join(local_config.files_loaded))
    echo_field('Host',
               '{}:{}'.format(local_config['db_hostname'] or 'localhost', local_config.get('db_port', None) or '5432'))

    echo_field('Database', local_config['db_database'])
    echo_field('User', local_config['db_username'])
    echo_field('Environment', local_config['env'])
    echo_field('Index Driver', local_config['index_driver'])

    echo()
    echo('Valid connection:\t', nl=False)
    try:
        index = index_connect(local_config=local_config)
        echo(style('YES', bold=True))
        for role, user, description in index.users.list_users():
            if user == local_config['db_username']:
                echo('You have %s privileges.' % style(role.upper(), bold=True))
    except OperationalError as e:
        handle_exception('Error Connecting to Database: %s', e)
    except IndexSetupError as e:
        handle_exception('Database not initialised: %s', e)
