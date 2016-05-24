#!/usr/bin/env python
# coding=utf-8
"""
Configure the Data Cube from the command-line.
"""
from __future__ import absolute_import

import base64
import logging
import os
from pathlib import Path

import click
from click import echo
from sqlalchemy.exc import OperationalError

from datacube.index import index_connect
from datacube.index.postgres._api import IndexSetupError
from datacube.ui import read_documents
from datacube.ui.click import global_cli_options, pass_index, pass_config, CLICK_SETTINGS

USER_ROLES = ('user', 'ingest', 'manage', 'admin')

_LOG = logging.getLogger(__name__)

PASS_INDEX = pass_index(app_name='datacube-config')
PASS_RAW_INDEX = pass_index(app_name='datacube-config', expect_initialised=False)


@click.group(help="Configure the Data Cube", context_settings=CLICK_SETTINGS)
@global_cli_options
def cli():
    pass


@cli.group(help='Initialise the database')
def database():
    pass


@database.command('init', help='Initialise the database')
@click.option(
    '--default-types/--no-default-types', is_flag=True, default=True,
    help="Add default types? (default: true)"
)
@click.option(
    '--init-users/--no-init-users', is_flag=True, default=True,
    help="Include user roles and grants. (default: true)"
)
@PASS_RAW_INDEX
def database_init(index, default_types, init_users):
    echo('Initialising database...')
    was_created = index.init_db(with_default_types=default_types,
                                with_permissions=init_users)
    if was_created:
        echo('Done.')
    else:
        echo('Updated.')


@cli.command('check')
@pass_config
def check(config_file):
    """
    Verify & view current configuration
    """
    echo('Host: {}:{}'.format(config_file.db_hostname or 'localhost', config_file.db_port or '5432'))
    echo('Database: {}'.format(config_file.db_database))
    echo('User: {}'.format(config_file.db_username))

    echo('\n')
    echo('Attempting connect')
    try:
        index_connect(local_config=config_file)
        echo('Success.')
    except OperationalError as e:
        echo("Unable to connect to database: %s" % e)
        click.get_current_context().exit(1)
    except IndexSetupError as e:
        echo("Database not initialised: %s" % e)
        click.get_current_context().exit(1)


@cli.group(name='type', help='Dataset types')
def dataset_type():
    pass


@dataset_type.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@PASS_INDEX
@click.pass_context
def add_dataset_types(ctx, index, files):
    """
    Add storage types to the index
    """
    for descriptor_path, parsed_doc in _read_docs(files):
        try:
            index.datasets.types.add_document(parsed_doc)
            echo('Added "%s"' % parsed_doc['name'])
        except KeyError as ke:
            _LOG.exception(ke)
            _LOG.error('Invalid dataset type definition: %s', descriptor_path)
            ctx.exit(1)


@cli.command('grant')
@click.argument('role',
                type=click.Choice(USER_ROLES),
                nargs=1)
@click.argument('users', nargs=-1)
@PASS_INDEX
def grant(index, role, users):
    """
    Grant a role to users
    """
    index.grant_role(role, *users)


@cli.command('create')
@click.argument('role',
                type=click.Choice(USER_ROLES), nargs=1)
@click.argument('user', nargs=1)
@PASS_INDEX
@pass_config
def create_user(config, index, role, user):
    """
    Create a User
    """
    key = base64.urlsafe_b64encode(os.urandom(12)).decode('utf-8')
    index.create_user(user, key, role)

    click.echo('{host}:{port}:*:{username}:{key}'.format(
        host=config.db_hostname or 'localhost',
        port=config.db_port,
        username=user,
        key=key
    ))


@cli.command('users')
@PASS_INDEX
def list_users(index):
    """
    List users
    """
    for role, user, description in index.list_users():
        click.echo('{0:6}\t{1:15}\t{2}'.format(role, user, description if description else ''))


def _read_docs(paths):
    return read_documents(*(Path(f) for f in paths))


if __name__ == '__main__':
    cli()
