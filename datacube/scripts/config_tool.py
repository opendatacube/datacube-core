#!/usr/bin/env python
# coding=utf-8
"""
Configure the Data Cube from the command-line.
"""
from __future__ import absolute_import

import base64
import logging
import sys
import os

from pathlib import Path
import yaml

import click
from click import echo

from datacube.index import index_connect
from datacube.ui import read_documents
from datacube.ui.click import global_cli_options, pass_index, pass_config, CLICK_SETTINGS

USER_ROLES = ('user', 'ingest', 'manage', 'admin')

_LOG = logging.getLogger(__name__)

PASS_INDEX = pass_index(app_name='datacube-config')


@click.group(help="Configure the Data Cube", context_settings=CLICK_SETTINGS)
@global_cli_options
def cli():
    pass


@cli.group(help='Initialise the database')
def database():
    pass


@database.command('init', help='Initialise the database')
@click.option(
    '--default-collections/--no-default-collections', is_flag=True, default=True,
    help="Add default collections? (default: true)"
)
@click.option(
    '--init-users/--no-init-users', is_flag=True, default=True,
    help="Include user roles and grants. (default: true)"
)
@PASS_INDEX
def database_init(index, default_collections, init_users):
    echo('Initialising database...')
    was_created = index.init_db(with_default_collection=default_collections,
                                with_permissions=init_users)
    if was_created:
        echo('Done.')
    else:
        echo('Updated.')


@cli.group(help='Dataset collections')
def collections():
    pass


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
    #: pylint: disable=broad-except
    except Exception:
        _LOG.exception("Connection error")
        echo('Connection error', file=sys.stderr)
        click.get_current_context().exit(1)


@collections.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@PASS_INDEX
def collection_add(index, files):
    """
    Add collections to the database
    """
    for descriptor_path, parsed_doc in _read_docs(files):
        index.collections.add(parsed_doc)


@collections.command('list')
@PASS_INDEX
def collections_list(index):
    """
    List all collections
    """
    for collection in index.collections.get_all():
        echo("{c.name:15}".format(c=collection))


@cli.group(help='Storage types')
def storage():
    pass


@storage.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@PASS_INDEX
@click.pass_context
def add_storage_types(ctx, index, files):
    """
    Add storage types to the index
    """
    for descriptor_path, parsed_doc in _read_docs(files):
        try:
            index.storage.types.add(parsed_doc)
            echo('Added "%s"' % parsed_doc['name'])
        except KeyError as ke:
            _LOG.exception(ke)
            _LOG.error('Invalid storage type definition: %s', descriptor_path)
            ctx.exit(1)


@storage.command('list')
@PASS_INDEX
def list_storage_types(index):
    """
    List all Storage Types
    """
    for storage_type in index.storage.types.get_all():
        echo("{m.name:20}\t{m.description!s}".format(m=storage_type))


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


@storage.command('print')
@click.argument('storage_arg')
@PASS_INDEX
def print_storage_type(index, storage_arg):
    """
    Print Storage Type in YAML format

    <STORAGE_ARG> may be an id or a name
    """
    try:
        storage_type_id = int(storage_arg)
        storage_type = index.storage.types.get(storage_type_id)
    except ValueError:
        storage_type = index.storage.types.get_by_name(storage_arg)

    if storage_type is not None:
        echo(yaml.dump(storage_type.document))
    else:
        echo("Error: Storage type was found")


def _read_docs(paths):
    return read_documents(*(Path(f) for f in paths))


if __name__ == '__main__':
    cli()
