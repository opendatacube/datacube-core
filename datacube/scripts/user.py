from __future__ import absolute_import

import os
import base64
import logging
import click

from datacube.ui import click as ui
from datacube.ui.click import cli


_LOG = logging.getLogger('datacube-user')
USER_ROLES = ('user', 'ingest', 'manage', 'admin')


@cli.group(name='user', help='User management commands')
def user_cmd():
    pass


@user_cmd.command('list')
@ui.pass_index()
def list_users(index):
    """
    List users
    """
    for role, user, description in index.list_users():
        click.echo('{0:6}\t{1:15}\t{2}'.format(role, user, description if description else ''))


@user_cmd.command('grant')
@click.argument('role',
                type=click.Choice(USER_ROLES),
                nargs=1)
@click.argument('users', nargs=-1)
@ui.pass_index()
def grant(index, role, users):
    """
    Grant a role to users
    """
    index.grant_role(role, *users)


@cli.command('create')
@click.argument('role',
                type=click.Choice(USER_ROLES), nargs=1)
@click.argument('user', nargs=1)
@ui.pass_index()
@ui.pass_config
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
