
import logging
import click
import csv
import sys
import yaml
import yaml.resolver

from collections import OrderedDict

from datacube.utils import gen_password
from datacube.config import LocalConfig
from datacube.index.index import Index
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.utils.serialise import SafeDatacubeDumper

_LOG = logging.getLogger('datacube-user')
USER_ROLES = ('user', 'ingest', 'manage', 'admin')


@cli.group(name='user', help='User management commands')
def user_cmd():
    pass


def build_user_list(index):
    lstdct = []
    for role, user, description in index.users.list_users():
        info = OrderedDict((
            ('role', role),
            ('user', user),
            ('description', description)
        ))
        lstdct.append(info)
    return lstdct


def _write_csv(index):
    writer = csv.DictWriter(sys.stdout, ['role', 'user', 'description'], extrasaction='ignore')
    writer.writeheader()

    def add_first_role(row):
        roles_ = row['role']
        row['role'] = roles_ if roles_ else None
        return row

    writer.writerows(add_first_role(row) for row in index)


def _write_yaml(index):
    """
    Dump yaml data with support for OrderedDicts.

    Allows for better human-readability of output: such as dataset ID field first, sources last.

    (Ordered dicts are output identically to normal yaml dicts: their order is purely for readability)
    """

    return yaml.dump_all(index, sys.stdout, SafeDatacubeDumper, default_flow_style=False, indent=4)


_OUTPUT_WRITERS = {
    'csv': _write_csv,
    'yaml': _write_yaml,
}


@user_cmd.command('list')
@click.option('-f', help='Output format',
              type=click.Choice(list(_OUTPUT_WRITERS)), default='yaml', show_default=True)
@ui.pass_index()
def list_users(index, f):
    """
    List users
    """
    _OUTPUT_WRITERS[f](build_user_list(index))


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
    index.users.grant_role(role, *users)


@user_cmd.command('create')
@click.argument('role',
                type=click.Choice(USER_ROLES), nargs=1)
@click.argument('user', nargs=1)
@click.option('--description')
@ui.pass_index()
@ui.pass_config
def create_user(config, index, role, user, description):
    # type: (LocalConfig, Index, str, str, str) -> None
    """
    Create a User
    """
    password = gen_password(12)
    index.users.create_user(user, password, role, description=description)

    click.echo('{host}:{port}:*:{username}:{password}'.format(
        host=config.get('db_hostname', None) or 'localhost',
        port=config.get('db_port', None),
        username=user,
        password=password
    ))


@user_cmd.command('delete')
@click.argument('users', nargs=-1)
@ui.pass_index()
@ui.pass_config
def delete_user(config, index, users):
    """
    Delete a User
    """
    index.users.delete_user(*users)
