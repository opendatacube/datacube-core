# coding=utf-8
"""
Configure the Data Cube from the command-line.
"""
from __future__ import absolute_import

import sys

import click
import yaml

from datacube import index, config

CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(help="Configure the Data Cube", context_settings=CLICK_SETTINGS)
@click.option('--verbose', '-v', count=True, help="Use multiple times for more verbosity")
@click.option('--log-queries', is_flag=True, help="Print database queries.")
def cli(verbose, log_queries):
    config.init_logging(verbosity_level=verbose, log_queries=log_queries)


@cli.group()
def database():
    pass


@database.command('init', help='Initialise the database')
def database_init():
    dm = index.data_management_connect()
    dm.db.init()


@cli.group(help='Storage types')
def storage():
    pass


@storage.command('add')
@click.argument('yaml_file',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def add_storage(yaml_file):
    dm = index.data_management_connect()

    for descriptor_path in yaml_file:
        dm.ensure_storage_type(_parse_doc(descriptor_path))


@storage.command('template', help='Print an example YAML template')
def template_storage():
    sys.stderr.write('TODO: Print an example storage-type template\n')


@storage.command('list')
def list_storage():
    sys.stderr.write('TODO: list storage types\n')


@cli.group(help='Dataset-storage mappings')
def mappings():
    pass


@mappings.command('add')
@click.argument('yaml_file',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def add_mappings(yaml_file):
    dm = index.data_management_connect()

    for descriptor_path in yaml_file:
        dm.ensure_storage_mapping(_parse_doc(descriptor_path))


@mappings.command('template', help='Print an example YAML template')
def template_mappings():
    sys.stderr.write('TODO: Print an example dataset-storage mapping template\n')


@mappings.command('list')
def list_mappings():
    sys.stderr.write('TODO: list mappings\n')


def _parse_doc(file_path):
    return yaml.load(open(file_path, 'r'))


if __name__ == '__main__':
    cli()
