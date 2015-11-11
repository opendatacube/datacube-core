# coding=utf-8
"""
Configure the Data Cube from the command-line.
"""
from __future__ import absolute_import

import logging
import sys

import click
import yaml

from datacube import index

CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(help="Configure the Data Cube", context_settings=CLICK_SETTINGS)
@click.option('--verbose', '-v', count=True, help="Use multiple times for more verbosity")
def cli(verbose):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.WARN)
    logging.getLogger('datacube').setLevel(logging.WARN - 10 * verbose)


@cli.group(help='Storage types')
def storage():
    pass


@storage.command('add')
@click.argument('yaml_file',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def add_storage(yaml_file):
    config = index.data_management_connect()

    for descriptor_path in yaml_file:
        config.ensure_storage_type(_parse_doc(descriptor_path))


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
    config = index.data_management_connect()

    for descriptor_path in yaml_file:
        config.ensure_storage_mapping(_parse_doc(descriptor_path))


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
