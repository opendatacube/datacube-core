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


@cli.group()
def storage():
    pass


@storage.command('add')
@click.argument('descriptor',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def add_storage(descriptor):
    config = index.data_management_connect()

    for descriptor_path in descriptor:
        config.ensure_storage_type(_parse_doc(descriptor_path))


def _parse_doc(file_path):
    return yaml.load(open(file_path, 'r'))


@storage.command('list')
def list_storage():
    sys.stderr.write('TODO: list storage types\n')


@cli.group('mappings')
def mappings():
    pass


@mappings.command('add')
@click.argument('descriptor',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def add_mappings(descriptor):
    config = index.data_management_connect()

    for descriptor_path in descriptor:
        config.ensure_storage_mapping(_parse_doc(descriptor_path))


@mappings.command('list')
def list_mappings():
    sys.stderr.write('TODO: list mappings\n')


if __name__ == '__main__':
    cli()
