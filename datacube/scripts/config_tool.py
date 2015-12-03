# coding=utf-8
"""
Configure the Data Cube from the command-line.
"""
from __future__ import absolute_import

import logging
import sys
from pathlib import Path

import click

from datacube import config, ui
from datacube.index import index_connect

CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])

_LOG = logging.getLogger(__name__)


@click.group(help="Configure the Data Cube", context_settings=CLICK_SETTINGS)
@click.option('--verbose', '-v', count=True, help="Use multiple times for more verbosity")
@click.option('--log-queries', is_flag=True, help="Print database queries.")
def cli(verbose, log_queries):
    config.init_logging(verbosity_level=verbose, log_queries=log_queries)


@cli.group()
def database():
    pass


@database.command('init', help='Initialise the database')
@click.option('--no-default-collection', is_flag=True, help="Don't add a default collection.")
def database_init(no_default_collection):
    api = index_connect()

    _LOG.info('Initialising database...')
    was_created = api.init_db(with_default_collection=not no_default_collection)
    if was_created:
        _LOG.info('Done.')
    else:
        _LOG.info('Nothing to do.')


@cli.group(help='Dataset collections')
def collections():
    pass


@collections.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def collection_add(files):
    api = index_connect()

    for descriptor_path, parsed_doc in _read_docs(files):
        api.collections.add(parsed_doc)


@cli.group(help='Storage types')
def storage():
    pass


@storage.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def add_storage(files):
    dm = index_connect()

    for descriptor_path, parsed_doc in _read_docs(files):
        dm.storage_types.add(parsed_doc)


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
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def add_mappings(files):
    dm = index_connect()

    for descriptor_path, parsed_doc in _read_docs(files):
        try:
            dm.mappings.add(parsed_doc)
        except KeyError as ke:
            _LOG.error('Unable to add invalid storage mapping file: %s', descriptor_path)
            _LOG.exception(ke)


@mappings.command('template', help='Print an example YAML template')
def template_mappings():
    sys.stderr.write('TODO: Print an example dataset-storage mapping template\n')


@mappings.command('list')
def list_mappings():
    sys.stderr.write('TODO: list mappings\n')


def _read_docs(paths):
    return ui.read_documents(*(Path(f) for f in paths))


if __name__ == '__main__':
    cli()
