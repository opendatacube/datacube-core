#!/usr/bin/env python
# coding=utf-8
"""
Configure the Data Cube from the command-line.
"""
from __future__ import absolute_import

import logging
import os
import sys
from pathlib import Path

import click

from datacube.ui import click as ui
from datacube.index import index_connect

CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])

_LOG = logging.getLogger(__name__)


@click.group(help="Configure the Data Cube", context_settings=CLICK_SETTINGS)
@ui.global_cli_options
def cli():
    pass


@cli.group(help='Initialise the database')
def database():
    pass


@database.command('init', help='Initialise the database')
@click.option('--no-default-collection', is_flag=True, help="Don't add a default collection.")
@ui.pass_index
def database_init(index, no_default_collection):

    _LOG.info('Initialising database...')
    was_created = index.init_db(with_default_collection=not no_default_collection)
    if was_created:
        _LOG.info('Done.')
    else:
        _LOG.info('Nothing to do.')


@cli.group(help='Dataset collections')
def collections():
    pass


@cli.command('check', help='Verify & view current configuration.')
@ui.pass_config
def check(config):
    _LOG.info('Host: %s:%s', config.db_hostname or 'localhost', config.db_port or '5432')
    _LOG.info('Database: %s', config.db_database)
    # Windows users need to use py3 for getlogin().
    _LOG.info('User: %s', config.db_username or os.getlogin())

    _LOG.info('\n')
    _LOG.info('Attempting connect')
    try:
        index_connect(local_config=config)
        _LOG.info('Success.')
    #: pylint: disable=broad-except
    except Exception:
        _LOG.exception('Connection error')


@collections.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index
def collection_add(index, files):

    for descriptor_path, parsed_doc in _read_docs(files):
        index.collections.add(parsed_doc)


@cli.group(help='Storage types')
def storage():
    pass


@storage.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index
def add_storage(index, files):

    for descriptor_path, parsed_doc in _read_docs(files):
        index.storage_types.add(parsed_doc)


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
@ui.pass_index
def add_mappings(index, files):

    for descriptor_path, parsed_doc in _read_docs(files):
        try:
            index.mappings.add(parsed_doc)
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
