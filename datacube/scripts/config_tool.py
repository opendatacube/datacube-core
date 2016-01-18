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
from click import echo

from datacube.index import index_connect
from datacube.ui import read_documents
from datacube.ui.click import global_cli_options, pass_index, pass_config, CLICK_SETTINGS


_LOG = logging.getLogger(__name__)


@click.group(help="Configure the Data Cube", context_settings=CLICK_SETTINGS)
@global_cli_options
def cli():
    pass


@cli.group(help='Initialise the database')
def database():
    pass


@database.command('init', help='Initialise the database')
@click.option('--no-default-collection', is_flag=True, help="Don't add a default collection.")
@pass_index
def database_init(index, no_default_collection):
    echo('Initialising database...')
    was_created = index.init_db(with_default_collection=not no_default_collection)
    if was_created:
        echo('Done.')
    else:
        echo('Nothing to do.')


@cli.group(help='Dataset collections')
def collections():
    pass


@cli.command('check', help='Verify & view current configuration.')
@pass_config
def check(config):
    echo('Host: {}:{}'.format(config.db_hostname or 'localhost', config.db_port or '5432'))
    echo('Database: {}'.format(config.db_database))
    echo('User: {}'.format(config.db_username))

    echo('\n')
    echo('Attempting connect')
    try:
        index_connect(local_config=config)
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
@pass_index
def collection_add(index, files):
    for descriptor_path, parsed_doc in _read_docs(files):
        index.collections.add(parsed_doc)


@cli.group(help='Dataset-storage mappings')
def mappings():
    pass


@mappings.command('add')
@click.argument('files',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@pass_index
@click.pass_context
def add_mappings(ctx, index, files):
    for descriptor_path, parsed_doc in _read_docs(files):
        try:
            index.mappings.add(parsed_doc)
        except KeyError as ke:
            _LOG.exception(ke)
            _LOG.error('Invalid mapping document: %s', descriptor_path)
            ctx.exit(1)


@mappings.command('list')
@pass_index
def list_mappings(index):
    for mapping in index.mappings.get_all():
        echo("{m.id_:2d}. {m.name:15}: {m.description!s}".format(m=mapping))


def _read_docs(paths):
    return read_documents(*(Path(f) for f in paths))


if __name__ == '__main__':
    cli()
