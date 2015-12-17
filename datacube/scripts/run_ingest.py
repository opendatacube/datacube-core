#!/usr/bin/env python
# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

from pathlib import Path

import click

from datacube import index, ui
from datacube.ingest import index_datasets, store_datasets

CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(help="Ingest datasets into the Data Cube.", context_settings=CLICK_SETTINGS)
@ui.common_cli_options
@click.option('--no-storage', is_flag=True, help="Don't create storage units")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def cli(datasets, no_storage):
    indexed_datasets = []
    i = index.index_connect()
    for dataset_path in datasets:
        indexed_datasets += index_datasets(Path(dataset_path), index=i)

    if not no_storage:
        store_datasets(indexed_datasets, index=i)


if __name__ == '__main__':
    cli()
