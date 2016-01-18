#!/usr/bin/env python
# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

from pathlib import Path

import click

from datacube.ui import click as ui
from datacube.ui.click import CLICK_SETTINGS
from datacube.ingest import index_datasets, store_datasets


@click.command(help="Ingest datasets into the Data Cube.", context_settings=CLICK_SETTINGS)
@ui.global_cli_options
@click.option('--workers', default=0)
@click.option('--no-storage', is_flag=True, help="Don't create storage units")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index
def cli(index, datasets, workers, no_storage):
    indexed_datasets = []
    for dataset_path in datasets:
        indexed_datasets += index_datasets(Path(dataset_path), index=index)

    if not no_storage:
        store_datasets(indexed_datasets, index=index, workers=workers)


if __name__ == '__main__':
    cli()
