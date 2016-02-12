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

from datacube.executor import get_executor


def parse_endpoint(ctx, param, value):
    if not value:
        return None

    try:
        ip, port = tuple(value.split(':'))
        return ip, int(port)
    except ValueError:
        ctx.fail('%s is not a valid endpoint' % value)


@click.command(help="Ingest datasets into the Data Cube.", context_settings=CLICK_SETTINGS)
@ui.global_cli_options
@click.option('--scheduler', callback=parse_endpoint)
@click.option('--workers', default=0)
@click.option('--no-storage', is_flag=True, help="Don't create storage units")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index
def cli(index, datasets, workers, scheduler, no_storage):
    indexed_datasets = []
    for dataset_path in datasets:
        indexed_datasets += index_datasets(Path(dataset_path), index=index)

    executor = get_executor(scheduler, workers)
    if not no_storage:
        store_datasets(indexed_datasets, index=index, executor=executor)


if __name__ == '__main__':
    cli()
