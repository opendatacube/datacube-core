#!/usr/bin/env python
# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import
import os
from itertools import chain

from pathlib import Path

import click

from datacube.ui import click as ui
from datacube.ui.click import CLICK_SETTINGS
from datacube.ingest import index_datasets, store_datasets
from datacube.storage.storage import stack_storage_units

from datacube.executor import get_executor


def parse_endpoint(ctx, param, value):
    if not value:
        return None

    try:
        ip, port = tuple(value.split(':'))
        return ip, int(port)
    except ValueError:
        ctx.fail('%s is not a valid endpoint' % value)


@click.group(help="Data Management Tool", context_settings=CLICK_SETTINGS)
@ui.global_cli_options
def cli():
    pass


@cli.command('stack', help='Stack storage units')
@click.option('--scheduler', callback=parse_endpoint)
@click.option('--workers', default=0)
@click.argument('types', nargs=-1)
@ui.pass_index
def stack(index, types, workers, scheduler):
    if not types:
        storage_types = index.storage.types.get_all()
    else:
        storage_types = [index.storage.types.get_by_name(name) for name in types]

    for storage_type in storage_types:
        storage_units_by_tile_index = {}
        for storage_unit in index.storage.search(type=storage_type.id_):
            storage_units_by_tile_index.setdefault(storage_unit.tile_index, []).append(storage_unit)

        if not storage_units_by_tile_index:
            continue

        stacked_storage_units = [
            stack_storage_units(storage_units, storage_type.resolve_location(os.urandom(16).encode('hex')+'.nc'))
            for storage_units in storage_units_by_tile_index.values()
            ]
        old_storage_units = list(chain.from_iterable(storage_units_by_tile_index.values()))

        index.storage.replace(old_storage_units, stacked_storage_units)

        for storage_unit in old_storage_units:
            os.unlink(str(storage_unit.local_path))


@cli.command('ingest', help="Ingest datasets into the Data Cube.")
@click.option('--scheduler', callback=parse_endpoint)
@click.option('--workers', default=0)
@click.option('--no-storage', is_flag=True, help="Don't create storage units")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@ui.pass_index
def ingest(index, datasets, workers, scheduler, no_storage):
    indexed_datasets = []
    for dataset_path in datasets:
        indexed_datasets += index_datasets(Path(dataset_path), index=index)

    executor = get_executor(scheduler, workers)
    if not no_storage:
        store_datasets(indexed_datasets, index=index, executor=executor)


if __name__ == '__main__':
    cli()
