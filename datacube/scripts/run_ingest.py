#!/usr/bin/env python
# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import
import os
from datetime import datetime

from pathlib import Path
from dateutil.tz import tzutc
from dateutil.relativedelta import relativedelta
import click

from datacube.ui import click as ui
from datacube.ui.click import CLICK_SETTINGS
from datacube.model import Range
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

    tasks = []
    for storage_type in storage_types:
        start_date = datetime(1970, 1, 1, tzinfo=tzutc())
        end_date = datetime(2016, 1, 1, tzinfo=tzutc())
        period = relativedelta(months=1)
        while start_date < end_date:
            storage_units_by_tile_index = {}
            for storage_unit in index.storage.search(type=storage_type.id_, time=Range(start_date, start_date+period)):
                storage_units_by_tile_index.setdefault(storage_unit.tile_index, []).append(storage_unit)

            for tile_index, storage_units in storage_units_by_tile_index.items():
                if len(storage_units) < 2:
                    continue

                filename = storage_type.generate_uri(tile_index=tile_index,
                                                     start_time=start_date.strftime('%Y%m'),
                                                     end_time=(start_date+period).strftime('%Y%m'))
                tasks.append((storage_units, filename))
            start_date = start_date + period

    for storage_units, filename in tasks:
        index.storage.replace(storage_units, [stack_storage_units(storage_units, filename)])
        for storage_unit in storage_units:
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
