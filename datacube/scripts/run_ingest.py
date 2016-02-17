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

from sqlalchemy.exc import DBAPIError

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
    executor = get_executor(scheduler, workers)

    if not types:
        storage_types = index.storage.types.get_all()
    else:
        storage_types = [index.storage.types.get_by_name(name) for name in types]

    tasks = []
    for storage_type in storage_types:
        # TODO: figure out start date - search ordered by time, get first, round down
        start_date = datetime(1986, 1, 1, tzinfo=tzutc())
        # TODO: figure out end date - now, round down
        end_date = datetime(2016, 1, 1, tzinfo=tzutc())
        tasks += list(_stack_storage_type(storage_type, start_date, end_date, index))

    stacked = executor.map(_do_stack, tasks)
    for (storage_units, filename), stacked in zip(tasks, (executor.result(s) for s in stacked)):
        index.storage.replace(storage_units, stacked)
        for storage_unit in storage_units:
            os.unlink(str(storage_unit.local_path))


def _stack_storage_type(storage_type, start_date, end_date, index):
    period, date_format = {
        'year': (relativedelta(years=1), '%Y'),
        'month': (relativedelta(months=1), '%Y%m'),
    }[storage_type.aggregation_period]
    # TODO: order by time will remove the need to run multiple searches
    while start_date < end_date:
        storage_units_by_tile_index = {}
        for storage_unit in index.storage.search(type=storage_type.id_, time=Range(start_date, start_date + period)):
            storage_units_by_tile_index.setdefault(storage_unit.tile_index, []).append(storage_unit)

        for tile_index, storage_units in storage_units_by_tile_index.items():
            if len(storage_units) < 2:
                continue

            filename = storage_type.generate_uri(tile_index=tile_index,
                                                 start_time=start_date.strftime(date_format),
                                                 end_time=(start_date + period).strftime(date_format))
            yield (storage_units, filename)
        start_date += period


def _do_stack(task):
    storage_units, filename = task
    return [stack_storage_units(storage_units, filename)]


@cli.command('check', help='Check database consistency')
@click.argument('types', nargs=-1)
@ui.pass_index
def check(index, types):
    if not types:
        storage_types = index.storage.types.get_all()
    else:
        storage_types = [index.storage.types.get_by_name(name) for name in types]

    for storage_type in storage_types:
        try:
            overlaps = list(index.storage.get_overlaps(storage_type))
            click.echo('%s: %s overlaping storage units' % (storage_type.name, len(overlaps)))
        except DBAPIError:
            click.echo('Failed to get overlaps! cube extension is, probably, not loaded')
            break

    for storage_type in storage_types:
        click.echo('%s: %s missing storage units' % (storage_type.name, 'TODO'))
        # TODO: find missing storage units


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
