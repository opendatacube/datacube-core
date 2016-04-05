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
from datacube.storage import tile_datasets_with_storage_type

PASS_INDEX = ui.pass_index(app_name='datacube-ingest')


@click.group(help="Data Management Tool", context_settings=CLICK_SETTINGS)
@ui.global_cli_options
def cli():
    pass


@cli.command('stack', help='Stack storage units')
@ui.executor_cli_options
@click.argument('types', nargs=-1)
@PASS_INDEX
def stack(index, executor, types):
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
        for storage_unit in index.storage.search(type=storage_type.id, time=Range(start_date, start_date + period)):
            storage_units_by_tile_index.setdefault(storage_unit.tile_index, []).append(storage_unit)

        for tile_index, storage_units in storage_units_by_tile_index.items():
            if len(storage_units) < 2:
                continue

            storage_units.sort(key=lambda su: su.coordinates['time'].begin)
            filename = storage_type.generate_uri(tile_index=tile_index,
                                                 start_time=start_date.strftime(date_format),
                                                 end_time=(start_date + period).strftime(date_format))
            yield (storage_units, filename)
        start_date += period


def _do_stack(task):
    storage_units, filename = task
    return [stack_storage_units(storage_units, filename)]


@cli.command('check', help='Check database consistency')
@click.option('--check-index', is_flag=True, default=True, help="check that datasets have all possible storage unit")
@click.option('--check-storage', is_flag=True, default=True, help="check that storage units have valid filepaths")
@click.option('--check-overlaps', is_flag=True, default=False, help="check that storage units don't overlap (long)")
@click.argument('types', nargs=-1)
@PASS_INDEX
def check(index, check_index, check_storage, check_overlaps, types):
    if not types:
        storage_types = index.storage.types.get_all()
    else:
        storage_types = [index.storage.types.get_by_name(name) for name in types]

    for storage_type in storage_types:
        click.echo('Checking %s' % storage_type.name)
        if check_overlaps:
            click.echo('Overlaps. Might take REALLY long time...')
            try:
                overlaps = list(index.storage.get_overlaps(storage_type))
                click.echo('%s overlaping storage units' % len(overlaps))
            except DBAPIError:
                click.echo('Failed to get overlaps! cube extension might not be loaded')

        if check_index:
            missing_units = 0
            datasets = index.datasets.search_by_metadata(storage_type.document['match']['metadata'])
            for dataset in datasets:
                tiles = tile_datasets_with_storage_type([dataset], storage_type)
                storage_units = index.storage.search(index.datasets.get_field('id') == dataset.id)
                for storage_unit in storage_units:
                    tiles.pop(storage_unit.tile_index)

                # if tiles:
                #     click.echo('%s missing units %s' % (dataset, tiles.keys()))

                missing_units += len(tiles)
            click.echo('%s missing storage units' % missing_units)

        if check_storage:
            missing_files = 0
            for storage_unit in index.storage.search(type=storage_type.id):
                if not storage_unit.local_path.exists():
                    missing_files += 1
            click.echo('%s missing storage unit files' % missing_files)


@cli.command('ingest', help="Ingest datasets into the Data Cube.")
@ui.executor_cli_options
@click.option('--no-storage', is_flag=True, help="Don't create storage units")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@PASS_INDEX
def ingest(index, executor, datasets, no_storage):
    indexed_datasets = []
    for dataset_path in datasets:
        indexed_datasets += index_datasets(Path(dataset_path), index=index)

    if not no_storage:
        store_datasets(indexed_datasets, index=index, executor=executor)


if __name__ == '__main__':
    cli()
