"""
Create statistical summaries command

"""
from __future__ import absolute_import, print_function

import logging
import re
import os
from functools import partial

import click
import dask.array as da
import dask
import pandas as pd
from pandas.tseries.offsets import YearBegin, YearEnd
from pathlib import Path

import datacube
from datacube.api import Tile
from datacube.model.utils import make_dataset, xr_apply, datasets_to_doc
from datacube.storage import netcdf_writer
from datacube.storage.storage import create_netcdf_storage_unit
from datacube.ui import task_app
from datacube.ui.click import to_pathlib


_LOG = logging.getLogger(__name__)


APP_NAME = 'datacube-stacker'


def get_filename(config, cell_index, year):
    file_path_template = str(Path(config['location'], config['file_path_template']))
    return file_path_template.format(tile_index=cell_index, start_time=year)


def _stacked_log_entry(stacked, unstacked):
    if unstacked:
        if stacked:
            msg = ('is already stacked ({stacked}, '
                   'but has new data that needs to be stacked ({unstacked})')
        else:
            msg = 'needs to be stacked'
    else:
        if stacked:
            msg = 'is already stacked'
        else:
            msg = 'is empty'
    return msg


def make_stacker_tasks(index, config, **kwargs):
    filename_timestamp_pattern = r'\/(?:\w+)_-?\d+_-?\d+_(\d+).nc$'  # TODO: Get from config?

    product = config['product']

    query = {kw: arg for kw, arg in kwargs.items() if kw in ['time', 'cell_index'] and arg is not None}

    gw = datacube.api.GridWorkflow(index=index, product=product.name)
    cells = gw.list_cells(product=product.name, **query)
    for (cell_index, tile) in cells.items():
        for (year, year_tile) in _split_by_year(tile):
            stacked, unstacked = _get_stacked_datasets(year_tile, year, filename_timestamp_pattern)
            msg = _stacked_log_entry(stacked, unstacked).format(stacked=len(stacked), unstacked=len(unstacked))
            _LOG.debug('%s, %s: (with %s) %s', cell_index, year, len(year_tile.sources), msg)

            if unstacked:
                year_tile = gw.update_tile_lineage(year_tile)
                output_filename = get_filename(config, cell_index, year)
                yield dict(year=year,
                           tile=year_tile,
                           cell_index=cell_index,
                           output_filename=output_filename,
                           global_attributes=config['global_attributes'],
                           variable_params=config['variable_params'],
                           app_config_file=config['app_config_file']
                          )


def make_stacker_config(index, config, export_path=None, **query):
    config['product'] = index.products.get_by_name(config['output_type'])

    if export_path is not None:
        config['location'] = export_path
        config['index_datasets'] = False
    else:
        config['index_datasets'] = True

    if not os.access(config['location'], os.W_OK):
        _LOG.warning('Current user appears not have write access output location: %s', config['location'])

    chunking = config['storage']['chunking']
    chunking = [chunking[dim] for dim in config['storage']['dimension_order']]

    var_param_keys = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous', 'attrs'}
    variable_params = {}
    for mapping in config['measurements']:
        varname = mapping['name']
        variable_params[varname] = {k: v for k, v in mapping.items() if k in var_param_keys}
        variable_params[varname]['chunksizes'] = chunking

    config['variable_params'] = variable_params

    return config


def _split_by_year(tile, time_dim='time'):
    start_range = tile.sources[time_dim][0].data
    end_range = tile.sources[time_dim][-1].data

    for date in pd.date_range(start=YearBegin(normalize=True).rollback(start_range),
                              end=end_range,
                              freq='AS',
                              normalize=True):
        sources_slice = tile.sources.loc[{time_dim: slice(date, YearEnd(normalize=True).rollforward(date))}]
        year_str = '{0:%Y}'.format(date)
        yield year_str, Tile(sources=sources_slice, geobox=tile.geobox)


def _get_stacked_datasets(tile, stacked_epoch, pattern):
    ds_stacked = set()
    ds_unstacked = set()
    for timeslice in tile.sources:
        ts_sources = timeslice.item()
        if len(ts_sources) != 1:
            raise ValueError('Cannot handle multiple source datasets for a single non-spatial coordinate')
        ds = ts_sources[0]
        if _is_pattern_match(ds.local_path, pattern) == stacked_epoch:
            ds_stacked.add(ds)
        else:
            ds_unstacked.add(ds)
    return ds_stacked, ds_unstacked


def _is_pattern_match(path, pattern):
    result = re.search(pattern, str(path))
    if len(result.groups()) != 1:
        return None
    return result.groups()[0]


def make_datasets(tile, file_path, config):
    def _make_dataset(labels, sources):
        new_dataset = make_dataset(product=tile.product,
                                   sources=sources,
                                   extent=tile.geobox.extent,
                                   center_time=labels['time'],
                                   uri=file_path.absolute().as_uri(),
                                   app_info=get_app_metadata(config),
                                   valid_data=sources[0].extent)
        return new_dataset

    return xr_apply(tile.sources, _make_dataset, dtype='O')


def get_app_metadata(config):
    doc = {
        'lineage': {
            'algorithm': {
                'name': 'datacube-stacker',
                'version': datacube.__version__,
                'repo_url': 'https://github.com/data-cube/agdc-v2.git',
                'parameters': {'configuration_file': config.get('app_config_file', 'unknown')}
            },
        }
    }
    return doc


def _single_dataset(labels, dataset_list):
    return dataset_list[0]


def do_stack_task(task):
    datasets_to_add = None
    datasets_to_update = None
    datasets_to_archive = None

    global_attributes = task['global_attributes']
    variable_params = task['variable_params']

    output_filename = Path(task['output_filename'])
    tile = task['tile']

    if task.get('make_new_datasets', False):
        datasets_to_add = make_datasets(tile, output_filename, task)
        datasets_to_archive = xr_apply(tile.sources, _single_dataset, dtype='O')

        output_datasets = datasets_to_add
    else:
        datasets_to_update = xr_apply(tile.sources, _single_dataset, dtype='O')

        output_datasets = datasets_to_update

    data = datacube.api.GridWorkflow.load(tile, dask_chunks=dict(time=1))  # TODO: chunk along output NetCDF chunk?
    data['dataset'] = datasets_to_doc(output_datasets)

    nco = create_netcdf_storage_unit(output_filename,
                                     data.crs,
                                     data.coords,
                                     data.data_vars,
                                     variable_params,
                                     global_attributes)

    for name, variable in data.data_vars.items():
        try:
            with dask.set_options(get=dask.async.get_sync):
                da.store(variable.data, nco[name], lock=True)
        except ValueError:
            nco[name][:] = netcdf_writer.netcdfy_data(variable.values)
        nco.sync()

    nco.close()
    return datasets_to_add, datasets_to_update, datasets_to_archive


def process_result(index, result):
    datasets_to_add, datasets_to_update, datasets_to_archive = result

    for dataset in datasets_to_add:
        index.datasets.add(dataset, skip_sources=True)
        _LOG.info('Dataset added')

    for dataset in datasets_to_update:
        index.datasets.update(dataset)
        _LOG.info('Dataset updated')

    files_to_archive = set()
    for dataset in datasets_to_archive:
        files_to_archive.add(dataset.local_path)
        index.datasets.archive(dataset.id)

    # for file_path in files_to_archive:
    #     try:
    #         file_path.unlink()
    #     except OSError as e:
    #         _LOG.warning('Could not delete file: %s', e)


def do_nothing(*args, **kwargs):
    pass


@click.command(name=APP_NAME)
@datacube.ui.click.pass_index(app_name=APP_NAME)
@datacube.ui.click.global_cli_options
@click.option('--cell-index', 'cell_index', help='Limit the process to a particular cell (e.g. 14,-11)',
              callback=task_app.validate_cell_index, default=None)
@click.option('--year', 'time', callback=task_app.validate_year, help='Limit the process to a particular year')
@click.option('--export-path', 'export_path',
              help='Write the stacked files to an external location without updating the index',
              default=None,
              type=click.Path(exists=True, writable=True, file_okay=False))
@task_app.queue_size_option
@task_app.task_app_options
@task_app.task_app(make_config=make_stacker_config, make_tasks=make_stacker_tasks)
def main(index, config, tasks, executor, queue_size, **kwargs):
    click.echo('Starting stacking utility...')

    task_func = partial(do_stack_task, config)
    process_func = partial(process_result, index) if config['index_datasets'] else do_nothing
    task_app.run_tasks(tasks, executor, task_func, process_func, queue_size)


if __name__ == '__main__':
    main()
