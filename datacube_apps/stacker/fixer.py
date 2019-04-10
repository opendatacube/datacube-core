"""
Finds single timeslice files that have not been stacked (based on filename), and rewrites them

This tool is used to update NetCDF metadata for files that are not picked up by the stacker

"""

import copy
import datetime
import itertools
import logging
import os
import re
import socket
from functools import partial
from collections import Counter
from typing import List

import click
import dask.array as da
import dask
from dateutil import tz
from pathlib import Path
import pandas as pd
import xarray as xr

import datacube
from datacube.model import Dataset
from datacube.model.utils import xr_apply, datasets_to_doc
from datacube.drivers.netcdf import create_netcdf_storage_unit, netcdf_writer
from datacube.ui import task_app

_LOG = logging.getLogger(__name__)

APP_NAME = 'datacube-fixer'


def make_filename(config, cell_index, start_time):
    file_path_template = str(Path(config['location'], config['file_path_template']))
    return file_path_template.format(tile_index=cell_index, start_time=start_time, version=config['taskfile_utctime'])


def get_temp_file(final_output_path):
    """
    Get a temp file path
    Changes "/path/file.nc" to "/path/.tmp/file.nc.host.pid.tmp"
    :param Path final_output_path:
    :return: Path to temporarily write output
    :rtype: Path
    """

    tmp_folder = final_output_path.parent / '.tmp'
    id_file = '{host}.{pid}'.format(host=socket.gethostname(), pid=os.getpid())
    tmp_path = (tmp_folder / final_output_path.stem).with_suffix(final_output_path.suffix + id_file + '.tmp')
    try:
        tmp_folder.mkdir(parents=True)
    except OSError:
        pass
    if tmp_path.exists():
        tmp_path.unlink()
    return tmp_path


FIND_TIME_RE = re.compile(r'.+_(?P<start_time>\d+)(?:_v\d+)?\.nc\Z')


def get_single_dataset_paths(cell):
    cnt = Counter(ds.local_path for ds in itertools.chain(*cell.sources.values))
    files_to_fix = [local_path for local_path, count in cnt.items()
                    if count == 1 and FIND_TIME_RE.search(str(local_path)).groups()]
    return files_to_fix


def make_fixer_tasks(index, config, time=None, cell_index=None, **kwargs):
    """Find datasets that have a location not shared by other datasets and make it into a task
    """
    gw = datacube.api.GridWorkflow(index=index, product=config['product'].name)

    for query in task_app.break_query_into_years(time):
        cells = gw.list_cells(product=config['product'].name, cell_index=cell_index, **query)

        for cell_index_key, cell in cells.items():
            files_to_fix = get_single_dataset_paths(cell)
            if files_to_fix:
                for cell_time, tile in cell.split('time'):
                    source_path = tile.sources.values.item()[0].local_path
                    if source_path in files_to_fix:
                        tile = gw.update_tile_lineage(tile)
                        start_time = '{0:%Y%m%d%H%M%S%f}'.format(pd.Timestamp(cell_time).to_datetime())
                        output_filename = make_filename(config, cell_index_key, start_time)
                        _LOG.info('Fixing required for: time=%s, cell=%s. Output=%s',
                                  start_time, cell_index_key, output_filename)
                        yield dict(start_time=cell_time,
                                   tile=tile,
                                   cell_index=cell_index_key,
                                   output_filename=output_filename)


def make_fixer_config(index, config, export_path=None, **query):
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

    config['taskfile_utctime'] = int(datacube.utils.datetime_to_seconds_since_1970(datetime.datetime.now()))

    return config


def build_history_string(config, task, keep_original=True):
    tile = task['tile']
    input_path = str(tile.sources[0].item()[0].local_path)
    if keep_original:
        original_dataset = xr.open_dataset(input_path)
        original_history = original_dataset.attrs.get('history', '')
    else:
        original_history = 'Original file at {}'.format(input_path)

    if original_history:
        original_history += '\n'

    new_history = '{dt} {user} {app} ({ver}) {args}  # {comment}'.format(
        dt=datetime.datetime.now(tz.tzlocal()).isoformat(),
        user=os.environ.get('USERNAME') or os.environ.get('USER'),
        app=APP_NAME,
        ver=datacube.__version__,
        args=', '.join([config['app_config_file'],
                        str(config['taskfile_utctime']),
                        task['output_filename'],
                        str(task['start_time']),
                        str(task['cell_index'])
                       ]),
        comment='Updating NetCDF metadata from config file'
    )
    return original_history + new_history


def _unwrap_dataset_list(labels, dataset_list):
    return dataset_list[0]


def do_fixer_task(config, task):
    global_attributes = config['global_attributes']

    # Don't keep the original history if we are trying to fix it
    global_attributes['history'] = build_history_string(config, task, keep_original=False)

    variable_params = config['variable_params']

    output_filename = Path(task['output_filename'])
    output_uri = output_filename.absolute().as_uri()
    temp_filename = get_temp_file(output_filename)
    tile = task['tile']

    # Only use the time chunk size (eg 5), but not spatial chunks
    # This means the file only gets opened once per band, and all data is available when compressing on write
    # 5 * 4000 * 4000 * 2bytes == 152MB, so mem usage is not an issue
    chunk_profile = {'time': config['storage']['chunking']['time']}

    data = datacube.api.GridWorkflow.load(tile, dask_chunks=chunk_profile)

    unwrapped_datasets = xr_apply(tile.sources, _unwrap_dataset_list, dtype='O')
    data['dataset'] = datasets_to_doc(unwrapped_datasets)

    try:
        nco = create_netcdf_storage_unit(temp_filename,
                                         data.crs,
                                         data.coords,
                                         data.data_vars,
                                         variable_params,
                                         global_attributes)
        write_data_variables(data.data_vars, nco)
        nco.close()

        temp_filename.rename(output_filename)

        if config.get('check_data_identical', False):
            new_tile = make_updated_tile(unwrapped_datasets, output_uri, tile.geobox)
            new_data = datacube.api.GridWorkflow.load(new_tile, dask_chunks=chunk_profile)
            check_identical(data, new_data, output_filename)

    except Exception as e:
        if temp_filename.exists():
            temp_filename.unlink()
        raise e

    return unwrapped_datasets, output_uri


def write_data_variables(data_vars, nco):
    for name, variable in data_vars.items():
        try:
            with dask.set_options(get=dask.local.get_sync):
                da.store(variable.data, nco[name], lock=True)
        except ValueError:
            nco[name][:] = netcdf_writer.netcdfy_data(variable.values)
        nco.sync()


def check_identical(data1, data2, output_filename):
    with dask.set_options(get=dask.local.get_sync):
        if not all((data1 == data2).all().values()):
            _LOG.error("Mismatch found for %s, not indexing", output_filename)
            raise ValueError("Mismatch found for %s, not indexing" % output_filename)
    return True


def make_updated_tile(old_datasets, new_uri, geobox):
    def update_dataset_location(labels, dataset: Dataset) -> List[Dataset]:
        new_dataset = copy.copy(dataset)
        new_dataset.uris = [new_uri]
        return [new_dataset]

    updated_datasets = xr_apply(old_datasets, update_dataset_location, dtype='O')
    return datacube.api.Tile(sources=updated_datasets, geobox=geobox)


def process_result(index, result):
    datasets, new_uri = result
    for dataset in datasets.values:
        _LOG.info('Updating dataset location: %s', dataset.local_path)
        old_uri = dataset.local_uri
        index.datasets.add_location(dataset.id, new_uri)
        index.datasets.archive_location(dataset.id, old_uri)


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
@task_app.task_app(make_config=make_fixer_config, make_tasks=make_fixer_tasks)
def fixer(index, config, tasks, executor, queue_size, **kwargs):
    """This script rewrites unstacked dataset files to correct their NetCDF metadata."""
    click.echo('Starting fixer utility...')

    task_func = partial(do_fixer_task, config)
    process_func = partial(process_result, index) if config['index_datasets'] else None
    task_app.run_tasks(tasks, executor, task_func, process_func, queue_size)


if __name__ == '__main__':
    fixer()
