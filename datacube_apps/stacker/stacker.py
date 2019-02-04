"""
Create time-stacked NetCDF files

"""

import copy
import datetime
import itertools
import logging
import os
import socket
from functools import partial
from typing import List

import click
import dask.array as da
import dask
from dateutil import tz
from pathlib import Path

import datacube
from datacube.model import Dataset
from datacube.model.utils import xr_apply, datasets_to_doc
from datacube.utils import mk_part_uri
from datacube.drivers.netcdf import create_netcdf_storage_unit, netcdf_writer
from datacube.ui import task_app

_LOG = logging.getLogger(__name__)

APP_NAME = 'datacube-stacker'


def get_filename(config, cell_index, year):
    file_path_template = str(Path(config['location'], config['file_path_template']))
    return file_path_template.format(tile_index=cell_index, start_time=year, version=config['taskfile_utctime'])


def get_temp_file(final_output_path) -> Path:
    """
    Get a temp file path
    Changes "/path/file.nc" to "/path/.tmp/file.nc.host.pid.tmp"
    :param Path final_output_path:
    :return: Path to temporarily write output
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


def make_stacker_tasks(index, config, cell_index=None, time=None, **kwargs):
    gw = datacube.api.GridWorkflow(index=index, product=config['product'].name)

    for query in task_app.break_query_into_years(time):
        cells = gw.list_cells(product=config['product'].name, cell_index=cell_index, **query)
        for cell_index_key, tile in cells.items():
            for year, year_tile in tile.split_by_time(freq='A'):
                storage_files = set(ds.local_path for ds in itertools.chain(*year_tile.sources.values))
                if len(storage_files) > 1:
                    year_tile = gw.update_tile_lineage(year_tile)
                    output_filename = get_filename(config, cell_index_key, year)
                    _LOG.info('Stacking required for: year=%s, cell=%s. Output=%s',
                              year, cell_index_key, output_filename)
                    yield dict(year=year,
                               tile=year_tile,
                               cell_index=cell_index_key,
                               output_filename=output_filename)
                elif len(storage_files) == 1:
                    [only_filename] = storage_files
                    _LOG.info('Stacking not required for: year=%s, cell=%s. existing=%s',
                              year, cell_index_key, only_filename)


def make_stacker_config(index, config, export_path=None, check_data=None, **query):
    config['product'] = index.products.get_by_name(config['output_type'])

    if export_path is not None:
        config['location'] = export_path
        config['index_datasets'] = False
    else:
        config['index_datasets'] = True

    if check_data is not None:
        config['check_data_identical'] = check_data

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


def get_history_attribute(config, task):
    return '{dt} {user} {app} ({ver}) {args}  # {comment}'.format(
        dt=datetime.datetime.now(tz.tzlocal()).isoformat(),
        user=os.environ.get('USERNAME') or os.environ.get('USER'),
        app=APP_NAME,
        ver=datacube.__version__,
        args=', '.join([config['app_config_file'],
                        str(config['taskfile_utctime']),
                        task['output_filename'],
                        str(task['year']),
                        str(task['cell_index'])
                       ]),
        comment='Stacking datasets for a year into a single NetCDF file'
    )


def _unwrap_dataset_list(labels, dataset_list):
    return dataset_list[0]


def do_stack_task(config, task):
    global_attributes = config['global_attributes']
    global_attributes['history'] = get_history_attribute(config, task)

    variable_params = config['variable_params']

    variable_params['dataset'] = {
        'chunksizes': (1,),
        'zlib': True,
        'complevel': 9,
    }

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
    _LOG.debug('Verifying file: "%s"', output_filename)
    with dask.set_options(get=dask.local.get_sync):
        if not all((data1 == data2).all().values()):
            _LOG.error("Mismatch found for %s, not indexing", output_filename)
            raise ValueError("Mismatch found for %s, not indexing" % output_filename)
    return True


def make_updated_tile(old_datasets, new_uri, geobox):
    def update_dataset_location(idx, labels, dataset: Dataset) -> List[Dataset]:
        idx, = idx
        new_dataset = copy.copy(dataset)
        new_dataset.uris = [mk_part_uri(new_uri, idx)]
        return [new_dataset]

    updated_datasets = xr_apply(old_datasets, update_dataset_location, with_numeric_index=True)
    return datacube.api.Tile(sources=updated_datasets, geobox=geobox)


def process_result(index, result):
    datasets, new_common_uri = result
    for idx, dataset in enumerate(datasets.values):
        new_uri = mk_part_uri(new_common_uri, idx)
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
@click.option('--check-data/--no-check-data', is_flag=True, default=None,
              help="Overrides config option: check_data_identical")
@task_app.queue_size_option
@task_app.task_app_options
@task_app.task_app(make_config=make_stacker_config, make_tasks=make_stacker_tasks)
def main(index, config, tasks, executor, queue_size, **kwargs):
    """Store datasets into NetCDF files containing an entire year in the same file.

    - Uses the same configuration format as the `ingest` tool.
    - However, does not create new datasets, but instead updates dataset locations then archives the original location.
    """
    click.echo('Starting stacking utility...')

    task_func = partial(do_stack_task, config)
    process_func = partial(process_result, index) if config['index_datasets'] else None
    task_app.run_tasks(tasks, executor, task_func, process_func, queue_size)


if __name__ == '__main__':
    main()
