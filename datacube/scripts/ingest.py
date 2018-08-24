from __future__ import absolute_import

import time
import logging
import click
import cachetools
import itertools
import sys
from copy import deepcopy
from pathlib import Path
from pandas import to_datetime
from datetime import datetime

import datacube
from datacube.api.core import Datacube
from datacube.index.index import Index
from datacube.model import DatasetType, Range, GeoPolygon, Measurement
from datacube.model.utils import make_dataset, xr_apply, datasets_to_doc
from datacube.ui import click as ui
from datacube.utils import read_documents
from datacube.ui.task_app import check_existing_files, load_tasks as load_tasks_, save_tasks as save_tasks_
from datacube.drivers import storage_writer_by_name

from datacube.ui.click import cli

_LOG = logging.getLogger('datacube-ingest')

FUSER_KEY = 'fuse_data'


def find_diff(input_type, output_type, index, **query):
    from datacube.api.grid_workflow import GridWorkflow
    workflow = GridWorkflow(index, output_type.grid_spec)

    tiles_in = workflow.list_tiles(product=input_type.name, **query)
    tiles_out = workflow.list_tiles(product=output_type.name, **query)

    tasks = [{'tile': tile, 'tile_index': key} for key, tile in tiles_in.items() if key not in tiles_out]
    return tasks


def morph_dataset_type(source_type, config, index, storage_format):
    output_metadata_type = source_type.metadata_type
    if 'metadata_type' in config:
        output_metadata_type = index.metadata_types.get_by_name(config['metadata_type'])

    output_type = DatasetType(output_metadata_type, deepcopy(source_type.definition))
    output_type.definition['name'] = config['output_type']
    output_type.definition['managed'] = True
    output_type.definition['description'] = config['description']
    output_type.definition['storage'] = {k: v for (k, v) in config['storage'].items()
                                         if k in ('crs', 'tile_size', 'resolution', 'origin')}

    output_type.metadata_doc['format'] = {'name': storage_format}

    if 'metadata_type' in config:
        output_type.definition['metadata_type'] = config['metadata_type']

    def merge_measurement(measurement, spec):
        measurement.update({k: spec.get(k, measurement[k]) for k in ('name', 'nodata', 'dtype')})
        return Measurement(**measurement)

    output_type.definition['measurements'] = [merge_measurement(output_type.measurements[spec['src_varname']], spec)
                                              for spec in config['measurements']]
    return output_type


def get_variable_params(config):
    chunking = config['storage']['chunking']
    chunking = [chunking[dim] for dim in config['storage']['dimension_order']]

    variable_params = {}
    for mapping in config['measurements']:
        varname = mapping['name']
        variable_params[varname] = {k: v for k, v in mapping.items() if k in {'zlib',
                                                                              'complevel',
                                                                              'shuffle',
                                                                              'fletcher32',
                                                                              'contiguous',
                                                                              'attrs'}}
        variable_params[varname]['chunksizes'] = chunking

    return variable_params


def get_app_metadata(config, config_file):
    doc = {
        'lineage': {
            'algorithm': {
                'name': 'datacube-ingest',
                'repo_url': 'https://github.com/opendatacube/datacube-core.git',
                'parameters': {'configuration_file': config_file}
            },
        }
    }
    return doc


def get_filename(config, tile_index, sources, **kwargs):
    file_path_template = str(Path(config['location'], config['file_path_template']))
    time_format = '%Y%m%d%H%M%S%f'
    return Path(file_path_template.format(
        tile_index=tile_index,
        start_time=to_datetime(sources.time.values[0]).strftime(time_format),
        end_time=to_datetime(sources.time.values[-1]).strftime(time_format),
        version=config['taskfile_version'],
        **kwargs))


def get_measurements(source_type, config):
    def merge_measurement(measurement, spec):
        measurement.update({k: spec.get(k) or measurement[k] for k in ('nodata', 'dtype', 'resampling_method')})
        return Measurement(**measurement)

    return [merge_measurement(source_type.measurements[spec['src_varname']].copy(), spec)
            for spec in config['measurements']]


def get_namemap(config):
    return {spec['src_varname']: spec['name'] for spec in config['measurements']}


def ensure_output_type(index, config, storage_format, allow_product_changes=False):
    # type: (Index, dict, bool) -> (DatasetType, DatasetType)
    """
    Create the output product for the given ingest config if it doesn't already exist.

    It will throw a ValueError if the config already exists but differs from the existing.
    Set allow_product_changes=True to allow changes.
    """
    source_type = index.products.get_by_name(config['source_type'])
    if not source_type:
        click.echo("Source DatasetType %s does not exist" % config['source_type'])
        click.get_current_context().exit(1)

    output_type = morph_dataset_type(source_type, config, index, storage_format)
    _LOG.info('Created DatasetType %s', output_type.name)

    existing = index.products.get_by_name(output_type.name)
    if existing:
        can_update, safe_changes, unsafe_changes = index.products.can_update(output_type)
        if safe_changes or unsafe_changes:
            if not allow_product_changes:
                raise ValueError("Ingest config differs from the existing output product, "
                                 "but allow_product_changes=False")
            output_type = index.products.update(output_type)
        else:
            output_type = existing
    else:
        output_type = index.products.add(output_type)

    return source_type, output_type


@cachetools.cached(cache={}, key=lambda index, id_: id_)
def get_full_lineage(index, id_):
    return index.datasets.get(id_, include_sources=True)


def load_config_from_file(path):
    config_file = Path(path)
    _, config = next(read_documents(config_file))
    config['filename'] = str(config_file.absolute())

    return config


def create_task_list(index, output_type, year, source_type, config):
    config['taskfile_version'] = int(time.time())

    query = {}
    if year:
        query['time'] = Range(datetime(year=year[0], month=1, day=1), datetime(year=year[1] + 1, month=1, day=1))
    if 'ingestion_bounds' in config:
        bounds = config['ingestion_bounds']
        query['x'] = Range(bounds['left'], bounds['right'])
        query['y'] = Range(bounds['bottom'], bounds['top'])

    tasks = find_diff(source_type, output_type, index, **query)
    _LOG.info('%s tasks discovered', len(tasks))

    def check_valid(tile, tile_index):
        if FUSER_KEY in config:
            return True

        require_fusing = [source for source in tile.sources.values if len(source) > 1]
        if require_fusing:
            _LOG.warning('Skipping %s - no "%s" specified in config: %s', tile_index, FUSER_KEY, require_fusing)

        return not require_fusing

    def update_sources(sources):
        return tuple(get_full_lineage(index, dataset.id) for dataset in sources)

    def update_task(task):
        tile = task['tile']
        for i in range(tile.sources.size):
            tile.sources.values[i] = update_sources(tile.sources.values[i])
        return task

    tasks = (update_task(task) for task in tasks if check_valid(**task))
    return tasks


def ingest_work(config, source_type, output_type, tile, tile_index):
    # pylint: disable=too-many-locals
    _LOG.info('Starting task %s', tile_index)
    driver = storage_writer_by_name(config['storage']['driver'])

    if driver is None:
        _LOG.error('Failed to load storage driver %s', config['storage']['driver'])
        raise ValueError('Something went wrong: no longer can find driver pointed by storage.driver option')

    namemap = get_namemap(config)
    measurements = get_measurements(source_type, config)
    variable_params = get_variable_params(config)
    global_attributes = config['global_attributes']

    with datacube.set_options(reproject_threads=1):
        fuse_func = {'copy': None}[config.get(FUSER_KEY, 'copy')]
        data = Datacube.load_data(tile.sources, tile.geobox, measurements, fuse_func=fuse_func)

    nudata = data.rename(namemap)
    file_path = get_filename(config, tile_index, tile.sources)

    def mk_uri(file_path):
        if driver.uri_scheme == "file":
            return file_path.absolute().as_uri()
        return '{}://{}'.format(driver.uri_scheme, file_path)

    def _make_dataset(labels, sources):
        return make_dataset(product=output_type,
                            sources=sources,
                            extent=tile.geobox.extent,
                            center_time=labels['time'],
                            uri=mk_uri(file_path),
                            app_info=get_app_metadata(config, config['filename']),
                            valid_data=GeoPolygon.from_sources_extents(sources, tile.geobox))

    datasets = xr_apply(tile.sources, _make_dataset, dtype='O')  # Store in Dataarray to associate Time -> Dataset
    nudata['dataset'] = datasets_to_doc(datasets)

    variable_params['dataset'] = {
        'chunksizes': (1,),
        'zlib': True,
        'complevel': 9,
    }

    storage_metadata = driver.write_dataset_to_storage(nudata, file_path,
                                                       global_attributes=global_attributes,
                                                       variable_params=variable_params,
                                                       storage_config=config['storage'])

    if (storage_metadata is not None) and len(storage_metadata) > 0:
        datasets.attrs['storage_metadata'] = storage_metadata

    _LOG.info('Finished task %s', tile_index)

    return datasets


def _index_datasets(index, results):
    n = 0
    for datasets in results:
        extra_args = {}
        # datasets is an xarray.DataArray
        if 'storage_metadata' in datasets.attrs:
            extra_args['storage_metadata'] = datasets.attrs['storage_metadata']

        for dataset in datasets.values:
            index.datasets.add(dataset, with_lineage=False, **extra_args)
            n += 1
    return n


def process_tasks(index, config, source_type, output_type, tasks, queue_size, executor):
    # pylint: disable=too-many-locals
    def submit_task(task):
        _LOG.info('Submitting task: %s', task['tile_index'])
        return executor.submit(ingest_work,
                               config=config,
                               source_type=source_type,
                               output_type=output_type,
                               **task)

    pending = []

    # Count of storage unit/s indexed successfully or failed to index
    index_successful = index_failed = 0

    # Count of storage unit/s failed during file creation
    f_failed = 0

    tasks = iter(tasks)
    pending += [submit_task(task) for task in itertools.islice(tasks, max(0, queue_size - len(pending)))]
    total = pending
    while pending:
        completed, failed, pending = executor.get_ready(pending)

        for future in failed:
            try:
                executor.result(future)
            except Exception as err:  # pylint: disable=broad-except
                _LOG.exception('Failed to create storage unit file (Exception: %s) ', str(err))
                f_failed += 1

        _LOG.info('Storage unit file creation status (completed: %s, failed: %s, pending: %s)',
                  (len(total) - len(pending) - f_failed),
                  f_failed,
                  len(pending))
        if not completed:
            time.sleep(1)
            continue

        try:
            # TODO: ideally we wouldn't block here indefinitely
            # maybe limit gather to 50-100 results and put the rest into a index backlog
            # this will also keep the queue full
            results = executor.results(completed)
            index_successful += _index_datasets(index, results)
        except Exception as err:  # pylint: disable=broad-except
            _LOG.exception('Failed to index storage unit file (Exception: %s)', str(err))
            index_failed += 1

        _LOG.info('Storage unit files indexed (successful: %s, failed: %s)', index_successful, index_failed)

    return index_successful, index_failed


def _validate_year(ctx, param, value):
    try:
        if value is None:
            return None
        years = list(map(int, value.split('-', 2)))
        if len(years) == 1:
            return years[0], years[0]
        return tuple(years)
    except ValueError:
        raise click.BadParameter('year must be specified as a single year (eg 1996) '
                                 'or as an inclusive range (eg 1996-2001)')


def get_driver_from_config(config):
    driver_name = config['storage']['driver']
    driver = storage_writer_by_name(driver_name)
    if driver is None:
        click.echo('Failed to load requested storage driver: ' + driver_name)
        sys.exit(2)
    return driver


@cli.command('ingest', help="Ingest datasets")
@click.option('--config-file', '-c',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
              help='Ingest configuration file')
@click.option('--year', callback=_validate_year, help='Limit the process to a particular year')
@click.option('--queue-size', type=click.IntRange(1, 100000), default=3200, help='Task queue size')
@click.option('--save-tasks', help='Save tasks to the specified file',
              type=click.Path(exists=False))
@click.option('--load-tasks', help='Load tasks from the specified file',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False))
@click.option('--dry-run', '-d', is_flag=True, default=False, help='Check if everything is ok')
@click.option('--allow-product-changes', is_flag=True, default=False,
              help='Allow the output product definition to be updated if it differs.')
@ui.executor_cli_options
@ui.pass_index(app_name='datacube-ingest')
def ingest_cmd(index,
               config_file,
               year,
               queue_size,
               save_tasks,
               load_tasks,
               dry_run,
               executor,
               allow_product_changes):
    # pylint: disable=too-many-locals

    if config_file:
        config = load_config_from_file(config_file)
        driver = get_driver_from_config(config)
        source_type, output_type = ensure_output_type(index, config, driver.format,
                                                      allow_product_changes=allow_product_changes)

        tasks = create_task_list(index, output_type, year, source_type, config)
    elif load_tasks:
        config, tasks = load_tasks_(load_tasks)
        driver = get_driver_from_config(config)
        source_type, output_type = ensure_output_type(index, config, driver.format,
                                                      allow_product_changes=allow_product_changes)
    else:
        click.echo('Must specify exactly one of --config-file, --load-tasks')
        sys.exit(-1)

    if dry_run:
        check_existing_files(get_filename(config, task['tile_index'], task['tile'].sources) for task in tasks)
    elif save_tasks:
        save_tasks_(config, tasks, save_tasks)
    else:
        successful, failed = process_tasks(index, config, source_type, output_type, tasks, queue_size, executor)
        click.echo('%d successful, %d failed' % (successful, failed))

        sys.exit(failed)
