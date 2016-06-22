from __future__ import absolute_import

import logging
import click
from copy import deepcopy
from pathlib import Path
from pandas import to_datetime

from datacube.api.core import Datacube
from datacube.model import DatasetType
from datacube.model.utils import generate_dataset, append_datasets_to_data, xr_iter, datasets_to_doc
from datacube.storage.storage import write_dataset_to_netcdf, append_variable_to_netcdf
from datacube.ui import click as ui
from datacube.utils import read_documents

from datacube.ui.click import cli

_LOG = logging.getLogger('agdc-ingest')


def write_product(data, sources, output_dataset_type, app_metadata, global_attrs, var_params, path):
    datasets = generate_dataset(data.extent, sources, output_dataset_type, path.absolute().as_uri(), app_metadata)
    data = append_datasets_to_data(data, datasets)
    write_dataset_to_netcdf(data, global_attrs, var_params, path)
    return datasets


def find_diff(input_type, output_type, index):
    from datacube.api.grid_workflow import GridWorkflow
    workflow = GridWorkflow(index, output_type.grid_spec)

    tiles_in = workflow.list_tiles(product=input_type.name)
    tiles_out = workflow.list_tiles(product=output_type.name)

    def update_dict(d, **kwargs):
        result = d.copy()
        result.update(kwargs)
        return result

    tasks = [update_dict(tile, index=key) for key, tile in tiles_in.items() if key not in tiles_out]
    return tasks


def morph_dataset_type(source_type, config):
    output_type = DatasetType(source_type.metadata_type, deepcopy(source_type.definition))
    output_type.definition['name'] = config['output_type']
    output_type.definition['managed'] = True
    output_type.definition['description'] = config['description']
    output_type.definition['storage'] = config['storage']
    output_type.metadata['format'] = {'name': 'NetCDF'}

    def merge_measurement(measurement, spec):
        measurement.update({k: spec.get(k, measurement[k]) for k in ('name', 'nodata', 'dtype')})
        return measurement

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
                'version': config.get('version', 'unknown'),
                'repo_url': 'https://github.com/GeoscienceAustralia/datacube-ingester.git',
                'parameters': {'configuration_file': config_file}
            },
        }
    }
    return doc


def get_filename(config, index, sources):
    file_path_template = str(Path(config['location'], config['file_path_template']))
    return file_path_template.format(tile_index=index,
                                     start_time=to_datetime(sources.time.values[0]).strftime('%Y%m%d%H%M%S%f'),
                                     end_time=to_datetime(sources.time.values[-1]).strftime('%Y%m%d%H%M%S%f'))


def get_measurements(source_type, config):
    def merge_measurement(measurement, spec):
        measurement.update({k: spec.get(k) or measurement[k] for k in ('nodata', 'dtype', 'resampling_method')})
        return measurement

    return [merge_measurement(source_type.measurements[spec['src_varname']].copy(), spec)
            for spec in config['measurements']]


def ingest_work(config, source_type, index, sources, geobox):
    namemap = get_namemap(config)
    measurements = get_measurements(source_type, config)
    variable_params = get_variable_params(config)
    global_attributes = config['global_attributes']

    data = Datacube.product_data(sources, geobox, measurements)
    nudata = data.rename(namemap)

    file_path = get_filename(config, index, sources)
    write_dataset_to_netcdf(nudata, global_attributes, variable_params, Path(file_path))

    return file_path


def get_namemap(config):
    return {spec['src_varname']: spec['name'] for spec in config['measurements']}


@cli.command('ingest', help="Ingest datasets")
@click.option('--config', '-c',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
              required=True,
              help='Ingest configuration file')
@ui.executor_cli_options
@click.option('--dry-run', '-d', is_flag=True, default=False, help='Check if everything is ok')
@ui.pass_index(app_name='agdc-ingest')
def ingest_cmd(index, config, dry_run, executor):
    config_name = Path(config).name
    _, config = next(read_documents(Path(config)))
    source_type = index.products.get_by_name(config['source_type'])
    if not source_type:
        _LOG.error("Source DatasetType %s does not exist", config['source_type'])
        return 1

    output_type = morph_dataset_type(source_type, config)
    _LOG.info('Created DatasetType %s', output_type.name)
    # TODO: don't add output_type in dry_run mode?
    output_type = index.products.add(output_type)

    tasks = find_diff(source_type, output_type, index)
    _LOG.info('%s tasks discovered', len(tasks))

    if dry_run:
        for task in tasks:
            file_path = get_filename(config, task['index'], task['sources'])
            _LOG.info('Would create %s', file_path)
        return

    results = []
    for task in tasks:
        results.append(executor.submit(ingest_work, config=config, source_type=source_type, **task))

    for task, result in zip(tasks, results):
        try:
            file_path = executor.result(result)
        except Exception:  # pylint: disable=broad-except
            _LOG.exception('Task failed')
            continue

        datasets = generate_dataset(task['geobox'].extent,
                                    task['sources'],
                                    output_type,
                                    Path(file_path).absolute().as_uri(),
                                    get_app_metadata(config, config_name))

        for idx, _, dataset in xr_iter(datasets):
            dataset = index.datasets.add(dataset)
            datasets[idx] = index.datasets.get(dataset.id, include_sources=True)

        append_variable_to_netcdf(file_path, 'dataset', datasets_to_doc(datasets), zlib=True)

