from __future__ import absolute_import

import logging
import click
from copy import deepcopy
from pathlib import Path
from pandas import to_datetime
from rasterio.coords import BoundingBox

from datacube.api.core import Datacube
from datacube.model import DatasetType
from datacube.model.utils import generate_dataset, append_datasets_to_data, xr_iter, merge
from datacube.storage.storage import write_dataset_to_netcdf
from datacube.ui import click as ui
from datacube.utils import read_documents

from datacube.ui.click import cli

_LOG = logging.getLogger('agdc-ingest')


def write_product(data, sources, output_dataset_type, app_metadata, global_attrs, var_params, path):
    datasets = generate_dataset(data, sources, output_dataset_type, path.absolute().as_uri(), app_metadata)
    data = append_datasets_to_data(data, datasets)
    write_dataset_to_netcdf(data, global_attrs, var_params, path)
    return datasets


def find_diff(input_type, output_type, bbox, index):
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


def do_work(tasks, work_func, index, executor):
    results = []
    for task in tasks:
        results.append(executor.submit(work_func, **task))

    for result in results:
        # TODO: try/catch
        datasets = executor.result(result)

        for i, labels, dataset in xr_iter(datasets):
            index.datasets.add(dataset)


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


def get_app_metadata(config):
    doc = {
        'lineage': {
            'algorithm': {
                'name': 'ingest',
                'version': '1.0'
            },
            # 'machine': {
            #     'software_versions': {
            #         'ingester':
            #     }
            # }
        }
    }
    if 'app_metadata' in config:
        merge(doc, config['app_metadata'])
    return doc


def get_measurements(source_type, config):
    def merge_measurement(measurement, spec):
        measurement.update({k: spec.get(k) or measurement[k] for k in ('nodata', 'dtype', 'resampling_method')})
        return measurement

    return [merge_measurement(source_type.measurements[spec['src_varname']].copy(), spec)
            for spec in config['measurements']]


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
    _, config = next(read_documents(Path(config)))
    source_type = index.products.get_by_name(config['source_type'])
    if not source_type:
        _LOG.error("Source DatasetType %s does not exist", config['source_type'])

    output_type = morph_dataset_type(source_type, config)
    _LOG.info('Created DatasetType %s', output_type.name)
    output_type = index.products.add(output_type)

    app_metadata = get_app_metadata(config)

    namemap = get_namemap(config)
    measurements = get_measurements(source_type, config)
    variable_params = get_variable_params(config)
    file_path_template = str(Path(config['location'], config['file_path_template']))

    bbox = BoundingBox(**config['ingestion_bounds'])
    tasks = find_diff(source_type, output_type, bbox, index)

    def ingest_work(index, sources, geobox):
        data = Datacube.product_data(sources, geobox, measurements)

        nudata = data.rename(namemap)

        file_path = file_path_template.format(tile_index=index,
                                              start_time=to_datetime(sources.time.values[0]).strftime('%Y%m%d%H%M%S%f'),
                                              end_time=to_datetime(sources.time.values[-1]).strftime('%Y%m%d%H%M%S%f'))
        nudatasets = write_product(nudata, sources, output_type, app_metadata,
                                   config['global_attributes'], variable_params, Path(file_path))
        return nudatasets

    do_work(tasks, ingest_work, index, executor)
