from __future__ import absolute_import

import logging
import click
import cachetools
from copy import deepcopy
from pathlib import Path
from pandas import to_datetime

from datacube.api.core import Datacube
from datacube.model import DatasetType, GeoPolygon
from datacube.model.utils import make_dataset, xr_apply, datasets_to_doc
from datacube.storage.storage import write_dataset_to_netcdf, append_variable_to_netcdf
from datacube.ui import click as ui
from datacube.utils import read_documents, intersect_points

from datacube.ui.click import cli

_LOG = logging.getLogger('agdc-ingest')


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


def get_namemap(config):
    return {spec['src_varname']: spec['name'] for spec in config['measurements']}


def ingest_work(config, source_type, output_type, index, sources, geobox):
    namemap = get_namemap(config)
    measurements = get_measurements(source_type, config)
    variable_params = get_variable_params(config)
    global_attributes = config['global_attributes']

    data = Datacube.product_data(sources, geobox, measurements)
    nudata = data.rename(namemap)
    file_path = Path(get_filename(config, index, sources))

    def _make_dataset(labels, sources):
        assert len(sources) == 1
        valid_data = intersect_points(geobox.extent.points, sources[0].extent.to_crs(geobox.crs).points)
        dataset = make_dataset(dataset_type=output_type,
                               sources=sources,
                               extent=geobox.extent,
                               center_time=labels['time'],
                               uri=file_path.absolute().as_uri(),
                               app_info=get_app_metadata(config, config['filename']),
                               valid_data=GeoPolygon(valid_data, geobox.crs))
        return dataset
    datasets = xr_apply(sources, _make_dataset, dtype='O')
    nudata['dataset'] = datasets_to_doc(datasets)

    write_dataset_to_netcdf(nudata, global_attributes, variable_params, file_path)

    return datasets


@cachetools.cached(cache={}, key=lambda index, id_: id_)
def get_full_lineage(index, id_):
    return index.datasets.get(id_, include_sources=True)


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
    config['filename'] = config_name
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

    def update_sources(labels, sources):
        return tuple(get_full_lineage(index, dataset.id) for dataset in sources)

    if dry_run:
        for task in tasks:
            _LOG.info('Would create %s', get_filename(config, task['index'], task['sources']))
        return

    results = []
    for task in tasks:
        task['sources'] = xr_apply(task['sources'], update_sources, dtype='O')
        results.append(executor.submit(ingest_work,
                                       config=config,
                                       source_type=source_type,
                                       output_type=output_type,
                                       **task))

    failed = 0
    for result in results:
        try:
            datasets = executor.result(result)
            for dataset in datasets.values:
                index.datasets.add(dataset)
        except Exception:  # pylint: disable=broad-except
            _LOG.exception('Task failed')
            failed += 1
            continue

    _LOG.info('%d successful, %d failed', len(tasks)-failed, failed)
