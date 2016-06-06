from __future__ import absolute_import

import logging
import uuid

import click
import numpy
from copy import deepcopy
from collections import OrderedDict
from pathlib import Path
from pandas import to_datetime
from rasterio.coords import BoundingBox

from datacube.api.core import Datacube
from datacube.model import DatasetType, Dataset, GeoBox, GeoPolygon, CRS
from datacube.storage.storage import write_dataset_to_netcdf
from datacube.ui import click as ui
from datacube.utils import read_documents

from datacube.ui.click import cli
import yaml
try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper

_LOG = logging.getLogger('agdc-ingest')


def set_geobox_info(doc, crs, extent):
    bb = extent.boundingbox
    gp = GeoPolygon([(bb.left, bb.top), (bb.right, bb.top), (bb.right, bb.bottom), (bb.left, bb.bottom)],
                    crs).to_crs(CRS('EPSG:4326'))
    doc.update({
        'extent': {
            'coord': {
                'ul': {'lon': gp.points[0][0], 'lat': gp.points[0][1]},
                'ur': {'lon': gp.points[1][0], 'lat': gp.points[1][1]},
                'lr': {'lon': gp.points[2][0], 'lat': gp.points[2][1]},
                'll': {'lon': gp.points[3][0], 'lat': gp.points[3][1]},
            }
        },
        'grid_spatial': {
            'projection': {
                'spatial_reference': str(crs),
                'geo_ref_points': {
                    'ul': {'x': bb.left, 'y': bb.top},
                    'ur': {'x': bb.right, 'y': bb.top},
                    'll': {'x': bb.left, 'y': bb.bottom},
                    'lr': {'x': bb.right, 'y': bb.bottom},
                }
            }
        }
    })


def generate_dataset(data, sources, prod_info, uri):
    nudata = data.copy()

    datasets = []
    for idx, (time, sources) in enumerate(zip(sources.time.values, sources.values)):
        document = {
            'id': str(uuid.uuid4()),
            'image': {
                'bands': {name: {'path': '', 'layer': name} for name in nudata.data_vars}
            },
            'lineage': {'source_datasets': {str(idx): dataset.metadata_doc for idx, dataset in enumerate(sources)}}
        }
        # TODO: extent is a bad thing to store - it duplicates coordinates
        set_geobox_info(document, data.crs, data.extent)
        document['extent']['from_dt'] = str(time)
        document['extent']['to_dt'] = str(time)
        document['extent']['center_dt'] = str(time)
        document.update(prod_info.metadata)
        dataset = Dataset(prod_info,
                          document,
                          local_uri=uri,
                          sources={str(idx): dataset for idx, dataset in enumerate(sources)})
        datasets.append(dataset)
    nudata['dataset'] = (['time'],
                         numpy.array([yaml.dump(dataset.metadata_doc, Dumper=SafeDumper, encoding='utf-8')
                                      for dataset in datasets], dtype='S'))
    return nudata, datasets


def write_product(data, sources, output_prod_info, global_attrs, var_params, path):
    nudata, nudatasets = generate_dataset(data, sources, output_prod_info, path.absolute().as_uri())
    write_dataset_to_netcdf(nudata, global_attrs, var_params, path)
    return nudatasets


def find_diff(input_type, output_type, bbox, datacube):
    from datacube.api.grid_workflow import GridWorkflow
    workflow = GridWorkflow(datacube, output_type.grid_spec)

    tiles_in = workflow.cell_observations(product=input_type.name)
    tiles_out = workflow.cell_observations(product=output_type.name)

    tasks = []
    for tile_index in set(tiles_in.keys()) | set(tiles_out.keys()):
        sources_in = datacube.product_sources(tiles_in.get(tile_index, []),
                                              lambda ds: ds.center_time,
                                              'time',
                                              'seconds since 1970-01-01 00:00:00')

        sources_out = datacube.product_sources(tiles_out.get(tile_index, []),
                                               lambda ds: ds.center_time,
                                               'time',
                                               'seconds since 1970-01-01 00:00:00')

        diff = numpy.setdiff1d(sources_in.time.values, sources_out.time.values)
        tasks += [(tile_index, sources_in.sel(time=[v])) for v in diff]
    return tasks


def do_work(tasks, work_func, index, executor):
    results = []
    for tile_index, groups in tasks:
        results.append(executor.submit(work_func, tile_index, groups))

    for result in results:
        # TODO: try/catch
        datasets = executor.result(result)

        for dataset in datasets:
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


def get_measurements(source_type, config):
    def merge_measurement(measurement, spec):
        measurement.update({k: spec.get(k, measurement[k]) for k in ('nodata', 'dtype')})
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
    source_type = index.datasets.types.get_by_name(config['source_type'])
    if not source_type:
        _LOG.error("Source DatasetType %s does not exist", config['source_type'])

    output_type = morph_dataset_type(source_type, config)
    _LOG.info('Created DatasetType %s', output_type.name)
    output_type = index.datasets.types.add(output_type)

    datacube = Datacube(index=index)

    grid_spec = output_type.grid_spec
    namemap = get_namemap(config)
    measurements = get_measurements(source_type, config)
    variable_params = get_variable_params(config)
    file_path_template = str(Path(config['location'], config['file_path_template']))

    bbox = BoundingBox(**config['ingestion_bounds'])
    tasks = find_diff(source_type, output_type, bbox, datacube)

    def ingest_work(tile_index, sources):
        geobox = GeoBox.from_grid_spec(grid_spec, tile_index)
        data = Datacube.product_data(sources, geobox, measurements)

        nudata = data.rename(namemap)

        file_path = file_path_template.format(tile_index=tile_index,
                                              start_time=to_datetime(sources.time.values[0]).strftime('%Y%m%d%H%M%S%f'),
                                              end_time=to_datetime(sources.time.values[-1]).strftime('%Y%m%d%H%M%S%f'))
        # TODO: algorithm params
        nudatasets = write_product(nudata, sources, output_type,
                                   config['global_attributes'], variable_params, Path(file_path))
        return nudatasets

    do_work(tasks, ingest_work, index, executor)
