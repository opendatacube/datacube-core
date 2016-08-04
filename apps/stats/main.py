from __future__ import absolute_import, print_function

import click
import numpy
from datetime import datetime
from itertools import product

from pandas import to_datetime
from pathlib import Path

from datacube.api import make_mask
from datacube.model import GridSpec, CRS, Coordinate, Variable
from datacube.api.grid_workflow import GridWorkflow
from datacube.ui import click as ui
from datacube.ui.click import to_pathlib
from datacube.utils import read_documents
from datacube.storage.storage import create_netcdf_storage_unit


STANDARD_VARIABLE_PARAM_NAMES = {'zlib',
                                 'complevel',
                                 'shuffle',
                                 'fletcher32',
                                 'contiguous',
                                 'attrs'}


def nco_from_sources(sources, geobox, measurements, variable_params, filename):
    coordinates = {name: Coordinate(coord.values, coord.units)
                   for name, coord in sources.coords.items()}
    coordinates.update(geobox.coordinates)

    variables = {variable['name']: Variable(dtype=numpy.dtype(variable['dtype']),
                                            nodata=variable['nodata'],
                                            dims=sources.dims + geobox.dimensions,
                                            units=variable['units'])
                 for variable in measurements}

    return create_netcdf_storage_unit(filename, geobox.crs, coordinates, variables, variable_params)


def _tuplify(keys, values, defaults):
    assert not set(values.keys()) - set(keys), 'bad keys'
    return tuple(values.get(key, default) for key, default in zip(keys, defaults))


def _slicify(step, size):
    return (slice(i, min(i + step, size)) for i in range(0, size, step))


def block_iter(steps, shape):
    return product(*(_slicify(step, size) for step, size in zip(steps, shape)))


def tile_dims(tile):
    sources = tile['sources']
    geobox = tile['geobox']
    return sources.dims + geobox.dimensions


def tile_shape(tile):
    sources = tile['sources']
    geobox = tile['geobox']
    return sources.shape + geobox.shape


def slice_tile(tile, chunk):
    sources = tile['sources']
    geobox = tile['geobox']
    tile_cpy = tile.copy()
    tile_cpy['sources'] = sources[chunk[:len(sources.shape)]]
    tile_cpy['geobox'] = geobox[chunk[len(sources.shape):]]
    return tile_cpy


def tile_iter(tile, chunk):
    steps = _tuplify(tile_dims(tile), chunk, tile_shape(tile))
    return block_iter(steps, tile_shape(tile))


def get_variable_params(config):
    chunking = config['storage']['chunking']
    chunking = [chunking[dim] for dim in config['storage']['dimension_order']]

    variable_params = {}
    for mapping in config['stats']:
        varname = mapping['name']
        variable_params[varname] = {k: v for k, v in mapping.items() if k in STANDARD_VARIABLE_PARAM_NAMES}
        variable_params[varname]['chunksizes'] = chunking

    return variable_params


def get_filename(path_template, index, sources):
    date_format = '%Y%m%d%H%M%S%f'
    return Path(str(path_template).format(tile_index=index,
                                          start_time=to_datetime(sources.time.values[0]).strftime(date_format),
                                          end_time=to_datetime(sources.time.values[-1]).strftime(date_format)))


def do_stats(task, config):
    source = task['source']
    measurement_name = source['measurements'][0]
    var_params = get_variable_params(config)

    results = create_output_files(config['stats'], config['location'], measurement_name, task, var_params)

    for tile_index in tile_iter(task['data'], {'x': 1000, 'y': 1000}):
        data = GridWorkflow.load(slice_tile(task['data'], tile_index),
                                 measurements=[measurement_name])[measurement_name]
        data = data.where(data != data.attrs['nodata'])

        for spec, sources in zip(source['masks'], task['masks']):
            mask = GridWorkflow.load(slice_tile(sources, tile_index),
                                     measurements=[spec['measurement']])[spec['measurement']]
            mask = make_mask(mask, **spec['flags'])
            data = data.where(mask)
            del mask

        for stat in config['stats']:
            data_stats = getattr(data, stat['name'])(dim='time')
            results[stat['name']][measurement_name][tile_index][0] = data_stats
            print(data_stats)

    for nco in config['stats'].values:
        nco.close()


def create_output_files(stats, output_dir, measurement, task, var_params):
    """
    Create output files and return a map of statistic name to writable NetCDF Dataset
    """
    results = {}
    for stat in stats:
        measurements = [{'name': measurement,
                         'units': '1',  # TODO: where does this come from???
                         'dtype': stat['dtype'],
                         'nodata': stat['nodata']}]

        filename_template = str(Path(output_dir, stat['file_path_template']))
        output_filename = get_filename(filename_template,
                                       task['index'],
                                       task['data']['sources'])
        results[stat['name']] = nco_from_sources(task['data']['sources'],
                                                 task['data']['geobox'],
                                                 measurements,
                                                 {measurement: var_params[stat['name']]},
                                                 output_filename)
    return results


def get_grid_spec(config):
    storage = config['storage']
    crs = CRS(storage['crs'])
    return GridSpec(crs=crs,
                    tile_size=[storage['tile_size'][dim] for dim in crs.dimensions],
                    resolution=[storage['resolution'][dim] for dim in crs.dimensions])


def make_tasks(index, config):
    query = dict(time=(datetime(2011, 1, 1), datetime(2011, 2, 1)))

    workflow = GridWorkflow(index, grid_spec=get_grid_spec(config))

    assert len(config['sources']) == 1  # TODO: merge multiple sources
    for source in config['sources']:
        data = workflow.list_cells(product=source['product'], cell_index=(15, -40), **query)
        masks = [workflow.list_cells(product=mask['product'], cell_index=(15, -40), **query)
                 for mask in source['masks']]

        for key in data.keys():
            yield {
                'source': source,
                'index': key,
                'data': data[key],
                'masks': [mask[key] for mask in masks]
            }


@click.command(name='stats')
@click.option('--app-config', '-c',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
              help='configuration file location', callback=to_pathlib)
@click.option('--year', type=click.IntRange(1960, 2060))
@ui.global_cli_options
@ui.executor_cli_options
@ui.pass_index(app_name='agdc-stats')
def main(index, app_config, year, executor):
    _, config = next(read_documents(app_config))

    tasks = make_tasks(index, config)

    futures = [executor.submit(do_stats, task, config) for task in tasks]

    for future in executor.as_completed(futures):
        result = executor.result(future)
        print(result)


if __name__ == '__main__':
    main()
