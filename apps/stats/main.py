from __future__ import absolute_import, print_function

import click
import numpy
from datetime import datetime
from itertools import product

from pandas import to_datetime
from pathlib import Path

from datacube.api import make_mask
from datacube.model import GridSpec, CRS, Coordinate, Variable
from datacube.model.utils import make_dataset, datasets_to_doc, xr_apply
from datacube.api.grid_workflow import GridWorkflow
from datacube.ui import click as ui
from datacube.ui.click import to_pathlib
from datacube.utils import read_documents, unsqueeze_data_array
from datacube.storage.storage import create_netcdf_storage_unit
from datacube.storage import netcdf_writer
from datacube.storage.masking import mask_valid_data as mask_invalid_data


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


def get_filename(path_template, index, start_time):
    date_format = '%Y%m%d'
    return Path(str(path_template).format(tile_index=index,
                                          start_time=start_time.strftime(date_format)))


def create_storage_unit(config, task, stat):
    def _make_dataset(labels, sources):
        dataset = make_dataset(dataset_type=config['products'][stat['name']],
                               sources=sources,
                               extent=task['data']['geobox'].extent,
                               center_time=labels['time'],
                               uri=None,  # TODO:
                               app_info=None,
                               valid_data=None)
        return dataset

    source = task['source']

    sources = task['data']['sources'].sum()
    for spec, mask_tile in zip(source['masks'], task['masks']):
        sources += mask_tile['sources'].sum()
    sources = unsqueeze_data_array(sources, 'time', 0, task['start_time'], task['data']['sources'].time.attrs)

    var_params = get_variable_params(config)

    measurements = list(config['products'][stat['name']].measurements.values())
    filename_template = str(Path(config['location'], stat['file_path_template']))
    output_filename = get_filename(filename_template,
                                   task['index'],
                                   task['start_time'])
    nco = nco_from_sources(sources,
                           task['data']['geobox'],
                           measurements,
                           {measurement['name']: var_params[stat['name']] for measurement in measurements},
                           output_filename)
    datasets = xr_apply(sources, _make_dataset, dtype='O')  # Store in Dataarray to associate Time -> Dataset
    datasets = datasets_to_doc(datasets)
    netcdf_writer.create_variable(nco, 'dataset', datasets, zlib=True)
    nco['dataset'][:] = netcdf_writer.netcdfy_data(datasets.values)
    return nco


def do_stats(task, config):
    source = task['source']

    results = {}
    for stat in config['stats']:
        results[stat['name']] = create_storage_unit(config, task, stat)

    for tile_index in tile_iter(task['data'], {'x': 1000, 'y': 1000}):
        data = GridWorkflow.load(slice_tile(task['data'], tile_index),
                                 measurements=task['source']['measurements'])
        data = mask_invalid_data(data)

        for spec, mask_tile in zip(source['masks'], task['masks']):
            mask = GridWorkflow.load(slice_tile(mask_tile, tile_index),
                                     measurements=[spec['measurement']])[spec['measurement']]
            mask = make_mask(mask, **spec['flags'])
            data = data.where(mask)
            del mask

        for stat in config['stats']:
            data_stats = getattr(data, stat['name'])(dim='time')
            for name, var in data_stats.data_vars.items():
                results[stat['name']][name][(0,) + tile_index[1:]] = var.values  # HACK: make netcdf slicing nicer?...

    for stat, nco in results.items():
        nco.close()


def get_grid_spec(config):
    storage = config['storage']
    crs = CRS(storage['crs'])
    return GridSpec(crs=crs,
                    tile_size=[storage['tile_size'][dim] for dim in crs.dimensions],
                    resolution=[storage['resolution'][dim] for dim in crs.dimensions])


def make_tasks(index, config):
    start_time = datetime(2011, 1, 1)
    end_time = datetime(2011, 2, 1)
    query = dict(time=(start_time, end_time))

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
                'masks': [mask[key] for mask in masks],
                'start_time': start_time,
                'end_time': end_time
            }


def make_products(index, config):
    results = {}
    for stat in config['stats']:
        name = stat['name']
        definition = {
            'name': name,
            'description': name,
            'metadata_type': 'eo',
            'metadata': {
                'format': 'NetCDF',
                'product_type': name,
            },
            'storage': config['storage'],
            'measurements': [
                {
                    'name': measurement,
                    'dtype': stat['dtype'],
                    'nodata': stat['nodata'],
                    'units': '1'
                }
                for measurement in config['sources'][0]['measurements']  # TODO: multiple source products
            ]
        }
        results[name] = index.products.from_doc(definition)
    return results


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

    config['products'] = make_products(index, config)
    tasks = make_tasks(index, config)

    futures = [executor.submit(do_stats, task, config) for task in tasks]

    for future in executor.as_completed(futures):
        result = executor.result(future)
        print(result)


if __name__ == '__main__':
    main()
