
from __future__ import absolute_import, print_function

import click
import numpy
from datetime import datetime
from pathlib import Path
from itertools import product

from datacube.api import make_mask
from datacube.model import GridSpec, CRS, Coordinate, Variable
from datacube.api.grid_workflow import GridWorkflow
from datacube.ui import click as ui
from datacube.utils import read_documents
from datacube.storage import netcdf_writer


class StorageUnit(object):
    def __init__(self, filename, crs, coordinates, variables, variable_params):
        if filename.exists():
            raise RuntimeError('Storage Unit already exists: %s' % filename)

        try:
            filename.parent.mkdir(parents=True)
        except OSError:
            pass

        self._nco = netcdf_writer.create_netcdf(str(filename))

        for name, coord in coordinates.items():
            netcdf_writer.create_coordinate(self._nco, name, coord.labels, coord.units)

        netcdf_writer.create_grid_mapping_variable(self._nco, crs)

        for name, variable in variables.items():
            var_params = variable_params.get(name, {})
            data_var = netcdf_writer.create_variable(self._nco, name, variable, **var_params)

    def __del__(self):
        self._nco.close()

    @classmethod
    def from_sources(cls, sources, geobox, type_, variable_params, filename):
        coordinates = {name: Coordinate(coord.values, coord.units) for name, coord in sources.coords.items()}
        coordinates.update(geobox.coordinates)

        variables = {name: Variable(numpy.dtype(variable['dtype']),
                                    variable['nodata'],
                                    type_.dimensions,
                                    variable['units']) for name, variable in type_.measurements.items()}

        return cls(filename, geobox.crs, coordinates, variables, variable_params)

    def __setitem__(self, key, value):
        for name, var in value.data_vars.items():
            self.write_var(key, name, var)
            # self._nco.sync()

    def write_var(self, key, name, var):
        dst = self._nco[name]
        index = tuple(key.get(dim, slice(None)) for dim in dst.dimensions)
        dst[index] = var.values


def _tuplify(keys, values, defaults):
    assert not set(values.keys()) - set(keys), 'bad keys'
    return tuple(values.get(key, default) for key, default in zip(keys, defaults))


def _slicify(step, size):
    return (slice(i, min(i+step, size)) for i in range(0, size, step))


def block_iter(steps, shape):
    return product(*(_slicify(step, size) for step, size in zip(steps, shape)))


def tile_dims(tile):
    sources = tile['sources']
    geobox = tile['geobox']
    return sources.dims + geobox.dimensions


def tile_shape(tile):
    sources = tile['sources']
    geobox = tile['geobox']
    return sources.shape+geobox.shape


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


def do_stats(task, config):
    source = task['source']
    measurement = source['measurements'][0]

    for index in tile_iter(task['data'], {'x': 1000, 'y': 1000}):
        data = GridWorkflow.load(slice_tile(task['data'], index), measurements=[measurement])[measurement]
        data = data.where(data != data.attrs['nodata'])

        for spec, sources in zip(source['masks'], task['masks']):
            mask = GridWorkflow.load(slice_tile(sources, index),
                                     measurements=[spec['measurement']])[spec['measurement']]
            mask = make_mask(mask, **spec['flags'])
            data = data.where(mask)
            del mask

        for stat in config['stats']:
            print(getattr(data, stat['name'])(dim='time'))


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

        tasks = [{
            'source': source,
            'index': key,
            'data': data[key],
            'masks': [mask[key] for mask in masks]
        } for key in data.keys()]

    return tasks


@click.command(name='stats')
@click.option('--app-config', '-c',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
              help='configuration file location')
@click.option('--year', type=click.IntRange(1960, 2060))
@ui.global_cli_options
@ui.executor_cli_options
@ui.pass_index(app_name='agdc-stats')
def main(index, app_config, year, executor):
    _, config = next(read_documents(Path(app_config)))

    tasks = make_tasks(index, config)

    futures = [executor.submit(do_stats, task, config) for task in tasks]
    for future in executor.as_completed(futures):
        result = executor.result(future)
        print(result)


if __name__ == '__main__':
    main()
