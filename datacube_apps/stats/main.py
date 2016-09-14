"""
Create statistical summaries command

"""

from __future__ import absolute_import, print_function

from collections import namedtuple, OrderedDict
from functools import reduce as reduce_
from itertools import product
from pathlib import Path

import click
import numpy
import xarray
from pandas import to_datetime

from datacube.api import make_mask
from datacube.api.grid_workflow import GridWorkflow
from datacube.model import GridSpec, CRS, Coordinate, Variable
from datacube.model.utils import make_dataset, datasets_to_doc, xr_apply
from datacube.storage import netcdf_writer
from datacube.storage.masking import mask_valid_data as mask_invalid_data
from datacube.storage.storage import create_netcdf_storage_unit
from datacube.ui import click as ui
from datacube.ui.click import to_pathlib
from datacube.utils import read_documents, unsqueeze_data_array
from datacube.utils.dates import date_sequence

from .statistics import apply_cross_measurement_reduction

STANDARD_VARIABLE_PARAM_NAMES = {'zlib',
                                 'complevel',
                                 'shuffle',
                                 'fletcher32',
                                 'contiguous',
                                 'attrs'}

StatAlgorithm = namedtuple('StatAlgorithm', ['dtype', 'nodata', 'units', 'masked', 'compute'])
Stat = namedtuple('Stat', ['product', 'algorithm', 'definition'])


class Lambda(object):
    def __init__(self, function, **kwargs):
        self.function = function
        self.kwargs = kwargs

    def __call__(self, x):
        return getattr(x, self.function)(dim='time', **self.kwargs)


class Ident(object):
    def __init__(self, x):
        self.x = x

    def __call__(self, x):
        return self.x


STAT_FUNCS = {
    'mean': Lambda('mean'),
    'median': Lambda('median'),
    'medoid': apply_cross_measurement_reduction,
    'percentile_10': Lambda('reduce', func=numpy.nanpercentile, q=10),
    'percentile_50': Lambda('reduce', func=numpy.nanpercentile, q=50),
    'percentile_90': Lambda('reduce', func=numpy.nanpercentile, q=90),
}


def make_stat_metadata(definition):
    return StatAlgorithm(
        dtype=Ident(definition['dtype']),
        nodata=definition['nodata'],
        units='1',
        masked=True,
        compute=STAT_FUNCS[definition['name']]
    )


def nco_from_sources(sources, geobox, measurements, variable_params, filename):
    coordinates = OrderedDict((name, Coordinate(coord.values, coord.units))
                              for name, coord in sources.coords.items())
    coordinates.update(geobox.coordinates)

    variables = OrderedDict((variable['name'], Variable(dtype=numpy.dtype(variable['dtype']),
                                                        nodata=variable['nodata'],
                                                        dims=sources.dims + geobox.dimensions,
                                                        units=variable['units']))
                            for variable in measurements)

    return create_netcdf_storage_unit(filename, geobox.crs, coordinates, variables, variable_params)


def _tuplify(keys, values, defaults):
    assert not set(values.keys()) - set(keys), 'bad keys'
    return tuple(values.get(key, default) for key, default in zip(keys, defaults))


def _slicify(step, size):
    return (slice(i, min(i + step, size)) for i in range(0, size, step))


def block_iter(steps, shape):
    return product(*(_slicify(step, size) for step, size in zip(steps, shape)))


def tile_iter(tile, chunk):
    steps = _tuplify(tile.dims, chunk, tile.shape)
    return block_iter(steps, tile.shape)


def get_variable_params(config):
    chunking = config.storage['chunking']
    chunking = [chunking[dim] for dim in config.storage['dimension_order']]

    variable_params = {}
    for mapping in config.stats:
        varname = mapping['name']
        variable_params[varname] = {k: v for k, v in mapping.items() if k in STANDARD_VARIABLE_PARAM_NAMES}
        variable_params[varname]['chunksizes'] = chunking

    return variable_params


def get_filename(path_template, tile_index, start_time):
    date_format = '%Y%m%d'
    return Path(str(path_template).format(tile_index=tile_index,
                                          start_time=start_time.strftime(date_format)))


def create_storage_unit(config, task, stat, filename_template):
    geobox = task['sources'][0]['data'].geobox

    def _make_dataset(labels, sources):
        dataset = make_dataset(dataset_type=stat.product,
                               sources=sources,
                               extent=geobox.extent,
                               center_time=labels['time'],
                               uri=None,  # TODO:
                               app_info=None,
                               valid_data=None)
        return dataset

    def merge_sources(prod):
        if stat.algorithm.masked:
            return reduce_(lambda a, b: a + b, (sources.sum() for sources in
                                                xarray.align(prod['data'].sources,
                                                             *[mask_tile.sources for mask_tile in prod['masks']])))
        else:
            return prod['data'].sources.sum()

    sources = reduce_(lambda a, b: a + b, (merge_sources(prod) for prod in task['sources']))
    sources = unsqueeze_data_array(sources, 'time', 0, task['start_time'],
                                   task['sources'][0]['data'].sources.time.attrs)

    # var_params = get_variable_params(config)  # TODO: better way?

    measurements = list(stat.product.measurements.values())
    # filename_template = str(Path(config['location'], stat['file_path_template']))
    output_filename = get_filename(filename_template,
                                   task['tile_index'],
                                   task['start_time'])
    nco = nco_from_sources(sources,
                           geobox,
                           measurements,
                           {},  # TODO: {measurement['name']: var_params[stat['name']] for measurement in measurements},
                           output_filename)
    datasets = xr_apply(sources, _make_dataset, dtype='O')  # Store in DataArray to associate Time -> Dataset
    datasets = datasets_to_doc(datasets)
    netcdf_writer.create_variable(nco, 'dataset', datasets, zlib=True)
    nco['dataset'][:] = netcdf_writer.netcdfy_data(datasets.values)
    return nco


def load_data(tile_index, prod):
    data = GridWorkflow.load(prod['data'][tile_index],
                             measurements=prod['spec']['measurements'])
    data = mask_invalid_data(data)

    for spec, mask_tile in zip(prod['spec']['masks'], prod['masks']):
        mask = GridWorkflow.load(mask_tile[tile_index],
                                 measurements=[spec['measurement']])[spec['measurement']]
        mask = make_mask(mask, **spec['flags'])
        data = data.where(mask)
        del mask
    return data


def do_stats(task, config):
    results = {}
    for stat_name, stat in task['products'].items():
        filename_template = str(Path(config.location, stat.definition['file_path_template']))
        results[stat_name] = create_storage_unit(config, task, stat, filename_template)

    for tile_index in tile_iter(task['sources'][0]['data'], {'x': 1000, 'y': 1000}):
        datasets = [load_data(tile_index, prod) for prod in task['sources']]
        data = xarray.concat(datasets, dim='time')
        data = data.isel(time=data.time.argsort())  # sort along time dim

        for stat_name, stat in task['products'].items():
            data_stats = stat.algorithm.compute(data)  # TODO: if stat.algorithm.masked
            for var_name, var in data_stats.data_vars.items():
                results[stat_name][var_name][(0,) + tile_index[1:]] = var.values  # HACK: make netcdf slicing nicer?...
                results[stat_name].sync()
                print(stat_name, tile_index[1:])

    for stat, nco in results.items():
        nco.close()


def make_tasks(index, products, config):
    for time_period in date_sequence(start=config.start_time, end=config.end_time,
                                     stats_duration=config.stats_duration, step_size=config.step_size):
        print(*time_period)
        workflow = GridWorkflow(index, grid_spec=config.grid_spec)

        tasks = {}
        for source in config.sources:
            data = workflow.list_cells(product=source['product'], time=time_period, cell_index=(15, -40))
            masks = [workflow.list_cells(product=mask['product'], time=time_period, cell_index=(15, -40))
                     for mask in source['masks']]

        for key in data.keys():
            tasks.setdefault(key, {
                'tile_index': key,
                'products': products,
                'start_time': time_period[0],
                'end_time': time_period[1],
                'sources': [],
            })['sources'].append({
                'data': data[key],
                'masks': [mask.get(key) for mask in masks],
                'spec': source,
            })

        for task in tasks.values():
            yield task


class StatsConfig(object):
    def __init__(self, config):
        self.config = config

        self.storage = config['storage']

        self.sources = config['sources']
        self.stats = config['stats']

        self.start_time = to_datetime(config['start_date'])
        self.end_time = to_datetime(config['end_date'])
        self.stats_duration = config['stats_duration']
        self.step_size = config['step_size']
        self.grid_spec = self.create_grid_spec()
        self.location = config['location']

    def create_grid_spec(self):
        storage = self.storage
        crs = CRS(storage['crs'])
        return GridSpec(crs=crs,
                        tile_size=[storage['tile_size'][dim] for dim in crs.dimensions],
                        resolution=[storage['resolution'][dim] for dim in crs.dimensions])


def make_products(index, config):
    results = {}

    # TODO: multiple source products
    prod = index.products.get_by_name(config.sources[0]['product'])
    measurements = [measurement for name, measurement in prod.measurements.items()
                    if name in config.sources[0]['measurements']]

    for stat in config.stats:
        algorithm = make_stat_metadata(stat)

        name = stat['name']
        definition = {
            'name': name,
            'description': name,
            'metadata_type': 'eo',
            'metadata': {
                'format': 'NetCDF',
                'product_type': name,
            },
            'storage': config.storage,
            'measurements': [
                {
                    'name': measurement['name'],
                    'dtype': algorithm.dtype(measurement['dtype']),
                    'nodata': algorithm.nodata,
                    'units': algorithm.units
                }
                for measurement in measurements  # TODO: multiple source products
                ]

        }
        results[name] = Stat(product=index.products.from_doc(definition),
                             algorithm=algorithm,
                             definition=stat)
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

    config = StatsConfig(config)

    products = make_products(index, config)
    tasks = make_tasks(index, products, config)

    futures = [executor.submit(do_stats, task, config) for task in tasks]

    for future in executor.as_completed(futures):
        result = executor.result(future)
        print(result)


if __name__ == '__main__':
    main()
