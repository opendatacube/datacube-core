"""
Create statistical summaries command

"""

from __future__ import absolute_import, print_function

from collections import OrderedDict, namedtuple
from functools import reduce as reduce_, partial
import itertools
import logging
from pathlib import Path

import click
import numpy
import xarray
from pandas import to_datetime

from datacube.api import make_mask
from datacube.api.grid_workflow import GridWorkflow
from datacube.model import GridSpec, CRS, Coordinate, Variable, DatasetType
from datacube.model.utils import make_dataset, datasets_to_doc, xr_apply
from datacube.storage import netcdf_writer
from datacube.storage.masking import mask_valid_data as mask_invalid_data
from datacube.storage.storage import create_netcdf_storage_unit
from datacube.ui import click as ui
from datacube.ui.click import to_pathlib
from datacube.utils import read_documents, unsqueeze_data_array
from datacube.utils.dates import date_sequence

from .statistics import apply_cross_measurement_reduction, nan_percentile, argpercentile, axisindex

_LOG = logging.getLogger(__name__)
STANDARD_VARIABLE_PARAM_NAMES = {'zlib',
                                 'complevel',
                                 'shuffle',
                                 'fletcher32',
                                 'contiguous',
                                 'attrs'}


StatMetadata = namedtuple('StatMetadata', ['masked', 'measurements', 'compute'])


def _compose_helper(f, g, *args):
    return f(g(*args))


def compose(f, g):
    return partial(_compose_helper, f, g)


def transform_measurements(input_measurements):
    return [
        {attr: measurement[attr] for attr in ['name', 'dtype', 'nodata', 'units']}
        for measurement in input_measurements]


def transform_measurements_index(input_measurements):
    index_measurements = [
        {
            'name': measurement['name'] + '_source',
            'dtype': 'int8',
            'nodata': -1,
            'units': '1'
        }
        for measurement in input_measurements
        ]
    date_measurements = [
        {
            'name': measurement['name'] + '_observed',
            'dtype': 'float64',
            'nodata': 0,
            'units': 'seconds since 1970-01-01 00:00:00'
        }
        for measurement in input_measurements
        ]

    return transform_measurements(input_measurements) + date_measurements + index_measurements


def expand_index(data, index):
    def not_foo(var):
        return axisindex(data.data_vars[var.name].values, var.values)
    data_values = index.apply(not_foo)

    def not_bar(var):
        return data.time.values[var.values]
    time_values = index.apply(not_bar).rename(OrderedDict((name, name+'_observed') for name in index.data_vars))

    return xarray.merge([data_values, time_values])


def do_index_stats(stat_func, data):
    index = stat_func(data)
    return expand_index(data, index)


def make_name_stat(name, masked=True):
    return StatMetadata(masked=masked,
                        measurements=transform_measurements,
                        compute=partial(getattr(xarray.Dataset, name), dim='time'))


STATS = {
    'mean': make_name_stat('mean'),
    'percentile_10': StatMetadata(masked=True,
                                  # pylint: disable=redundant-keyword-arg
                                  measurements=transform_measurements_index,
                                  compute=partial(do_index_stats, partial(getattr(xarray.Dataset, 'reduce'),
                                                                          dim='time',
                                                                          func=argpercentile,
                                                                          q=10.0)))
}


# STAT_FUNCS = {
#     'mean': Lambda('mean'),
#     'median': Lambda('median'),
#     'medoid': apply_cross_measurement_reduction,
#     'percentile_10': Lambda('reduce', func=nan_percentile, q=10),
#     'percentile_50': Lambda('reduce', func=nan_percentile, q=50),
#     'percentile_90': Lambda('reduce', func=nan_percentile, q=90),
# }


class StatProduct(object):
    def __init__(self, metadata_type, input_measurements, definition, storage):
        self.definition = definition
        self.product = self._create_product(metadata_type, input_measurements, storage)

    @property
    def name(self):
        return self.definition['name']

    @property
    def stat_name(self):
        return self.definition['statistic']

    @property
    def statistic(self):
        return STATS[self.stat_name]

    @property
    def masked(self):
        return self.statistic.masked

    @property
    def compute(self):
        return self.statistic.compute

    def _create_product(self, metadata_type, input_measurements, storage):
        data_measurements = self.statistic.measurements(input_measurements)

        product_definition = {
            'name': self.name,
            'description': 'Description for ' + self.name,
            'metadata_type': 'eo',
            'metadata': {
                'format': 'NetCDF',
                'product_type': self.stat_name,
            },
            'storage': storage,
            'measurements': data_measurements
        }
        DatasetType.validate(product_definition)
        return DatasetType(metadata_type, product_definition)


class StatsConfig(object):
    def __init__(self, config):
        self.config = config

        self.storage = config['storage']

        self.sources = config['sources']
        self.output_products = config['output_products']

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

    def get_variable_params(self):
        chunking = self.storage['chunking']
        chunking = [chunking[dim] for dim in self.storage['dimension_order']]

        variable_params = {}
        for mapping in self.output_products:
            varname = mapping['name']
            variable_params[varname] = {k: v for k, v in mapping.items() if k in STANDARD_VARIABLE_PARAM_NAMES}
            variable_params[varname]['chunksizes'] = chunking

        return variable_params


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
    return itertools.product(*(_slicify(step, size) for step, size in zip(steps, shape)))


def tile_iter(tile, chunk):
    steps = _tuplify(tile.dims, chunk, tile.shape)
    return block_iter(steps, tile.shape)


def get_filename(path_template, tile_index, start_time):
    return Path(str(path_template).format(tile_index=tile_index,
                                          start_time=start_time))


def create_storage_unit(config, task, stat, filename_template):
    geobox = task['sources'][0]['data'].geobox  # HACK: better way to get geobox
    all_measurement_defns = list(stat.product.measurements.values())

    output_filename = get_filename(filename_template,
                                   task['tile_index'],
                                   task['start_time'])
    datasets, sources = find_source_datasets(task, stat, geobox, uri=output_filename.as_uri())

    #measurement_names = [m['name'] for m in stat.data_measurements]
    #var_params = config.get_variable_params()[stat.name]
    # measurement_params = {
    #     measurement_name: var_params
    #     for measurement_name in measurement_names
    #     }

    nco = nco_from_sources(sources,
                           geobox,
                           all_measurement_defns,
                           {},  # TODO: measurement_params,
                           output_filename)

    netcdf_writer.create_variable(nco, 'dataset', datasets, zlib=True)
    nco['dataset'][:] = netcdf_writer.netcdfy_data(datasets.values)
    return nco


def find_source_datasets(task, stat, geobox, uri=None):
    def _make_dataset(labels, sources):
        dataset = make_dataset(product=stat.product,
                               sources=sources,
                               extent=geobox.extent,
                               center_time=labels['time'],
                               uri=uri,
                               app_info=None,
                               valid_data=None)
        return dataset

    def merge_sources(prod):
        if stat.masked:
            return reduce_(lambda a, b: a + b, (sources.sum() for sources in
                                                xarray.align(prod['data'].sources,
                                                             *[mask_tile.sources for mask_tile in prod['masks']])))
        else:
            return prod['data'].sources.sum()

    sources = reduce_(lambda a, b: a + b, (merge_sources(prod) for prod in task['sources']))
    sources = unsqueeze_data_array(sources, 'time', 0, task['start_time'],
                                   task['sources'][0]['data'].sources.time.attrs)

    datasets = xr_apply(sources, _make_dataset, dtype='O')  # Store in DataArray to associate Time -> Dataset
    datasets = datasets_to_doc(datasets)
    return datasets, sources


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
            _LOG.info("Computing %s in tile %s", stat_name, tile_index)
            assert stat.masked  # TODO: not masked
            data_stats = stat.compute(data)
            # For each of the data variables, shove this chunk into the output results
            for var_name, var in data_stats.data_vars.items():
                results[stat_name][var_name][(0,) + tile_index[1:]] = var.values  # HACK: make netcdf slicing nicer?...
                results[stat_name].sync()
                _LOG.debug("Updated %s %s", var_name, tile_index[1:])

    for stat, nco in results.items():
        nco.close()


def make_tasks(index, products, config):
    for time_period in date_sequence(start=config.start_time, end=config.end_time,
                                     stats_duration=config.stats_duration, step_size=config.step_size):
        _LOG.info('Making output_products tasks for %s to %s', time_period[0], time_period[1])
        workflow = GridWorkflow(index, grid_spec=config.grid_spec)

        # Tasks are grouped by tile_index, and may contain sources from multiple places
        # Each source may be masked by multiple masks
        tasks = {}
        for source_spec in config.sources:
            data = workflow.list_cells(product=source_spec['product'], time=time_period, cell_index=(15, -40))
            masks = [workflow.list_cells(product=mask['product'], time=time_period, cell_index=(15, -40))
                     for mask in source_spec['masks']]

            for tile_index, sources in data.items():
                task = tasks.setdefault(tile_index, {
                    'tile_index': tile_index,
                    'products': products,
                    'start_time': time_period[0],
                    'end_time': time_period[1],
                    'sources': [],
                })
                task['sources'].append({
                    'data': sources,
                    'masks': [mask.get(tile_index) for mask in masks],
                    'spec': source_spec,
                })

        for task in tasks.values():
            yield task


def make_products(index, config):
    _LOG.info('Creating output products')
    created_products = {}

    measurements = calc_output_measurements(index, config.sources)

    for stat in config.output_products:
        index_based_stat = StatProduct(index.metadata_types.get_by_name('eo'), measurements, stat, config.storage)
        created_products[stat['name']] = index_based_stat

    return created_products


def calc_output_measurements(index, sources):
    """
    Look up desired measurements from sources in the database index

    :return: list of measurement definitions
    """
    # Check consistent measurements
    first_source = sources[0]
    if not all(first_source['measurements'] == source['measurements'] for source in sources):
        raise RuntimeError("Configuration Error: listed measurements of source products are not all the same.")

    source_defn = sources[0]

    source_product = index.products.get_by_name(source_defn['product'])
    measurements = [measurement for name, measurement in source_product.measurements.items()
                    if name in source_defn['measurements']]

    return measurements


@click.command(name='output_products')
@click.option('--app-config', '-c',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
              help='configuration file location', callback=to_pathlib)
@click.option('--year', type=click.IntRange(1960, 2060))
@ui.global_cli_options
@ui.executor_cli_options
@ui.pass_index(app_name='agdc-output_products')
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
