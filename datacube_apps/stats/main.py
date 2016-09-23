"""
Create statistical summaries command

"""

from __future__ import absolute_import, print_function

from collections import OrderedDict
from functools import reduce as reduce_, partial
import itertools
import importlib
import logging
from pathlib import Path

import click
import numpy
import rasterio
import xarray
import pandas as pd

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
from datacube_apps.stats.statistics import argnanmedoid, argpercentile, axisindex

_LOG = logging.getLogger(__name__)
STANDARD_VARIABLE_PARAM_NAMES = {'zlib',
                                 'complevel',
                                 'shuffle',
                                 'fletcher32',
                                 'contiguous',
                                 'attrs'}
DEFAULT_GROUP_BY = 'time'


def datetime64_to_lextime(var):
    values = getattr(var, 'values', var)
    years = values.astype('datetime64[Y]').astype('int32') + 1970
    months = values.astype('datetime64[M]').astype('int32') % 12 + 1
    days = (values.astype('datetime64[D]') - values.astype('datetime64[M]') + 1).astype('int32')
    return years * 10000 + months * 100 + days


class ValueStat(object):
    """
    Holder class describing the outputs of a statistic and how to calculate it

    :param bool masked: whether to apply masking to the input data
    :param function measurements: how to turn a list of input measurements into a list of output measurements
    :param function compute: how to calculate the statistic
    """

    def __init__(self, stat_func, masked=True):
        self.masked = masked
        self.stat_func = stat_func

    def compute(self, data):
        return self.stat_func(data)

    @staticmethod
    def measurements(input_measurements):
        return [
            {attr: measurement[attr] for attr in ['name', 'dtype', 'nodata', 'units']}
            for measurement in input_measurements]


class NDVIStats(object):
    def __init__(self, masked=True):
        self.masked = masked

    @staticmethod
    def compute(data):
        ndvi = (data.nir - data.red) / (data.nir + data.red)
        ndvi_min = ndvi.min(dim='time')
        ndvi_max = ndvi.max(dim='time')
        ndvi_mean = ndvi.mean(dim='time')
        return xarray.Dataset({'ndvi_min': ndvi_min, 'ndvi_max': ndvi_max, 'ndvi_mean': ndvi_mean},
                              attrs=dict(crs=data.crs))

    @staticmethod
    def measurements(input_measurements):
        measurement_names = [m['name'] for m in input_measurements]
        if 'red' not in measurement_names or 'nir' not in measurement_names:
            raise ConfigurationError('Input measurements for NDVI must include "nir" and "red"')

        return [dict(name=name, dtype='float32', nodata=-1, units='1')
                for name in ('ndvi_min', 'ndvi_max', 'ndvi_mean')]


class PerBandIndexStat(ValueStat):
    """
    Each output variable contains values that actually exist in the input data

    :param stat_func: A function which takes an xarray.Dataset and returns an xarray.Dataset of indexes
    """

    def __init__(self, stat_func, masked=True):
        super(PerBandIndexStat, self).__init__(stat_func, masked)

    def compute(self, data):
        index = super(PerBandIndexStat, self).compute(data)

        def index_dataset(var):
            return axisindex(data.data_vars[var.name].values, var.values)

        data_values = index.apply(index_dataset)

        def index_time(var):
            return data.time.values[var.values]

        time_values = index.apply(index_time).rename(OrderedDict((name, name + '_observed')
                                                                 for name in index.data_vars))

        text_values = time_values.apply(datetime64_to_lextime).rename(OrderedDict((name, name + '_date')
                                                                                  for name in time_values.data_vars))

        return xarray.merge([data_values, time_values, text_values])

    @staticmethod
    def measurements(input_measurements):
        index_measurements = [
            {
                'name': measurement['name'] + '_source',
                'dtype': 'uint8',
                'nodata': 255,
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
        text_measurements = [
            {
                'name': measurement['name'] + '_observed_date',
                'dtype': 'int32',
                'nodata': 0,
                'units': 'Date as YYYYMMDD'
            }
            for measurement in input_measurements
            ]

        return ValueStat.measurements(input_measurements) + date_measurements + index_measurements + text_measurements


class PerStatIndexStat(ValueStat):
    """
    :param stat_func: A function which takes an xarray.Dataset and returns an xarray.Dataset of indexes
    """

    def __init__(self, stat_func, masked=True):
        super(PerStatIndexStat, self).__init__(stat_func, masked)

    def compute(self, data):
        index = super(PerStatIndexStat, self).compute(data)

        def index_dataset(var, axis):
            return axisindex(var, index, axis=axis)

        data_values = data.reduce(index_dataset, dim='time')
        observed = data.time.values[index]
        data_values['observed'] = (('y', 'x'), observed)
        data_values['observed_date'] = (('y', 'x'), datetime64_to_lextime(observed))

        return data_values

    @staticmethod
    def measurements(input_measurements):
        index_measurements = [
            {
                'name': 'source',
                'dtype': 'uint8',
                'nodata': 255,
                'units': '1'
            }
        ]
        date_measurements = [
            {
                'name': 'observed',
                'dtype': 'float64',
                'nodata': 0,
                'units': 'seconds since 1970-01-01 00:00:00'
            }
        ]
        text_measurements = [
            {
                'name': 'observed_date',
                'dtype': 'int32',
                'nodata': 0,
                'units': 'Date as YYYYMMDD'
            }
            ]
        return ValueStat.measurements(input_measurements) + date_measurements + index_measurements + text_measurements


def make_name_stat(name, masked=True):
    """
    A value returning statistic, relying on an xarray function of name being available

    :param name: The name of an xArray statistical function
    :param masked:
    :return:
    """
    return ValueStat(masked=masked,
                     stat_func=partial(getattr(xarray.Dataset, name), dim='time'))


def _medoid_helper(data):
    flattened = data.to_array(dim='variable')
    variable, time, y, x = flattened.shape
    index = numpy.empty((y, x), dtype='int64')
    # TODO: nditer?
    for iy in range(y):
        for ix in range(x):
            index[iy, ix] = argnanmedoid(flattened.values[:, :, iy, ix])
    return index


STATS = {
    'mean': make_name_stat('mean'),
    'percentile_10': PerBandIndexStat(masked=True,
                                      # pylint: disable=redundant-keyword-arg
                                      stat_func=partial(getattr(xarray.Dataset, 'reduce'),
                                                        dim='time',
                                                        func=argpercentile,
                                                        q=10.0)),
    'medoid': PerStatIndexStat(masked=True, stat_func=_medoid_helper),
    'min': make_name_stat('min'),
    'max': make_name_stat('max'),
    'ndvi_stats': NDVIStats()
}


class StatProduct(object):
    def __init__(self, metadata_type, input_measurements, definition, storage):
        self.definition = definition

        data_measurements = self.statistic.measurements(input_measurements)

        self.product = self._create_product(metadata_type, data_measurements, storage)
        self.netcdf_var_params = self._create_netcdf_var_params(storage, data_measurements)

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

    def _create_netcdf_var_params(self, storage, data_measurements):
        chunking = storage['chunking']
        chunking = [chunking[dim] for dim in storage['dimension_order']]

        variable_params = {}
        for measurement in data_measurements:
            name = measurement['name']
            variable_params[name] = {k: v for k, v in self.definition.items() if k in STANDARD_VARIABLE_PARAM_NAMES}
            variable_params[name]['chunksizes'] = chunking
            variable_params[name].update({k: v for k, v in measurement.items() if k in STANDARD_VARIABLE_PARAM_NAMES})
        return variable_params

    def _create_product(self, metadata_type, data_measurements, storage):
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
        self._definition = config

        self.storage = config['storage']

        self.sources = config['sources']
        self.output_products = config['output_products']

        self.start_time = pd.to_datetime(config['start_date'])
        self.end_time = pd.to_datetime(config['end_date'])
        self.stats_duration = config['stats_duration']
        self.step_size = config['step_size']
        self.grid_spec = self.create_grid_spec()
        self.location = config['location']
        self.computation = config['computation']

    def create_grid_spec(self):
        storage = self.storage
        crs = CRS(storage['crs'])
        return GridSpec(crs=crs,
                        tile_size=[storage['tile_size'][dim] for dim in crs.dimensions],
                        resolution=[storage['resolution'][dim] for dim in crs.dimensions])


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


def get_filename(path_template, **kwargs):
    return Path(str(path_template).format(**kwargs))


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
            all_sources = xarray.align(prod['data'].sources, *[mask_tile.sources for mask_tile in prod['masks']])
            return reduce_(lambda a, b: a + b, (sources.sum() for sources in all_sources))
        else:
            return prod['data'].sources.sum()

    sources = reduce_(lambda a, b: a + b, (merge_sources(prod) for prod in task['sources']))
    sources = unsqueeze_data_array(sources, dim='time', pos=0, coord=task['start_time'],
                                   attrs=task['sources'][0]['data'].sources.time.attrs)

    datasets = xr_apply(sources, _make_dataset, dtype='O')  # Store in DataArray to associate Time -> Dataset
    datasets = datasets_to_doc(datasets)
    return datasets, sources


def import_function(func_ref):
    module_name, _, func_name = func_ref.rpartition('.')
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


def load_masked_data(tile_index, source_prod):
    data = GridWorkflow.load(source_prod['data'][tile_index],
                             measurements=source_prod['spec']['measurements'])
    crs = data.crs
    data = mask_invalid_data(data)

    for spec, mask_tile in zip(source_prod['spec']['masks'], source_prod['masks']):
        fuse_func = import_function(spec['fuse_func']) if 'fuse_func' in spec else None
        mask = GridWorkflow.load(mask_tile[tile_index],
                                 measurements=[spec['measurement']],
                                 fuse_func=fuse_func)[spec['measurement']]
        mask = make_mask(mask, **spec['flags'])
        data = data.where(mask)
        del mask
    data.attrs['crs'] = crs
    return data


def load_data(tile_index, task):
    datasets = [load_masked_data(tile_index, source_prod) for source_prod in task['sources']]
    data = xarray.concat(datasets, dim='time')
    return data.isel(time=data.time.argsort())  # sort along time dim


class OutputDriver(object):
    def __init__(self, task, config):
        self.task = task
        self.config = config

        self.output_files = {}

    def close_files(self):
        for output_file in self.output_files.values():
            output_file.close()

    def __enter__(self):
        self.open_output_files()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_files()


class NetcdfOutputDriver(OutputDriver):
    def open_output_files(self):
        for prod_name, stat in self.task['output_products'].items():
            filename_template = str(Path(self.config.location, stat.definition['file_path_template']))
            self.output_files[prod_name] = self._create_storage_unit(stat, filename_template)

    def _create_storage_unit(self, stat, filename_template):
        geobox = self.task['sources'][0]['data'].geobox  # HACK: better way to get geobox
        all_measurement_defns = list(stat.product.measurements.values())

        output_filename = get_filename(filename_template,
                                       tile_index=self.task['tile_index'],
                                       start_time=self.task['start_time'])
        datasets, sources = find_source_datasets(self.task, stat, geobox, uri=output_filename.as_uri())

        nco = nco_from_sources(sources,
                               geobox,
                               all_measurement_defns,
                               stat.netcdf_var_params,
                               output_filename)

        netcdf_writer.create_variable(nco, 'dataset', datasets, zlib=True)
        nco['dataset'][:] = netcdf_writer.netcdfy_data(datasets.values)
        return nco

    def write_data(self, prod_name, var_name, tile_index, values):
        self.output_files[prod_name][var_name][(0,) + tile_index[1:]] = netcdf_writer.netcdfy_data(values)
        self.output_files[prod_name].sync()
        _LOG.debug("Updated %s %s", var_name, tile_index[1:])


class RioOutputDriver(OutputDriver):
    def open_output_files(self):
        for prod_name, stat in self.task['output_products'].items():
            for measurename, measure_def in stat.product.measurements.items():
                filename_template = str(Path(self.config.location, stat.definition['file_path_template']))
                geobox = self.task['sources'][0]['data'].geobox  # HACK: better way to get geobox

                output_filename = get_filename(filename_template,
                                               var_name=measurename,
                                               tile_index=self.task['tile_index'],
                                               start_time=self.task['start_time'])
                try:
                    output_filename.parent.mkdir(parents=True)
                except OSError:
                    pass

                profile = {
                    'blockxsize': 256,
                    'blockysize': 256,
                    'compress': 'lzw',
                    'driver': 'GTiff',
                    'interleave': 'band',
                    'tiled': True,
                    'dtype': measure_def['dtype'],
                    'nodata': measure_def['nodata'],
                    'width': geobox.width,
                    'height': geobox.height,
                    'affine': geobox.affine,
                    'crs': geobox.crs.crs_str,
                    'count': 1
                }

                output_name = prod_name + measurename
                self.output_files[output_name] = rasterio.open(str(output_filename), mode='w', **profile)

    def write_data(self, prod_name, var_name, tile_index, values):
        output_name = prod_name + var_name
        y, x = tile_index[1:]
        window = ((y.start, y.stop), (x.start, x.stop))
        _LOG.debug("Updating %s.%s %s", prod_name, var_name, window)

        dtype = self.task['output_products'][prod_name].product.measurements[var_name]['dtype']

        self.output_files[output_name].write(values.astype(dtype), indexes=1, window=window)


OUTPUT_DRIVERS = {
    'NetCDF CF': NetcdfOutputDriver,
    'rasterio': RioOutputDriver
}


def do_stats(task, config):
    output_driver = OUTPUT_DRIVERS[config.storage['driver']]
    with output_driver(task, config) as output_files:
        example_tile = task['sources'][0]['data']
        for tile_index in tile_iter(example_tile, config.computation['chunking']):
            data = load_data(tile_index, task)

            for prod_name, stat in task['output_products'].items():
                _LOG.info("Computing %s in tile %s", prod_name, tile_index)
                assert stat.masked  # TODO: not masked
                stats_data = stat.compute(data)

                # For each of the data variables, shove this chunk into the output results
                for var_name, var in stats_data.data_vars.items():
                    output_files.write_data(prod_name, var_name, tile_index, var.values)


def make_tasks(index, output_products, config):
    for time_period in date_sequence(start=config.start_time, end=config.end_time,
                                     stats_duration=config.stats_duration, step_size=config.step_size):
        _LOG.info('Making output_products tasks for %s to %s', time_period[0], time_period[1])
        workflow = GridWorkflow(index, grid_spec=config.grid_spec)

        # Tasks are grouped by tile_index, and may contain sources from multiple places
        # Each source may be masked by multiple masks
        tasks = {}
        for source_spec in config.sources:
            data = workflow.list_cells(product=source_spec['product'], time=time_period,
                                       cell_index=(15, -40),
                                       group_by=source_spec.get('group_by', DEFAULT_GROUP_BY))
            masks = [workflow.list_cells(product=mask['product'],
                                         time=time_period,
                                         cell_index=(15, -40),
                                         group_by=source_spec.get('group_by', DEFAULT_GROUP_BY))
                     for mask in source_spec['masks']]

            for tile_index, sources in data.items():
                task = tasks.setdefault(tile_index, {
                    'tile_index': tile_index,
                    'output_products': output_products,
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


class ConfigurationError(RuntimeError):
    pass


def make_products(index, config):
    _LOG.info('Creating output products')

    output_names = [prod['name'] for prod in config.output_products]
    if len(output_names) != len(set(output_names)):
        raise ConfigurationError('Output products must all have different names.')

    output_products = {}

    measurements = calc_output_measurements(index, config.sources)

    for prod in config.output_products:
        output_products[prod['name']] = StatProduct(index.metadata_types.get_by_name('eo'), measurements,
                                                    definition=prod, storage=config.storage)

    return output_products


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

    output_products = make_products(index, config)
    tasks = make_tasks(index, output_products, config)

    futures = [executor.submit(do_stats, task, config) for task in tasks]

    for future in executor.as_completed(futures):
        result = executor.result(future)
        print(result)


if __name__ == '__main__':
    main()
