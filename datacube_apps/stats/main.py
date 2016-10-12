"""
Create statistical summaries command

"""

from __future__ import absolute_import, print_function

import itertools
import logging
from functools import partial

import click
import numpy
import pandas as pd
import xarray
from datacube import Datacube
from datacube.api import make_mask
from datacube.api.grid_workflow import GridWorkflow, Tile
from datacube.api.query import query_group_by, query_geopolygon
from datacube.model import GridSpec, CRS, DatasetType, GeoBox
from datacube.storage.masking import mask_valid_data as mask_invalid_data
from datacube.ui import click as ui
from datacube.ui.click import to_pathlib
from datacube.utils import read_documents, import_function, tile_iter
from datacube.utils.dates import date_sequence
from datacube_apps.stats.output_drivers import NetcdfOutputDriver, RioOutputDriver
from datacube_apps.stats.statistics import ValueStat, WofsStats, NormalisedDifferenceStats, PerStatIndexStat, \
    compute_medoid, percentile_stat, StatsConfigurationError

_LOG = logging.getLogger(__name__)
DEFAULT_GROUP_BY = 'time'

STATS = {
    'min': ValueStat.from_stat_name('min'),
    'max': ValueStat.from_stat_name('max'),
    'mean': ValueStat.from_stat_name('mean'),
    'percentile_10': percentile_stat(10),
    'percentile_50': percentile_stat(50),
    'percentile_90': percentile_stat(90),
    'medoid': PerStatIndexStat(masked=True, stat_func=compute_medoid),
    'ndvi_stats': NormalisedDifferenceStats(name='ndvi', band1='nir', band2='red',
                                            stats=['min', 'mean', 'max']),
    'ndwi_stats': NormalisedDifferenceStats(name='ndwi', band1='green', band2='swir1',
                                            stats=['min', 'mean', 'max']),
    'ndvi_daily': NormalisedDifferenceStats(name='ndvi', band1='nir', band2='red', stats=['squeeze']),
    'wofs': WofsStats(),
}


class StatProduct(object):
    def __init__(self, metadata_type, input_measurements, definition, storage):
        self.definition = definition

        #: The product name.
        self.name = definition['name']

        #: The name of the statistic. Eg, mean, max, medoid, percentile_10
        self.stat_name = self.definition['statistic']

        #: The implementation of a statistic. See :class:`ValueStat`.
        #: Will provide `compute` and `measurements` functions.
        self.statistic = STATS[self.stat_name]

        self.data_measurements = self.statistic.measurements(input_measurements)

        self.product = self._create_product(metadata_type, self.data_measurements, storage)

    @property
    def masked(self):
        return self.statistic.masked

    @property
    def compute(self):
        return self.statistic.compute

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


class StatsApp(object):
    """
    A StatsApp can produce a set of time based statistical products.

    """

    def __init__(self, config, config_filename):
        #: Name of the configuration file used
        self.config_file = config_filename

        #: Description of output file format
        self.storage = config['storage']

        #: Definition of source products, including their name, which variables to pull from them, and
        #: a specification of any masking that should be applied.
        self.sources = config['sources']

        #: List of filenames and statistical methods used, describing what the outputs of the run will be.
        self.output_products = config['output_products']

        #: Base directory to write output files to.
        #: Files may be created in a sub-directory, depending on the configuration of the
        #: :attr:`output_driver`.
        self.location = config['location']

        #: How to slice a task up spatially to to fit into memory.
        self.computation = config.get('computation', {'chunking': {'x': 1000, 'y': 1000}})

        #: A function to generate a sequence of date ranges.
        self.generate_date_sequence = partial(date_sequence, start=pd.to_datetime(config['start_date']),
                                              end=pd.to_datetime(config['end_date']),
                                              stats_duration=config['stats_duration'], step_size=config['step_size'])

        #: Generates tasks to compute statistics. These tasks should be :class:`StatsTask` objects
        #: and will define spatial and temporal boundaries, as well as statistical operations to be run.
        self.task_generator = None
        if 'input_region' in config:
            # Generate statistics for an Ungridded region
            self.task_generator = partial(generate_non_gridded_tasks, input_region=config['input_region'],
                                          storage=self.storage)
        else:
            grid_spec = self._make_grid_spec(config['storage'])
            self.task_generator = partial(generate_gridded_tasks, grid_spec=grid_spec)

        #: A class which knows how to create and write out data to a permanent storage format.
        #: Implements :class:`.output_drivers.OutputDriver`.
        self.output_driver = OUTPUT_DRIVERS[self.storage['driver']]

    @classmethod
    def from_file(cls, filename):
        _, config = next(read_documents(filename))
        stats_app = cls(config, filename)
        stats_app.validate_config()
        return stats_app

    @staticmethod
    def _make_grid_spec(storage):
        """Make a grid spec based on a storage spec."""
        if 'tile_size' not in storage:
            return None

        crs = CRS(storage['crs'])
        return GridSpec(crs=crs,
                        tile_size=[storage['tile_size'][dim] for dim in crs.dimensions],
                        resolution=[storage['resolution'][dim] for dim in crs.dimensions])

    def validate_config(self):
        """Raise an error if configuration errors are found."""
        # Check output product names are unique
        output_names = [prod['name'] for prod in self.output_products]
        duplicate_names = [x for x in output_names if output_names.count(x) > 1]
        if duplicate_names:
            raise StatsConfigurationError('Output products must all have different names. '
                                          'Duplicates found: %s' % duplicate_names)

        # Check statistics are available
        requested_statistics = set(prod['statistic'] for prod in self.output_products)
        available_statistics = set(STATS.keys())
        if requested_statistics > available_statistics:
            raise StatsConfigurationError(
                'Requested Statistic %s is not a valid statistic. Available statistics are: %s'
                % (requested_statistics - available_statistics, available_statistics))

        # Check consistent measurements
        first_source = self.sources[0]
        if not all(first_source['measurements'] == source['measurements'] for source in self.sources):
            raise StatsConfigurationError("Configuration Error: listed measurements of source products "
                                          "are not all the same.")

    def run(self, index, executor, output_products, queue_length=50):
        tasks = self.generate_tasks(index, output_products)

        completed_tasks = map_orderless(executor, self.execute_stats_task, tasks, queue=queue_length)

        for task in completed_tasks:
            yield task

    def generate_tasks(self, index, output_products):
        """
        Generate a sequence of `StatsTask` definitions.

        A Task Definition contains:

          * tile_index
          * time_period
          * sources: (list of)
          * output_products

        Sources is a list of dictionaries containing:

          * data
          * masks (list of)
          * spec - Source specification, containing details about which bands to load and how to apply masks.

        :param output_products: List of output product definitions
        :return:
        """
        for task in self.task_generator(index=index, generate_date_sequence=self.generate_date_sequence,
                                        sources_spec=self.sources):
            task.output_products = output_products
            yield task

    def execute_stats_task(self, task):
        app_info = get_app_metadata(self.config_file)
        with self.output_driver(task=task, output_path=self.location, app_info=app_info,
                                storage=self.storage) as output_files:
            example_tile = task.sources[0]['data']
            for sub_tile_slice in tile_iter(example_tile, self.computation['chunking']):
                data = load_data(sub_tile_slice, task.sources)

                for prod_name, stat in task.output_products.items():
                    _LOG.info("Computing %s in tile %s", prod_name, sub_tile_slice)
                    assert stat.masked  # TODO: not masked
                    stats_data = stat.compute(data)

                    # For each of the data variables, shove this chunk into the output results
                    for var_name, var in stats_data.data_vars.items():
                        output_files.write_data(prod_name, var_name, sub_tile_slice, var.values)

    def make_output_products(self, index, metadata_type='eo'):
        """
        Return a dict mapping Output Product Name to StatProduct

        StatProduct describes the structure and how to compute the output product.
        """
        _LOG.info('Creating output products')

        output_products = {}

        measurements = source_measurement_defs(index, self.sources)

        metadata_type = index.metadata_types.get_by_name(metadata_type)
        for prod in self.output_products:
            output_products[prod['name']] = StatProduct(metadata_type=metadata_type,
                                                        input_measurements=measurements,
                                                        definition=prod,
                                                        storage=self.storage)

        return output_products


def _load_masked_data(sub_tile_slice, source_prod):
    data = GridWorkflow.load(source_prod['data'][sub_tile_slice],
                             measurements=source_prod['spec']['measurements'])
    crs = data.crs
    data = mask_invalid_data(data)

    if 'masks' in source_prod and 'masks' in source_prod['spec']:
        for mask_spec, mask_tile in zip(source_prod['spec']['masks'], source_prod['masks']):
            fuse_func = import_function(mask_spec['fuse_func']) if 'fuse_func' in mask_spec else None
            mask = GridWorkflow.load(mask_tile[sub_tile_slice],
                                     measurements=[mask_spec['measurement']],
                                     fuse_func=fuse_func)[mask_spec['measurement']]
            mask = make_mask(mask, **mask_spec['flags'])
            data = data.where(mask)
            del mask
    data.attrs['crs'] = crs  # Reattach crs, it gets lost when masking
    return data


def load_data(sub_tile_slice, sources):
    """
    Load a masked chunk of data from the datacube, based on a specification and list of datasets in `sources`.

    :param sub_tile_slice:
    :param sources: a dictionary containing `data`, `spec` and `masks`
    :return: :class:`xarray.Dataset` containing loaded data. Will be indexed and sorted by time.
    """
    datasets = [_load_masked_data(sub_tile_slice, source_prod) for source_prod in sources]
    for idx, dataset in enumerate(datasets):
        dataset.coords['source'] = ('time', numpy.repeat(idx, dataset.time.size))
    data = xarray.concat(datasets, dim='time')
    return data.isel(time=data.time.argsort())  # sort along time dim


OUTPUT_DRIVERS = {
    'NetCDF CF': NetcdfOutputDriver,
    'Geotiff': RioOutputDriver
}


class StatsTask(object):
    def __init__(self, time_period, tile_index=None, sources=None, output_products=None):
        self.tile_index = tile_index
        self.time_period = time_period
        self.sources = sources or []
        self.output_products = output_products or []

    @property
    def geobox(self):
        return self.sources[0]['data'].geobox

    @property
    def time_attributes(self):
        return self.sources[0]['data'].sources.time.attrs

    def keys(self):
        return self.__dict__.keys()

    def __getitem__(self, item):
        return getattr(self, item)


def generate_gridded_tasks(index, sources_spec, generate_date_sequence, grid_spec, cell_index=(14, -40)):
    """
    Generate the required tasks through time and across a spatial grid.

    :param index: Datacube Index
    :return:
    """
    workflow = GridWorkflow(index, grid_spec=grid_spec)
    for time_period in generate_date_sequence():
        _LOG.info('Making output_products tasks for time period: %s', time_period)

        # Tasks are grouped by tile_index, and may contain sources from multiple places
        # Each source may be masked by multiple masks
        tasks = {}
        for source_spec in sources_spec:
            data = workflow.list_cells(product=source_spec['product'], time=time_period,
                                       group_by=source_spec.get('group_by', DEFAULT_GROUP_BY),
                                       cell_index=cell_index)
            masks = [workflow.list_cells(product=mask['product'],
                                         time=time_period,
                                         group_by=source_spec.get('group_by', DEFAULT_GROUP_BY),
                                         cell_index=cell_index)
                     for mask in source_spec.get('masks', [])]

            for tile_index, sources in data.items():
                task = tasks.setdefault(tile_index, StatsTask(time_period=time_period, tile_index=tile_index))
                task.sources.append({
                    'data': sources,
                    'masks': [mask.get(tile_index) for mask in masks],
                    'spec': source_spec,
                })

        for task in tasks.values():
            yield task


def generate_non_gridded_tasks(index, storage, generate_date_sequence, input_region, sources_spec):
    """
    Make stats tasks for a defined spatial region, that doesn't fit into a standard grid.

    :param index: database index
    :param input_region: dictionary of query parameters defining the target input region. Usually
                         x/y spatial boundaries.
    :return:
    """
    dc = Datacube(index=index)

    def make_tile(product, time, group_by):
        datasets = dc.product_observations(product=product, time=time, **input_region)
        group_by = query_group_by(group_by=group_by)
        sources = dc.product_sources(datasets, group_by)

        res = storage['resolution']

        geopoly = query_geopolygon(**input_region)
        geopoly = geopoly.to_crs(CRS(storage['crs']))
        geobox = GeoBox.from_geopolygon(geopoly, (res['y'], res['x']))

        return Tile(sources, geobox)

    for time_period in generate_date_sequence():
        _LOG.info('Making output_products tasks for time period: %s', time_period)

        task = StatsTask(time_period)

        for source_spec in sources_spec:
            group_by_name = source_spec.get('group_by', DEFAULT_GROUP_BY)

            # Build Tile
            data = make_tile(product=source_spec['product'], time=time_period,
                             group_by=group_by_name)

            masks = [make_tile(product=mask['product'], time=time_period,
                               group_by=group_by_name)
                     for mask in source_spec.get('masks', [])]

            if len(data.sources.time) == 0:
                continue

            task.sources.append({
                'data': data,
                'masks': masks,
                'spec': source_spec,
            })

        if task.sources:
            yield task


def source_measurement_defs(index, sources):
    """
    Look up desired measurements from sources in the database index

    :return: list of measurement definitions
    """
    source_defn = sources[0]  # Sources should have been checked to all have the same measureemnts

    source_measurements = index.products.get_by_name(source_defn['product']).measurements

    measurements = [measurement for name, measurement in source_measurements.items()
                    if name in source_defn['measurements']]

    return measurements


def get_app_metadata(config_file):
    return {
        'lineage': {
            'algorithm': {
                'name': 'datacube-stats',
                'version': 'unknown',  # TODO get version from somewhere
                'repo_url': 'https://github.com/GeoscienceAustralia/agdc_statistics.git',
                'parameters': {'configuration_file': str(config_file)}
            },
        }
    }


def map_orderless(executor, core, tasks, queue=50):
    tasks = (i for i in tasks)  # ensure input is a generator

    # pre-fill queue
    results = [executor.submit(core, t) for t in itertools.islice(tasks, queue)]

    while results:
        future = next(executor.as_completed(results))  # block

        results.remove(future)  # pop completed

        task = next(tasks, None)
        if task is not None:
            results.append(executor.submit(core, *task))  # queue another

        yield executor.result(future)


@click.command(name='output_products')
@click.option('--app-config', '-c', 'stats_config_file',
              type=click.Path(exists=True, readable=True, writable=False, dir_okay=False),
              help='configuration file location', callback=to_pathlib)
@ui.global_cli_options
@ui.executor_cli_options
@ui.pass_index(app_name='agdc-output_products')
def main(index, stats_config_file, executor):
    app = StatsApp.from_file(stats_config_file)

    output_products = app.make_output_products(index)
    # TODO: Store output products in database

    completed_tasks = app.run(index, executor, output_products)

    for ds in completed_tasks:
        print('Completed: %s' % ds)
        # index.datasets.add(ds, skip_sources=True)  # index completed work
        # TODO: Record new datasets in database


if __name__ == '__main__':
    main()
