from collections import OrderedDict
import logging
from functools import reduce as reduce_
from pathlib import Path

import numpy
import rasterio
import xarray
from datacube.model import Coordinate, Variable, GeoPolygon
from datacube.model.utils import make_dataset, xr_apply, datasets_to_doc
from datacube.storage import netcdf_writer
from datacube.storage.storage import create_netcdf_storage_unit
from datacube.utils import unsqueeze_data_array

_LOG = logging.getLogger(__name__)
STANDARD_VARIABLE_PARAM_NAMES = {'zlib',
                                 'complevel',
                                 'shuffle',
                                 'fletcher32',
                                 'contiguous',
                                 'attrs'}


class OutputDriver(object):
    # TODO: Add check for valid filename extensions in each driver
    def __init__(self, config, task, app_info=None):
        self.task = task
        self.config = config

        self.output_files = {}
        self.app_info = app_info

    def close_files(self):
        for output_file in self.output_files.values():
            output_file.close()

    def open_output_files(self):
        raise NotImplementedError

    def write_data(self, prod_name, measurement_name, tile_index, values):
        raise NotImplementedError

    def _get_dtype(self, out_prod_name, measurement_name):
        return self.task.output_products[out_prod_name].product.measurements[measurement_name]['dtype']

    def __enter__(self):
        self.open_output_files()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_files()


class NetcdfOutputDriver(OutputDriver):
    """
    Write data to Datacube compatible NetCDF files
    """
    valid_extensions = ['nc']

    def open_output_files(self):
        for prod_name, stat in self.task.output_products.items():
            filename_template = str(Path(self.config.location, stat.definition['file_path_template']))
            output_filename = _format_filename(filename_template, **self.task)
            self.output_files[prod_name] = self._create_storage_unit(stat, output_filename)

    def _create_storage_unit(self, stat, output_filename):
        geobox = self.task.geobox
        all_measurement_defns = list(stat.product.measurements.values())

        datasets, sources = _find_source_datasets(self.task, stat, geobox, self.app_info, uri=output_filename.as_uri())

        variable_params = self._create_netcdf_var_params(stat)
        nco = self._nco_from_sources(sources,
                                     geobox,
                                     all_measurement_defns,
                                     variable_params,
                                     output_filename)

        netcdf_writer.create_variable(nco, 'dataset', datasets, zlib=True)
        nco['dataset'][:] = netcdf_writer.netcdfy_data(datasets.values)
        return nco

    @staticmethod
    def _create_netcdf_var_params(stat):
        chunking = stat.storage['chunking']
        chunking = [chunking[dim] for dim in stat.storage['dimension_order']]

        variable_params = {}
        for measurement in stat.data_measurements:
            name = measurement['name']
            variable_params[name] = {k: v for k, v in stat.definition.items() if k in STANDARD_VARIABLE_PARAM_NAMES}
            variable_params[name]['chunksizes'] = chunking
            variable_params[name].update({k: v for k, v in measurement.items() if k in STANDARD_VARIABLE_PARAM_NAMES})
        return variable_params

    @staticmethod
    def _nco_from_sources(sources, geobox, measurements, variable_params, filename):
        coordinates = OrderedDict((name, Coordinate(coord.values, coord.units))
                                  for name, coord in sources.coords.items())
        coordinates.update(geobox.coordinates)

        variables = OrderedDict((variable['name'], Variable(dtype=numpy.dtype(variable['dtype']),
                                                            nodata=variable['nodata'],
                                                            dims=sources.dims + geobox.dimensions,
                                                            units=variable['units']))
                                for variable in measurements)

        return create_netcdf_storage_unit(filename, geobox.crs, coordinates, variables, variable_params)

    def write_data(self, prod_name, measurement_name, tile_index, values):
        self.output_files[prod_name][measurement_name][(0,) + tile_index[1:]] = netcdf_writer.netcdfy_data(values)
        self.output_files[prod_name].sync()
        _LOG.debug("Updated %s %s", measurement_name, tile_index[1:])


class RioOutputDriver(OutputDriver):
    """
    Save data to file/s using rasterio. Eg. GeoTiff
    """
    valid_extensions = ['tif', 'tiff']

    def open_output_files(self):
        for prod_name, stat in self.task.output_products.items():
            for measurename, measure_def in stat.product.measurements.items():
                geobox = self.task.geobox
                filename_template = str(Path(self.config.location, stat.definition['file_path_template']))

                output_filename = _format_filename(filename_template,
                                                   var_name=measurename,
                                                   config=self.config,
                                                   **self.task)
                try:
                    output_filename.parent.mkdir(parents=True)
                except OSError:
                    pass

                profile = {
                    'blockxsize': self.config.storage['chunking']['x'],
                    'blockysize': self.config.storage['chunking']['y'],
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

    def write_data(self, prod_name, measurement_name, tile_index, values):
        output_name = prod_name + measurement_name
        y, x = tile_index[1:]
        window = ((y.start, y.stop), (x.start, x.stop))
        _LOG.debug("Updating %s.%s %s", prod_name, measurement_name, window)

        dtype = self._get_dtype(prod_name, measurement_name)

        self.output_files[output_name].write(values.astype(dtype), indexes=1, window=window)


def _format_filename(path_template, **kwargs):
    return Path(str(path_template).format(**kwargs))


def _find_source_datasets(task, stat, geobox, app_info, uri=None):
    def _make_dataset(labels, sources):
        return make_dataset(product=stat.product,
                            sources=sources,
                            extent=geobox.extent,
                            center_time=labels['time'],
                            uri=uri,
                            app_info=app_info,
                            valid_data=GeoPolygon.from_sources_extents(sources, geobox))

    def merge_sources(prod):
        if stat.masked:
            all_sources = xarray.align(prod['data'].sources, *[mask_tile.sources for mask_tile in prod['masks']])
            return reduce_(lambda a, b: a + b, (sources.sum() for sources in all_sources))
        else:
            return prod['data'].sources.sum()

    start_time, _ = task.time_period
    sources = reduce_(lambda a, b: a + b, (merge_sources(prod) for prod in task.sources))
    sources = unsqueeze_data_array(sources, dim='time', pos=0, coord=start_time,
                                   attrs=task.time_attributes)

    datasets = xr_apply(sources, _make_dataset, dtype='O')  # Store in DataArray to associate Time -> Dataset
    datasets = datasets_to_doc(datasets)
    return datasets, sources
