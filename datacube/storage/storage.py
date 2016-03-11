# coding=utf-8
"""
Create/store dataset data into storage units based on the provided storage mappings
"""
from __future__ import absolute_import, division, print_function

import logging
from contextlib import contextmanager
from datetime import datetime
from itertools import groupby

import yaml

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper
import dateutil.parser
from dateutil.tz import tzutc
import numpy
import rasterio.warp
from rasterio.warp import RESAMPLING

from datacube import compat
from datacube.model import StorageUnit, GeoBox, Variable, _uri_to_local_path, time_coordinate_value
from datacube.storage import netcdf_writer
from datacube.utils import namedtuples2dicts, attrs_all_equal
from datacube.storage.access.core import StorageUnitBase, StorageUnitDimensionProxy, StorageUnitStack
from datacube.storage.access.backends import NetCDF4StorageUnit, GeoTifStorageUnit

_LOG = logging.getLogger(__name__)

RESAMPLING_METHODS = {
    'nearest': RESAMPLING.nearest,
    'cubic': RESAMPLING.cubic,
    'bilinear': RESAMPLING.bilinear,
    'cubic_spline': RESAMPLING.cubic_spline,
    'lanczos': RESAMPLING.lanczos,
    'average': RESAMPLING.average,
}


class WarpingStorageUnit(StorageUnitBase):
    def __init__(self, datasets, geobox, mapping, fuse_func=None):
        if not datasets:
            raise ValueError('Shall not make empty StorageUnit')

        self._datasets = datasets
        self.geobox = geobox
        self._varmap = {name: attrs['src_varname'] for name, attrs in mapping.items()}
        self._mapping = mapping
        self._fuse_func = fuse_func

        self.coord_data = self.geobox.coordinate_labels
        self.coordinates = self.geobox.coordinates

        self.variables = {
            name: Variable(numpy.dtype(attrs['dtype']),
                           attrs.get('nodata', None),
                           self.geobox.dimensions,
                           attrs['units'])
            for name, attrs in mapping.items()
            }
        self.variables['extra_metadata'] = Variable(numpy.dtype('S30000'), None, tuple(), None)

    @property
    def crs(self):
        return self.geobox.crs

    @property
    def affine(self):
        return self.geobox.affine

    @property
    def extent(self):
        return self.geobox.extent

    def _get_coord(self, dim):
        return self.coord_data[dim]

    def _fill_data(self, name, index, dest):
        if name == 'extra_metadata':
            docs = yaml.dump_all([doc.metadata_doc for doc in self._datasets], Dumper=SafeDumper, encoding='utf-8')
            numpy.copyto(dest, docs)
        else:
            src_variable_name = self._varmap[name]
            resampling = RESAMPLING_METHODS[self._mapping[name]['resampling_method']]
            sources = [DatasetSource(dataset, src_variable_name) for dataset in self._datasets]
            fuse_sources(sources,
                         dest,
                         self.geobox[index].affine,  # NOTE: Overloaded GeoBox.__getitem__
                         self.geobox.crs_str,
                         self.variables[name].nodata,
                         resampling=resampling,
                         fuse_func=self._fuse_func)
        return dest


# TODO: global_attributes and variable_attributes should be members of access_unit
def write_access_unit_to_netcdf(access_unit, global_attributes, variable_attributes, variable_params, filename):
    """
    Write access.StorageUnit to NetCDF4.
    :param access_unit:
    :param global_attributes: key value pairs to write as global attributes
    :param variable_attributes: mapping of variable name to key-value pairs
    :param variable_params: mapping of variable name to netcdf variable creation params
    :param filename: output filename
    :return:

    :type access_unit: datacube.storage.access.StorageUnitBase
    """
    nco = netcdf_writer.create_netcdf(filename)
    for name, coord in access_unit.coordinates.items():
        coord_var = netcdf_writer.create_coordinate(nco, name, coord)
        coord_var[:] = access_unit.get_coord(name)[0]
    netcdf_writer.create_grid_mapping_variable(nco, access_unit.crs)
    if hasattr(access_unit, 'affine'):
        netcdf_writer.write_gdal_attributes(nco, access_unit.crs, access_unit.affine)
    netcdf_writer.write_geographical_extents_attributes(nco, access_unit.extent.to_crs('EPSG:4326').points)

    for name, variable in access_unit.variables.items():
        # Create variable
        var_params = variable_params.get(name, {})
        data_var = netcdf_writer.create_variable(nco, name, variable, **var_params)

        # Write data
        data_var[:] = netcdf_writer.netcdfy_data(access_unit.get(name).values)

        # Write extra attributes
        for key, value in variable_attributes.get(name, {}).items():
            if key == 'flags_definition':
                netcdf_writer.write_flag_definition(data_var, value)
            else:
                setattr(data_var, key, value)

    # write global atrributes
    for key, value in global_attributes.items():
        setattr(nco, key, value)
    nco.close()


def _accesss_unit_descriptor(access_unit, **stuff):
    geo_bounds = access_unit.extent.to_crs('EPSG:4326').boundingbox
    extents = {
        'geospatial_lat_min': geo_bounds.bottom,
        'geospatial_lat_max': geo_bounds.top,
        'geospatial_lon_min': geo_bounds.left,
        'geospatial_lon_max': geo_bounds.right,
        'time_min': datetime.fromtimestamp(access_unit.coordinates['time'].begin, tzutc()),
        'time_max': datetime.fromtimestamp(access_unit.coordinates['time'].end, tzutc())
    }
    coordinates = access_unit.coordinates
    descriptor = dict(coordinates=namedtuples2dicts(coordinates), extents=extents)
    descriptor.update(stuff)
    return descriptor


def create_storage_unit_from_datasets(tile_index, datasets, storage_type, output_uri):
    """
    Create storage unit at `tile_index` for datasets using mapping

    :param tile_index: X,Y index of the storage unit
    :type tile_index: tuple[int, int]
    :type datasets:  list[datacube.model.Dataset]
    :type storage_type:  datacube.model.StorageType
    :rtype: datacube.storage.access.core.StorageUnitBase
    """
    datasets_grouped_by_time = _group_datasets_by_time(datasets)
    geobox = GeoBox.from_storage_type(storage_type, tile_index)

    storage_units = [StorageUnitDimensionProxy(
        WarpingStorageUnit(group, geobox, mapping=storage_type.measurements),
        time_coordinate_value(time))
                     for time, group in datasets_grouped_by_time]
    access_unit = StorageUnitStack(storage_units=storage_units, stack_dim='time')

    su_filename = _uri_to_local_path(output_uri)
    try:
        su_filename.parent.mkdir(parents=True)
    except OSError:
        pass

    write_access_unit_to_netcdf(access_unit,
                                storage_type.global_attributes,
                                storage_type.variable_attributes,
                                storage_type.variable_params,
                                str(su_filename))

    descriptor = _accesss_unit_descriptor(access_unit, tile_index=tile_index)
    return StorageUnit([dataset.id for dataset in datasets],
                       storage_type,
                       descriptor,
                       output_uri=output_uri)


def storage_unit_to_access_unit(storage_unit):
    """
    :type storage_units: datacube.model.StorageUnit
    :rtype: datacube.storage.access.core.StorageUnitBase
    """
    coordinates = storage_unit.coordinates
    variables = {
        name: Variable(
            dtype=numpy.dtype(attributes['dtype']),
            nodata=attributes.get('nodata', None),
            dimensions=storage_unit.storage_type.dimensions,
            units=attributes['units'])
        for name, attributes in storage_unit.storage_type.measurements.items()
        }
    if storage_unit.storage_type.driver == 'NetCDF CF':
        variables['extra_metadata'] = Variable(numpy.dtype('S30000'), None, ('time',), None)
        return NetCDF4StorageUnit(storage_unit.local_path, coordinates=coordinates, variables=variables)

    if storage_unit.storage_type.driver == 'GeoTiff':
        result = GeoTifStorageUnit(storage_unit.local_path, coordinates=coordinates, variables=variables)
        time = datetime.datetime.strptime(storage_unit.descriptor['extents']['time_min'], '%Y-%m-%dT%H:%M:%S.%f')
        return StorageUnitDimensionProxy(result, time_coordinate_value(time))

    raise RuntimeError('unsupported storage unit access driver %s' % storage_unit.storage_type.driver)


def stack_storage_units(storage_units, output_uri):
    """
    :type storage_units: list[datacube.model.StorageUnit]
    :return:
    """
    if not attrs_all_equal(storage_units, 'storage_type'):
        raise TypeError('all storage units must have the same storage type')
    if not attrs_all_equal(storage_units, 'tile_index'):
        raise TypeError('all storage units must have the same tile index')

    tile_index = storage_units[0].tile_index
    storage_type = storage_units[0].storage_type
    access_units = [storage_unit_to_access_unit(su) for su in storage_units]
    access_unit = StorageUnitStack(storage_units=access_units, stack_dim='time')
    geobox = GeoBox.from_storage_type(storage_type, tile_index)
    access_unit.crs = geobox.crs
    access_unit.affine = geobox.affine
    access_unit.extent = geobox.extent

    write_access_unit_to_netcdf(access_unit,
                                storage_type.global_attributes,
                                storage_type.variable_attributes,
                                storage_type.variable_params,
                                str(_uri_to_local_path(output_uri)))

    descriptor = _accesss_unit_descriptor(access_unit, tile_index=tile_index)
    return StorageUnit([id_ for su in storage_units for id_ in su.dataset_ids],
                       storage_type,
                       descriptor,
                       output_uri=output_uri)


def _group_datasets_by_time(datasets):
    return [(time, list(group)) for time, group in groupby(datasets, lambda ds: ds.time)]


def _rasterio_resampling_method(measurement_descriptor):
    return RESAMPLING_METHODS[measurement_descriptor['resampling_method'].lower()]


def generate_filename(tile_index, datasets, storage_type):
    return storage_type.generate_uri(
        tile_index=tile_index,
        start_time=_parse_time(datasets[0].time).strftime('%Y%m%d%H%M%S%f'),
        end_time=_parse_time(datasets[-1].time).strftime('%Y%m%d%H%M%S%f'),
    )


def _parse_time(time):
    if isinstance(time, compat.string_types):
        return dateutil.parser.parse(time)
    return time


def fuse_sources(sources, destination, dst_transform, dst_projection, dst_nodata,
                 resampling=RESAMPLING.nearest, fuse_func=None):
    def reproject(source, dest):
        with source.open() as src:
            rasterio.warp.reproject(src,
                                    dest,
                                    src_transform=source.transform,
                                    src_crs=source.crs,
                                    src_nodata=source.nodata,
                                    dst_transform=dst_transform,
                                    dst_crs=dst_projection,
                                    dst_nodata=dst_nodata,
                                    resampling=resampling,
                                    NUM_THREADS=4)

    def copyto_fuser(dest, src):
        numpy.copyto(dest, src, where=(src != dst_nodata))

    fuse_func = fuse_func or copyto_fuser

    if len(sources) == 1:
        reproject(sources[0], destination)
        return destination

    destination.fill(dst_nodata)
    if len(sources) == 0:
        return destination

    buffer_ = numpy.empty(destination.shape, dtype=destination.dtype)
    for source in sources:
        reproject(source, buffer_)
        fuse_func(destination, buffer_)

    return destination


class DatasetSource(object):
    def __init__(self, dataset, measurement_id):
        dataset_measurement_descriptor = dataset.metadata.measurements_dict[measurement_id]
        self._filename = str(dataset.local_path.parent.joinpath(dataset_measurement_descriptor['path']))
        self._band_id = dataset_measurement_descriptor.get('layer', 1)
        self.transform = None
        self.crs = None
        self.nodata = None
        self.format = dataset.format

    @contextmanager
    def open(self):
        for nasty_format in ('netcdf', 'hdf'):
            if nasty_format in self.format.lower():
                filename = '%s:"%s":%s' % (self.format, self._filename, self._band_id)
                bandnumber = 1
                break
        else:
            filename = self._filename
            bandnumber = self._band_id

        try:
            _LOG.debug("openening %s, band %s", filename, bandnumber)
            with rasterio.open(filename) as src:
                self.transform = src.affine
                self.crs = src.crs
                self.nodata = src.nodatavals[0] or (0 if self.format == 'JPEG2000' else None)  # TODO: sentinel 2 hack
                yield rasterio.band(src, bandnumber)
        except Exception as e:
            _LOG.error("Error opening source dataset: %s", filename)
            raise e
