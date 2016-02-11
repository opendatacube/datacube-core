# coding=utf-8
"""
Create/store dataset data into storage units based on the provided storage mappings
"""
from __future__ import absolute_import, division, print_function

import logging
from collections import defaultdict
from contextlib import contextmanager
from itertools import groupby
from datetime import datetime

import yaml

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper
import dateutil.parser
from dateutil.tz import tzutc
import numpy
from osgeo import ogr, osr
import rasterio.warp
from rasterio.coords import BoundingBox
from rasterio.warp import RESAMPLING, transform_bounds

from datacube import compat
from datacube.model import StorageUnit, GeoBox, Variable, _uri_to_local_path, time_coordinate_value
from datacube.storage import netcdf_writer
from datacube.utils import namedtuples2dicts
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


def tile_datasets_with_storage_type(datasets, storage_type):
    """
    compute indexes of tiles covering the datasets, as well as
    which datasets comprise which tiles

    :type datasets:  list[datacube.model.Dataset]
    :type storage_type:  datacube.model.StorageType
    :rtype: dict[tuple[int, int], list[datacube.model.Dataset]]
    """
    datasets = sort_datasets_by_time(datasets)
    bounds_override = storage_type.roi and _roi_to_bounds(storage_type.roi, storage_type.spatial_dimensions)
    return _grid_datasets(datasets, bounds_override, storage_type.crs, storage_type.tile_size)


def sort_datasets_by_time(datasets):
    datasets.sort(key=lambda ds: ds.time)
    return datasets


def _roi_to_bounds(roi, dims):
    return BoundingBox(roi[dims[0]][0], roi[dims[1]][0], roi[dims[0]][1], roi[dims[1]][1])


def _grid_datasets(datasets, bounds_override, grid_proj, grid_size):
    tiles = defaultdict(list)
    for dataset in datasets:
        dataset_proj = dataset.crs
        dataset_bounds = dataset.bounds
        bounds = bounds_override or BoundingBox(*transform_bounds(dataset_proj, grid_proj, *dataset_bounds))

        for y in range(int(bounds.bottom // grid_size[1]), int(bounds.top // grid_size[1]) + 1):
            for x in range(int(bounds.left // grid_size[0]), int(bounds.right // grid_size[0]) + 1):
                tile_index = (x, y)
                if _check_intersect(tile_index, grid_size, grid_proj, dataset_bounds, dataset_proj):
                    tiles[tile_index].append(dataset)

    return tiles


def _check_intersect(tile_index, tile_size, tile_crs, dataset_bounds, dataset_crs):
    tile_sr = osr.SpatialReference()
    tile_sr.SetFromUserInput(tile_crs)
    dataset_sr = osr.SpatialReference()
    dataset_sr.SetFromUserInput(dataset_crs)
    transform = osr.CoordinateTransformation(tile_sr, dataset_sr)

    tile_poly = _poly_from_bounds(tile_index[0] * tile_size[0],
                                  tile_index[1] * tile_size[1],
                                  (tile_index[0] + 1) * tile_size[0],
                                  (tile_index[1] + 1) * tile_size[1],
                                  32)
    tile_poly.Transform(transform)

    return tile_poly.Intersects(_poly_from_bounds(*dataset_bounds))


def _poly_from_bounds(left, bottom, right, top, segments=None):
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(left, bottom)
    ring.AddPoint(left, top)
    ring.AddPoint(right, top)
    ring.AddPoint(right, bottom)
    ring.AddPoint(left, bottom)
    if segments:
        ring.Segmentize(2 * (right + top - left - bottom) / segments)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly


class WarpingStorageUnit(StorageUnitBase):
    def __init__(self, datasets, geobox, mapping, fuse_func=None):
        if not datasets:
            raise ValueError('Shall not make empty StorageUnit')

        self._datasets = datasets
        self.geobox = geobox
        self._varmap = {attrs['varname']: name for name, attrs in mapping.items()}
        self._mapping = mapping
        self._fuse_func = fuse_func

        self.coord_data = self.geobox.coordinate_labels
        self.coordinates = self.geobox.coordinates

        self.variables = {
            attrs['varname']: Variable(numpy.dtype(attrs['dtype']),
                                       attrs.get('nodata', None),
                                       self.geobox.dimensions,
                                       attrs.get('units', '1'))
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
            measurement_id = self._varmap[name]
            resampling = RESAMPLING_METHODS[self._mapping[measurement_id]['resampling_method']]
            sources = [DatasetSource(dataset, measurement_id) for dataset in self._datasets]
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
    if getattr(access_unit, 'affine'):
        netcdf_writer.write_gdal_attributes(nco, access_unit.crs, access_unit.affine)
    netcdf_writer.write_geographical_extents_attributes(nco, access_unit.extent.to_crs('EPSG:4326').points)

    for name, variable in access_unit.variables.items():
        var_params = variable_params.get(name, {})
        data_var = netcdf_writer.create_variable(nco, name, variable, **var_params)
        data_var[:] = netcdf_writer.netcdfy_data(access_unit.get(name).values)

        # write extra attributes
        for key, value in variable_attributes.get(name, {}).items():
            setattr(data_var, key, value)

    # write global atrributes
    for key, value in global_attributes.items():
        nco.setncattr(key, value)
    nco.close()


def _accesss_unit_descriptor(access_unit, **stuff):
    geo_bounds = access_unit.geobox.geographic_extent.boundingbox
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

    write_access_unit_to_netcdf(access_unit,
                                storage_type.global_attributes,
                                storage_type.variable_attributes,
                                storage_type.variable_params,
                                str(_uri_to_local_path(output_uri)))

    descriptor = _accesss_unit_descriptor(access_unit, tile_index=tile_index)
    return StorageUnit([dataset.id for dataset in datasets],
                       storage_type,
                       descriptor,
                       output_uri=output_uri)


def _group_datasets_by_time(datasets):
    return [(time, list(group)) for time, group in groupby(datasets, lambda ds: ds.time)]


def _rasterio_resampling_method(measurement_descriptor):
    return RESAMPLING_METHODS[measurement_descriptor['resampling_method'].lower()]


def generate_filename(tile_index, datasets, mapping):
    merged = {
        'tile_index': tile_index,
        'mapping_id': mapping.id_,
        'start_time': _parse_time(datasets[0].time),
        'end_time': _parse_time(datasets[-1].time),
    }
    merged.update(mapping.match.metadata)

    return mapping.storage_pattern.format(**merged)


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
