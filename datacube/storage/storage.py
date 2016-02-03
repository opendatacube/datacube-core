# coding=utf-8
"""
Create/store dataset data into storage units based on the provided storage mappings
"""
from __future__ import absolute_import, division, print_function

import logging
import tempfile
import os.path
from contextlib import contextmanager
from itertools import groupby

import dateutil.parser
import numpy
from osgeo import ogr, osr
import rasterio.warp
from rasterio.coords import BoundingBox
from rasterio.warp import RESAMPLING, transform_bounds

from datacube import compat
from datacube.model import StorageUnit, GeoBox, Coordinate
from datacube.storage.netcdf_writer import create_netcdf_writer
from datacube.storage.utils import namedtuples2dicts, datetime_to_seconds_since_1970

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
    tiles = {}
    for dataset in datasets:
        dataset_proj = dataset.crs
        dataset_bounds = dataset.bounds
        bounds = bounds_override or BoundingBox(*transform_bounds(dataset_proj, grid_proj, *dataset_bounds))

        for y in range(int(bounds.bottom // grid_size[1]), int(bounds.top // grid_size[1]) + 1):
            for x in range(int(bounds.left // grid_size[0]), int(bounds.right // grid_size[0]) + 1):
                tile_index = (x, y)
                if _check_intersect(tile_index, grid_size, grid_proj, dataset_bounds, dataset_proj):
                    tiles.setdefault(tile_index, []).append(dataset)

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


def create_storage_unit_from_datasets(tile_index, datasets, storage_type, output_uri):
    """
    Create storage unit at `tile_index` for datasets using mapping

    :param tile_index: X,Y index of the storage unit
    :type tile_index: tuple[int, int]
    :type datasets:  list[datacube.model.Dataset]
    :type storage_type:  datacube.model.StorageType
    :param output_uri: URI specifying filename, must be file:// (for now)
    :type output_filename:  str
    :rtype: datacube.model.StorageUnit
    """
    if not datasets:
        raise ValueError('Shall not create empty StorageUnit%s %s' % (tile_index, output_uri))


    datasets_grouped_by_time = _group_datasets_by_time(datasets)
    time_values = [time for time, _ in datasets_grouped_by_time]

    geobox = GeoBox.from_storage_type(storage_type, tile_index)
    geo_bounds = geobox.geographic_boundingbox
    extents = {
        'geospatial_lat_min': geo_bounds.bottom,
        'geospatial_lat_max': geo_bounds.top,
        'geospatial_lon_min': geo_bounds.left,
        'geospatial_lon_max': geo_bounds.right,
        'time_min': time_values[0],
        'time_max': time_values[-1]
    }
    coordinates = geobox.coordinates
    coordinates['time'] = Coordinate(numpy.dtype(numpy.float64),
                                     datetime_to_seconds_since_1970(time_values[0]),
                                     datetime_to_seconds_since_1970(time_values[-1]),
                                     len(time_values),
                                     'seconds since 1970-01-01 00:00:00')

    descriptor = dict(coordinates=namedtuples2dicts(coordinates), extents=extents, tile_index=tile_index)

    su = StorageUnit([dataset.id for dataset in datasets],
                     storage_type,
                     descriptor,
                     storage_type.local_uri_to_location_relative_path(output_uri))

    if su.storage_type.driver != 'NetCDF CF':
        raise ValueError('Storage driver is not supported (yet): %s' % storage_type.driver)
    write_storage_unit_to_disk(su, datasets)

    return su


def write_storage_unit_to_disk(storage_unit, datasets):
    """
    :type storage_unit: datacube.model.StorageUnit
    """
    data_provider = GroupDatasetsByTimeDataProvider.from_storage_unit(storage_unit, datasets)
    time_values = data_provider.get_time_values()
    output_filename = storage_unit.local_path

    if output_filename.exists():
        raise RuntimeError('file already exists: %s' % output_filename)

    _LOG.info("Creating Storage Unit %s", output_filename)

    tmpfile, tmpfilename = tempfile.mkstemp(dir=str(output_filename.parent))
    try:
        with create_netcdf_writer(tmpfilename, data_provider.geobox) as su_writer:
            su_writer.create_time_values(time_values)

            for time_index, docs in data_provider.get_metadata_documents():
                su_writer.add_source_metadata(time_index, docs)

            for measurement_descriptor, chunking, data in data_provider.get_measurements():
                output_var = su_writer.ensure_variable(measurement_descriptor, chunking)
                for time_index, buffer_ in enumerate(data):
                    output_var[time_index] = buffer_

        os.close(tmpfile)
        os.rename(tmpfilename, str(output_filename))
    finally:
        try:
            os.unlink(tmpfilename)
        except OSError:
            pass


class GroupDatasetsByTimeDataProvider(object):
    """
    :type storage_type: datacube.model.StorageType
    :type geobox: datacube.model.GeoBox
    """
    def __init__(self, datasets, tile_index, storage_type):
        self.datasets_grouped_by_time = _group_datasets_by_time(datasets)
        self._warn_if_mosaiced_datasets(tile_index)
        self.geobox = GeoBox.from_storage_type(storage_type, tile_index)
        self.storage_type = storage_type

    @classmethod
    def from_storage_unit(cls, storage_unit, datasets):
        return cls(datasets, storage_unit.tile_index, storage_unit.storage_type)

    def _warn_if_mosaiced_datasets(self, tile_index):
        for time, group in self.datasets_grouped_by_time:
            if len(group) > 1:
                _LOG.warning("Mosaicing multiple datasets %s@%s: %s", tile_index, time, group)

    def get_time_values(self):
        return [time for time, _ in self.datasets_grouped_by_time]

    def get_metadata_documents(self):
        for time_index, (_, group) in enumerate(self.datasets_grouped_by_time):
            yield time_index, (dataset.metadata_doc for dataset in group)

    def _fused_data(self, measurement_id):
        measurement_descriptor = self.storage_type.measurements[measurement_id]
        shape = self.geobox.shape
        buffer_ = numpy.empty(shape, dtype=measurement_descriptor['dtype'])
        nodata = measurement_descriptor.get('nodata')
        for time_index, (_, time_group) in enumerate(self.datasets_grouped_by_time):
            buffer_ = fuse_sources([DatasetSource(dataset, measurement_id) for dataset in time_group],
                                   buffer_,
                                   self.geobox.affine,
                                   self.geobox.crs_str,
                                   nodata,
                                   resampling=_rasterio_resampling_method(measurement_descriptor))
            yield buffer_

    def get_measurements(self):
        measurements = self.storage_type.measurements
        chunking = self.storage_type.chunking
        for measurement_id, measurement_descriptor in measurements.items():
            yield measurement_descriptor, chunking, self._fused_data(measurement_id)


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
