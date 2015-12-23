# coding=utf-8
"""
Create/store dataset data into storage units based on the provided storage mappings
"""
from __future__ import absolute_import, division, print_function

import logging

from contextlib import contextmanager
from itertools import groupby
import os.path
import tempfile

import dateutil.parser
import numpy

from osgeo import ogr, osr
import rasterio.warp
from rasterio.warp import RESAMPLING, transform_bounds
from rasterio.coords import BoundingBox
from affine import Affine
from datacube.storage.utils import ensure_path_exists
from .netcdf_writer import NetCDFWriter

from datacube import compat
from datacube.model import StorageUnit, TileSpec
from datacube.storage.netcdf_indexer import index_netcdfs

_LOG = logging.getLogger(__name__)


def _parse_time(time):
    if isinstance(time, compat.string_types):
        return dateutil.parser.parse(time)
    return time


def generate_filename(tile_index, mapping, datasets):
    merged = {
        'tile_index': tile_index,
        'start_time': _parse_time(datasets[0].metadata_doc['extent']['from_dt']),
        'end_time': _parse_time(datasets[-1].metadata_doc['extent']['to_dt']),
    }
    merged.update(mapping.match.metadata)

    return mapping.storage_pattern.format(**merged)


def _dataset_bounds(dataset):
    geo_ref_points = dataset.metadata_doc['grid_spatial']['projection']['geo_ref_points']
    return BoundingBox(geo_ref_points['ll']['x'], geo_ref_points['ll']['y'],
                       geo_ref_points['ur']['x'], geo_ref_points['ur']['y'])


def _dataset_projection(dataset):
    projection = dataset.metadata_doc['grid_spatial']['projection']

    crs = projection.get('spatial_reference', None)
    if crs:
        return str(crs)

    # TODO: really need CRS specified properly in agdc-metadata.yaml
    if projection['datum'] == 'GDA94':
        return 'EPSG:283' + str(abs(projection['zone']))

    if projection['datum'] == 'WGS84':
        if projection['zone'][-1] == 'S':
            return 'EPSG:327' + str(abs(int(projection['zone'][:-1])))
        else:
            return 'EPSG:326' + str(abs(int(projection['zone'][:-1])))

    raise RuntimeError('Cant figure out the projection: %s %s' % (projection['datum'], projection['zone']))


def _poly_from_bounds(left, bottom, right, top, segments=None):
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(left, bottom)
    ring.AddPoint(left, top)
    ring.AddPoint(right, top)
    ring.AddPoint(right, bottom)
    ring.AddPoint(left, bottom)
    if segments:
        ring.Segmentize(2*(right+top-left-bottom)/segments)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly


def _check_intersect(tile_index, tile_size, tile_crs, dataset_bounds, dataset_crs):
    tile_sr = osr.SpatialReference()
    tile_sr.SetFromUserInput(tile_crs)
    dataset_sr = osr.SpatialReference()
    dataset_sr.SetFromUserInput(dataset_crs)
    transform = osr.CoordinateTransformation(tile_sr, dataset_sr)

    tile_poly = _poly_from_bounds(tile_index[0]*tile_size[0],
                                  tile_index[1]*tile_size[1],
                                  (tile_index[0]+1)*tile_size[0],
                                  (tile_index[1]+1)*tile_size[1],
                                  32)
    tile_poly.Transform(transform)

    return tile_poly.Intersects(_poly_from_bounds(*dataset_bounds))


def _grid_datasets(datasets, bounds_override, grid_proj, grid_size):
    tiles = {}
    for dataset in datasets:
        dataset_proj = _dataset_projection(dataset)
        dataset_bounds = _dataset_bounds(dataset)
        bounds = bounds_override or BoundingBox(*transform_bounds(dataset_proj, grid_proj, *dataset_bounds))

        for y in range(int(bounds.bottom//grid_size[1]), int(bounds.top//grid_size[1])+1):
            for x in range(int(bounds.left//grid_size[0]), int(bounds.right//grid_size[0])+1):
                tile_index = (x, y)
                if _check_intersect(tile_index, grid_size, grid_proj, dataset_bounds, dataset_proj):
                    tiles.setdefault(tile_index, []).append(dataset)

    return tiles


def _dataset_time(dataset):
    center_dt = dataset.metadata_doc['extent']['center_dt']
    if isinstance(center_dt, compat.string_types):
        center_dt = dateutil.parser.parse(center_dt)
    return center_dt


def _get_tile_transform(tile_index, tile_size, tile_res):
    x = (tile_index[0] + (1 if tile_res[0] < 0 else 0)) * tile_size[0]
    y = (tile_index[1] + (1 if tile_res[1] < 0 else 0)) * tile_size[1]
    return Affine(tile_res[0], 0.0, x, 0.0, tile_res[1], y)


def _create_data_variable(ncfile, measurement_descriptor, chunking):
    varname = measurement_descriptor['varname']
    dtype = measurement_descriptor['dtype']
    nodata = measurement_descriptor.get('nodata', None)
    units = measurement_descriptor.get('units', None)
    return ncfile.ensure_variable(varname, dtype, chunking, nodata, units)


def fuse_sources(sources, destination, dst_transform, dst_projection, dst_nodata,
                 resampling=RESAMPLING.nearest, fuse_func=None):
    def reproject(source, dest):
        with source.open() as src:
            rasterio.warp.reproject(src,
                                    dest,
                                    src_transform=source.transform,
                                    src_crs=source.projection,
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
        self._filename = str(dataset.metadata_path.parent.joinpath(dataset_measurement_descriptor['path']))
        self._band_id = dataset_measurement_descriptor.get('layer', 1)  # TODO: store band id in the MD doc
        self.transform = None
        self.projection = None
        self.nodata = None
        self.format = dataset.format
        if self.format == 'NetCDF4':
            self._band_id = 1  # TODO: this magically works for H8 netcdf

    @contextmanager
    def open(self):
        try:
            _LOG.debug("openening %s:%s", self._filename, self._band_id)
            with rasterio.open(self._filename, driver='JP2OpenJPEG') as src:
                self.transform = src.affine
                self.projection = src.crs
                self.nodata = src.nodatavals[0] or (0 if self.format == 'JPEG2000' else None)  # TODO: sentinel 2 hack
                yield rasterio.band(src, self._band_id)
        finally:
            src.close()


def _map_resampling(name):
    return {
        'nearest': RESAMPLING.nearest,
        'cubic': RESAMPLING.cubic,
        'bilinear': RESAMPLING.bilinear,
        'cubic_spline': RESAMPLING.cubic_spline,
        'lanczos': RESAMPLING.lanczos,
        'average': RESAMPLING.average,
    }[name.lower()]


def _roi_to_bounds(roi, dims):
    return BoundingBox(roi[dims[0]][0], roi[dims[1]][0], roi[dims[0]][1], roi[dims[1]][1])


def create_storage_unit(tile_index, datasets, mapping, filename):
    """
    Create storage unit at tile_index for datasets using mapping

    :type tile_index: tuple[int, int]
    :type datasets:  list[datacube.model.Dataset]
    :type mapping:  datacube.model.StorageMapping
    :type filename:  str
    :rtype: datacube.model.StorageUnit
    """
    storage_type = mapping.storage_type
    if storage_type.driver != 'NetCDF CF':
        raise RuntimeError('Storage driver is not supported (yet): %s' % storage_type.driver)

    if not filename.startswith('file://'):
        raise RuntimeError('URI protocol is not supported (yet): %s' % mapping.storage_pattern)

    filename = filename[7:]

    tile_size = storage_type.tile_size
    tile_res = storage_type.resolution
    tile_spec = TileSpec(storage_type.projection,
                         _get_tile_transform(tile_index, tile_size, tile_res),
                         width=int(tile_size[0] / abs(tile_res[0])),
                         height=int(tile_size[1] / abs(tile_res[1])))

    if os.path.isfile(filename):
        raise RuntimeError('file already exists: %s' % filename)

    tmpfile, tmpfilename = tempfile.mkstemp(dir=os.path.dirname(filename))
    try:
        _create_storage_unit(tile_index, datasets, mapping, tile_spec, tmpfilename)
        os.rename(tmpfilename, filename)
    finally:
        try:
            os.unlink(tmpfilename)
        except OSError:
            pass

    # TODO: move 'hardcoded' coordinate specs (name, units, etc) into tile_spec
    # TODO: then we can pull the descriptor out of the tile_spec
    # TODO: and netcdf writer will be more generic
    su_descriptor = index_netcdfs([filename])[filename]
    return StorageUnit([dataset.id for dataset in datasets],
                       mapping,
                       su_descriptor,
                       mapping.local_path_to_location_offset('file://'+filename))


def tile_datasets_with_mapping(datasets, mapping):
    """
    compute indexes of tiles covering the datasets, as well as
    which datasets comprise which tiles

    :type datasets:  list[datacube.model.Dataset]
    :type mapping:  datacube.model.StorageMapping
    :rtype: dict[tuple[int, int], list[datacube.model.Dataset]]
    """
    storage_type = mapping.storage_type
    bounds = mapping.roi and _roi_to_bounds(mapping.roi, storage_type.spatial_dimensions)
    return _grid_datasets(datasets, bounds, storage_type.projection, storage_type.tile_size)


def store_datasets_with_mapping(datasets, mapping):
    """
    Create storage units for datasets using mapping

    :type datasets:  list[datacube.model.Dataset]
    :type mapping:  datacube.model.StorageMapping
    :rtype: datacube.model.StorageUnit
    """
    datasets.sort(key=_dataset_time)
    for tile_index, datasets in tile_datasets_with_mapping(datasets, mapping).items():
        filename = generate_filename(tile_index, mapping, datasets)
        yield create_storage_unit(tile_index, datasets, mapping, filename)


def _create_storage_unit(tile_index, datasets, mapping, tile_spec, filename):
    dataset_groups = [(key, list(group)) for key, group in groupby(datasets, _dataset_time)]

    ncfile = NetCDFWriter(filename, tile_spec, len(dataset_groups))
    ncfile.set_time_values(group[0] for group in dataset_groups)

    for index, (time, group) in enumerate(dataset_groups):
        if len(group) > 1:
            _LOG.warning("Mosaicing multiple datasets %s@%s: %s", tile_index, time, group)
        # TODO: ncfile.extra_meta = json.dumps(group[0].metadata_doc)

    _fill_storage_unit(ncfile, dataset_groups, mapping.measurements, tile_spec, mapping.storage_type.chunking)

    ncfile.close()


def _fill_storage_unit(ncfile, dataset_groups, measurements, tile_spec, chunking):
    for measurement_id, measurement_descriptor in measurements.items():
        var, src_filename_var = _create_data_variable(ncfile, measurement_descriptor, chunking)

        buffer_ = numpy.empty(var.shape[1:], dtype=var.dtype)
        for index, (time_value, time_group) in enumerate(dataset_groups):
            fuse_sources([DatasetSource(dataset, measurement_id) for dataset in time_group],
                         buffer_,
                         tile_spec.affine,
                         tile_spec.projection,
                         getattr(var, '_FillValue', None),
                         resampling=_map_resampling(measurement_descriptor['resampling_method']))
            var[index] = buffer_
            # TODO: src_filename_var[index] = foo... is it needed??
