# coding=utf-8
"""
Create/store dataset data into storage units based on the provided storage mappings
"""
from __future__ import absolute_import, division, print_function

import logging
from contextlib import contextmanager
from pathlib import Path

from datacube.model import CRS
from datacube.storage import netcdf_writer
from datacube.config import OPTIONS
from datacube.utils import clamp, datetime_to_seconds_since_1970, uri_to_local_path
from datacube.compat import urlparse, urljoin

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper
import numpy

from affine import Affine
import rasterio.warp
import rasterio.crs

try:
    from rasterio.warp import Resampling
except ImportError:
    from rasterio.warp import RESAMPLING as Resampling


_LOG = logging.getLogger(__name__)

RESAMPLING_METHODS = {
    'nearest': Resampling.nearest,
    'cubic': Resampling.cubic,
    'bilinear': Resampling.bilinear,
    'cubic_spline': Resampling.cubic_spline,
    'lanczos': Resampling.lanczos,
    'average': Resampling.average,
}

assert str(rasterio.__version__) >= '0.34.0', "rasterio version 0.34.0 or higher is required"
GDAL_NETCDF_TIME = ('NETCDF_DIM_'
                    if str(rasterio.__gdal_version__) >= '1.10.0' else
                    'NETCDF_DIMENSION_') + 'time'


def _rasterio_resampling_method(resampling):
    return RESAMPLING_METHODS[resampling.lower()]


if str(rasterio.__version__) >= '0.36.0':
    def _rasterio_crs_wkt(src):
        return str(src.crs.wkt)
else:
    def _rasterio_crs_wkt(src):
        return str(src.crs_wkt)

if str(rasterio.__version__) >= '1.0':
    def _rasterio_transform(src):
        return src.transform
else:
    def _rasterio_transform(src):
        return src.affine


def _calc_offsets(off, src_size, dst_size):
    """
    >>> _calc_offsets(11, 10, 12) # no overlap
    (10, 0, 0)
    >>> _calc_offsets(-11, 12, 10) # no overlap
    (0, 10, 0)
    >>> _calc_offsets(5, 10, 12) # overlap
    (5, 0, 5)
    >>> _calc_offsets(-5, 12, 10) # overlap
    (0, 5, 5)
    >>> _calc_offsets(5, 10, 4) # containment
    (5, 0, 4)
    >>> _calc_offsets(-5, 4, 10) # containment
    (0, 5, 4)
    """
    read_off = clamp(off, 0, src_size)
    write_off = clamp(-off, 0, dst_size)
    size = min(src_size - read_off, dst_size - write_off)
    return read_off, write_off, size


def _no_scale(affine, eps=0.01):
    return abs(affine.a - 1.0) < eps and abs(affine.e - 1.0) < eps


def _no_fractional_translate(affine, eps=0.01):
    return abs(affine.c % 1.0) < eps and abs(affine.f % 1.0) < eps


def reproject(source, dest, dst_transform, dst_nodata, dst_projection, resampling):
    with source.open() as src:
        array_transform = ~source.transform * dst_transform
        if (source.crs == dst_projection and _no_scale(array_transform) and
                (resampling == Resampling.nearest or _no_fractional_translate(array_transform))):
            dydx = (int(round(array_transform.f)), int(round(array_transform.c)))
            read, write, shape = zip(*map(_calc_offsets, dydx, src.shape, dest.shape))

            dest.fill(dst_nodata)
            if all(shape):
                window = ((read[0], read[0] + shape[0]), (read[1], read[1] + shape[1]))
                tmp = src.ds.read(indexes=src.bidx, window=window)
                numpy.copyto(dest[write[0]:write[0] + shape[0], write[1]:write[1] + shape[1]],
                             tmp, where=(tmp != source.nodata))
        else:
            if source.override:
                src = src.ds.read(indexes=src.bidx)

            if dest.dtype == numpy.dtype('int8'):
                dest = dest.view(dtype='uint8')
                dst_nodata = dst_nodata.astype('uint8')

            rasterio.warp.reproject(src,
                                    dest,
                                    src_transform=source.transform,
                                    src_crs=str(source.crs),
                                    src_nodata=source.nodata,
                                    dst_transform=dst_transform,
                                    dst_crs=str(dst_projection),
                                    dst_nodata=dst_nodata,
                                    resampling=resampling,
                                    NUM_THREADS=OPTIONS['reproject_threads'])


def fuse_sources(sources, destination, dst_transform, dst_projection, dst_nodata, resampling='nearest', fuse_func=None):
    """
    Reproject and fuse `sources` into a 2D numpy array `destination`.
    """
    assert len(destination.shape) == 2

    resampling = _rasterio_resampling_method(resampling)

    def copyto_fuser(dest, src):
        numpy.copyto(dest, src, where=(src != dst_nodata))

    fuse_func = fuse_func or copyto_fuser

    if len(sources) == 1:
        reproject(sources[0], destination, dst_transform, dst_nodata, dst_projection, resampling)
        return destination

    destination.fill(dst_nodata)
    if len(sources) == 0:
        return destination

    buffer_ = numpy.empty(destination.shape, dtype=destination.dtype)
    for source in sources:
        reproject(source, buffer_, dst_transform, dst_nodata, dst_projection, resampling)
        fuse_func(destination, buffer_)

    return destination


class DatasetSource(object):
    """model.Dataset is about metadata, this takes a dataset and knows how to get the actual data bytes."""
    def __init__(self, dataset, measurement_id):
        """

        :type dataset: datacube.model.Dataset
        :param measurement_id:
        """
        self._dataset = dataset
        self._bandinfo = dataset.type.measurements[measurement_id]
        self._descriptor = dataset.measurements[measurement_id]
        self.override = False
        self.transform = None
        self.crs = dataset.crs
        self.dtype = None
        self.nodata = None
        self.format = dataset.format
        self.time = dataset.center_time
        self.local_uri = dataset.local_uri

    @contextmanager
    def open(self):
        filename, bandnumber = self.wheres_my_data()

        try:
            _LOG.debug("opening %s, band %s", filename, bandnumber)
            with rasterio.open(filename) as src:
                if bandnumber is None:
                    if 'netcdf' in self.format.lower():
                        bandnumber = self.wheres_my_band(src, self.time)
                    else:
                        bandnumber = 1

                self.transform = self.whats_my_transform(src)

                try:
                    self.crs = CRS(_rasterio_crs_wkt(src))
                except ValueError:
                    _LOG.warning('No CRS in %s, band %s. Falling back to dataset CRS. Gonna be slow...')
                    self.override = True  # HACK: mmmm... side effects! See reproject above

                self.dtype = numpy.dtype(src.dtypes[0])
                self.nodata = self.dtype.type(src.nodatavals[0] if src.nodatavals[0] is not None else
                                              self._bandinfo.get('nodata'))

                yield rasterio.band(src, bandnumber)

        except Exception as e:
            _LOG.error("Error opening source dataset: %s", filename)
            raise e

    def wheres_my_data(self):
        if self._descriptor['path']:
            url_str = self._descriptor['path']
            url = urlparse(url_str)
            if not url.scheme and not Path(url.path).is_absolute():
                url_str = urljoin(self.local_uri, self._descriptor['path'])
        else:
            url_str = self.local_uri
        url = urlparse(url_str)

        # if format is NETCDF of HDF need to pass NETCDF:path:band as filename to rasterio/GDAL
        for nasty_format in ('netcdf', 'hdf'):
            if nasty_format in self.format.lower():
                if url.scheme and url.scheme != 'file':
                    raise RuntimeError("Can't access %s over %s" % (self.format, url.scheme))
                filename = '%s:%s:%s' % (self.format, uri_to_local_path(url_str), self._descriptor['layer'])
                return filename, None

        if url.scheme and url.scheme != 'file':
            return url_str, self._descriptor.get('layer', 1)

        # if local path strip scheme and other gunk
        return str(uri_to_local_path(url_str)), self._descriptor.get('layer', 1)

    def wheres_my_band(self, src, time):
        if GDAL_NETCDF_TIME not in src.tags(1):
            _LOG.warning("NetCDF dataset has no time dimension")  # HACK: should support time-less datasets
            return 1

        sec_since_1970 = datetime_to_seconds_since_1970(time)

        idx = 0
        dist = float('+inf')
        for i in range(1, src.count + 1):
            v = float(src.tags(i)[GDAL_NETCDF_TIME])
            if abs(sec_since_1970 - v) < dist:
                idx = i
                dist = abs(sec_since_1970 - v)
        return idx

    def whats_my_transform(self, src):
        transform = _rasterio_transform(src)
        if not transform.is_identity:
            return transform

        # source probably doesn't have transform
        _LOG.warning('No GeoTransform in %s, band %s. Falling back to dataset GeoTransform. Gonna be slow...')
        self.override = True  # HACK: mmmm... side effects! See reproject above

        bounds = self._dataset.metadata.grid_spatial['geo_ref_points']
        width = bounds['lr']['x'] - bounds['ul']['x']
        height = bounds['lr']['y'] - bounds['ul']['y']
        return (Affine.translation(bounds['ul']['x'], bounds['ul']['y']) *
                Affine.scale(width / src.shape[1], height / src.shape[0]))


def create_netcdf_storage_unit(filename,
                               crs, coordinates, variables, variable_params, global_attributes=None,
                               netcdfparams=None):
    """
    Create a NetCDF file on disk.

    :param pathlib.Path filename: filename to write to
    :param datacube.model.CRS crs: Datacube CRS object defining the spatial projection
    :return: open netCDF4.Dataset object
    """
    if filename.exists():
        raise RuntimeError('Storage Unit already exists: %s' % filename)

    try:
        filename.parent.mkdir(parents=True)
    except OSError:
        pass

    _LOG.info('Creating storage unit: %s', filename)

    nco = netcdf_writer.create_netcdf(str(filename), **(netcdfparams or {}))

    for name, coord in coordinates.items():
        netcdf_writer.create_coordinate(nco, name, coord.values, coord.units)

    netcdf_writer.create_grid_mapping_variable(nco, crs)

    for name, variable in variables.items():
        set_crs = all(dim in variable.dims for dim in crs.dimensions)
        var_params = variable_params.get(name, {})
        data_var = netcdf_writer.create_variable(nco, name, variable, set_crs=set_crs, **var_params)

        for key, value in var_params.get('attrs', {}).items():
            setattr(data_var, key, value)

    for key, value in (global_attributes or {}).items():
        setattr(nco, key, value)

    return nco


def write_dataset_to_netcdf(dataset, filename, global_attributes=None, variable_params=None,
                            netcdfparams=None):
    """
    Write a Data Cube style xarray Dataset to a NetCDF file

    Requires a spatial Dataset, with attached coordinates and global crs attribute.

    :param `xarray.Dataset` dataset:
    :param filename: Output filename
    :param global_attributes: Global file attributes. dict of attr_name: attr_value
    :param variable_params: dict of variable_name: {param_name: param_value, [...]}
                            Allows setting storage and compression options per variable.
                            See the `netCDF4.Dataset.createVariable` for available
                            parameters.
    :param netcdfparams: Optional params affecting netCDF file creation
    """
    global_attributes = global_attributes or {}
    variable_params = variable_params or {}
    filename = Path(filename)

    nco = create_netcdf_storage_unit(filename,
                                     dataset.crs,
                                     dataset.coords,
                                     dataset.data_vars,
                                     variable_params,
                                     global_attributes,
                                     netcdfparams)

    for name, variable in dataset.data_vars.items():
        nco[name][:] = netcdf_writer.netcdfy_data(variable.values)

    nco.close()
