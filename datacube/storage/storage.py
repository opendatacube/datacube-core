# coding=utf-8
"""
Create/store dataset data into storage units based on the provided storage mappings
"""
from __future__ import absolute_import, division, print_function

import logging
from contextlib import contextmanager
from pathlib import Path

from datacube.model import Variable, CRS
from datacube.storage import netcdf_writer

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper
import numpy
import rasterio.warp
import rasterio.crs
from rasterio.warp import RESAMPLING

from datacube.utils import clamp, datetime_to_seconds_since_1970

_LOG = logging.getLogger(__name__)

RESAMPLING_METHODS = {
    'nearest': RESAMPLING.nearest,
    'cubic': RESAMPLING.cubic,
    'bilinear': RESAMPLING.bilinear,
    'cubic_spline': RESAMPLING.cubic_spline,
    'lanczos': RESAMPLING.lanczos,
    'average': RESAMPLING.average,
}

def _rasterio_resampling_method(measurement_descriptor):
    return RESAMPLING_METHODS[measurement_descriptor['resampling_method'].lower()]


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
    size = min(src_size-read_off, dst_size-write_off)
    return read_off, write_off, size


def fuse_sources(sources, destination, dst_transform, dst_projection, dst_nodata,
                 resampling=RESAMPLING.nearest, fuse_func=None):
    assert len(destination.shape) == 2

    def no_scale(affine, eps=0.01):
        return abs(affine.a - 1.0) < eps and abs(affine.e - 1.0) < eps

    def no_fractional_translate(affine, eps=0.01):
        return abs(affine.c % 1.0) < eps and abs(affine.f % 1.0) < eps

    def reproject(source, dest):
        with source.open() as src:
            array_transform = ~source.transform * dst_transform
            if (source.crs == dst_projection and no_scale(array_transform) and
                    (resampling == RESAMPLING.nearest or no_fractional_translate(array_transform))):
                dydx = (int(round(array_transform.f)), int(round(array_transform.c)))
                read, write, shape = zip(*map(_calc_offsets, dydx, src.shape, dest.shape))

                if all(shape):
                    # TODO: dtype and nodata conversion
                    assert src.dtype == dest.dtype
                    assert source.nodata == dst_nodata
                    window = ((read[0], read[0] + shape[0]), (read[1], read[1] + shape[1]))
                    dest[write[0]:write[0] + shape[0], write[1]:write[1] + shape[1]] = src.ds.read(indexes=src.bidx,
                                                                                                   window=window)
            else:
                # HACK: dtype shenanigans to make sure 'NaN' string gets translated to NaN value
                rasterio.warp.reproject(src,
                                        dest,
                                        src_transform=source.transform,
                                        src_crs=str(source.crs),
                                        src_nodata=numpy.dtype(src.dtype).type(source.nodata),
                                        dst_transform=dst_transform,
                                        dst_crs=str(dst_projection),
                                        dst_nodata=dest.dtype.type(dst_nodata),
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
        """

        :type dataset: datacube.model.Dataset
        :param measurement_id:
        """
        self._bandinfo = dataset.type.measurements[measurement_id]
        self._descriptor = dataset.measurements[measurement_id]
        self.transform = None
        self.crs = None
        self.nodata = None
        self.format = dataset.format
        self.time = dataset.center_time
        self.local_path = dataset.local_path

    @contextmanager
    def open(self):
        if self._descriptor['path']:
            if Path(self._descriptor['path']).is_absolute():
                filename = self._descriptor['path']
            else:
                filename = str(self.local_path.parent.joinpath(self._descriptor['path']))
        else:
            filename = str(self.local_path)

        for nasty_format in ('netcdf', 'hdf'):
            if nasty_format in self.format.lower():
                filename = 'file://%s:%s:%s' % (self.format, filename, self._descriptor['layer'])
                bandnumber = None
                break
        else:
            bandnumber = self._descriptor.get('layer', 1)

        try:
            _LOG.debug("openening %s, band %s", filename, bandnumber)
            with rasterio.open(filename) as src:

                if bandnumber is None:
                    if 'netcdf' in self.format.lower():
                        bandnumber = self.wheres_my_band(src, self.time)
                    else:
                        bandnumber = 1

                self.transform = src.affine
                self.crs = CRS(str(src.crs_wkt))
                self.nodata = src.nodatavals[0] or self._bandinfo.get('nodata')
                yield rasterio.band(src, bandnumber)
        except Exception as e:
            _LOG.error("Error opening source dataset: %s", filename)
            raise e

    def wheres_my_band(self, src, time):
        sec_since_1970 = datetime_to_seconds_since_1970(time)

        idx = 0
        dist = float('+inf')
        for i in range(1, src.count+1):
            v = float(src.tags(i)['NETCDF_DIM_time'])
            if abs(sec_since_1970 - v) < dist:
                idx = i
        return idx


def write_dataset_to_netcdf(access_unit, global_attributes, variable_params, filename):
    if filename.exists():
        raise RuntimeError('Storage Unit already exists: %s' % filename)

    try:
        filename.parent.mkdir(parents=True)
    except OSError:
        pass

#    _LOG.info("Writing storage unit: %s", filename)
    nco = netcdf_writer.create_netcdf(str(filename))

    for name, coord in access_unit.coords.items():
        netcdf_writer.create_coordinate(nco, name, coord.values, coord.units)

    netcdf_writer.create_grid_mapping_variable(nco, access_unit.crs)

    for name, variable in access_unit.data_vars.items():
        # Create variable
        var_params = variable_params.get(name, {})
        data_var = netcdf_writer.create_variable(nco, name,
                                                 Variable(variable.dtype,
                                                          getattr(variable, 'nodata', None),
                                                          variable.dims,
                                                          getattr(variable, 'units', '1')),
                                                 **var_params)

        # Write data
        data_var[:] = netcdf_writer.netcdfy_data(variable.values)

        # TODO: 'flags_definition', 'spectral_definition'?

        for key, value in variable_params.get(name, {}).get('attrs', {}).items():
            setattr(data_var, key, value)

    # write global atrributes
    for key, value in global_attributes.items():
        setattr(nco, key, value)

    nco.close()
