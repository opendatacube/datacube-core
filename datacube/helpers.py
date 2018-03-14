"""
Useful functions for Datacube users

Not used internally, those should go in `utils.py`
"""
from __future__ import absolute_import
import rasterio
import numpy as np

DEFAULT_PROFILE = {
    'blockxsize': 256,
    'blockysize': 256,
    'compress': 'lzw',
    'driver': 'GTiff',
    'interleave': 'band',
    'nodata': 0.0,
    'photometric': 'RGBA',
    'tiled': True}


def write_geotiff(filename, dataset, time_index=None, profile_override=None):
    """
    Write an xarray dataset to a geotiff

    :param filename: Output filename
    :attr dataset: xarray dataset containing multiple bands to write to file
    :attr time_index: time index to write to file
    :attr profile_override: option dict, overrides rasterio file creation options.
    """
    profile_override = profile_override or {}

    try:
        dtypes = {val.dtype for val in dataset.data_vars.values()}
        assert len(dtypes) == 1  # Check for multiple dtypes
    except AttributeError:
        dtypes = [dataset.dtype]

    profile = DEFAULT_PROFILE.copy()
    profile.update({
        'width': dataset.dims[dataset.crs.dimensions[1]],
        'height': dataset.dims[dataset.crs.dimensions[0]],
        'affine': dataset.affine,
        'crs': dataset.crs.crs_str,
        'count': len(dataset.data_vars),
        'dtype': str(dtypes.pop())
    })
    profile.update(profile_override)

    with rasterio.open(str(filename), 'w', **profile) as dest:
        if hasattr(dataset, 'data_vars'):
            for bandnum, data in enumerate(dataset.data_vars.values(), start=1):
                dest.write(data.isel(time=time_index).data, bandnum)
        else:  # Assume that we have a DataArray
            dest.write(dataset.isel(time=time_index).data, 0)


def ga_pq_fuser(dest, src):
    """
    Fuse two Geoscience Australia Pixel Quality ndarrays

    To be used as a `fuse_func` when loaded `grouped` data, for example when grouping
    by solar day to avoid duplicate data from scene overlaps.
    """
    valid_bit = 8
    valid_val = (1 << valid_bit)

    no_data_dest_mask = ~(dest & valid_val).astype(bool)
    np.copyto(dest, src, where=no_data_dest_mask)

    both_data_mask = (valid_val & dest & src).astype(bool)
    np.copyto(dest, src & dest, where=both_data_mask)
