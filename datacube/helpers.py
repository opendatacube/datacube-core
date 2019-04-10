"""
Useful functions for Datacube users

Not used internally, those should go in `utils.py`
"""

import numpy as np
import rasterio

DEFAULT_PROFILE = {
    'blockxsize': 256,
    'blockysize': 256,
    'compress': 'lzw',
    'driver': 'GTiff',
    'interleave': 'band',
    'nodata': 0.0,
    'tiled': True}


def write_geotiff(filename, dataset, profile_override=None, time_index=None):
    """
    Write an ODC style xarray.Dataset to a GeoTIFF file.

    :param filename: Output filename
    :param dataset: xarray dataset containing one or more bands to write to a file.
    :param profile_override: option dict, overrides rasterio file creation options.
    :param time_index: DEPRECATED
    """
    if time_index is not None:
        raise ValueError('''The write_geotiff function no longer supports passing in `time_index`.
        The same function can be achieved by calling `dataset.isel(time=<time_index>)` before passing
 in your dataset. It was removed because it made the function much less useful for more advanced cases.''')

    profile_override = profile_override or {}

    geobox = getattr(dataset, 'geobox', None)

    if geobox is None:
        raise ValueError('Can only write datasets with specified `crs` attribute')

    try:
        dtypes = {val.dtype for val in dataset.data_vars.values()}
        assert len(dtypes) == 1  # Check for multiple dtypes
    except AttributeError:
        dtypes = [dataset.dtype]

    profile = DEFAULT_PROFILE.copy()
    height, width = geobox.shape

    profile.update({
        'width': width,
        'height': height,
        'transform': geobox.affine,
        'crs': geobox.crs.crs_str,
        'count': len(dataset.data_vars),
        'dtype': str(dtypes.pop())
    })
    profile.update(profile_override)

    _calculate_blocksize(profile)

    with rasterio.open(str(filename), 'w', **profile) as dest:
        if hasattr(dataset, 'data_vars'):
            for bandnum, data in enumerate(dataset.data_vars.values(), start=1):
                dest.write(data.data, bandnum)


def _calculate_blocksize(profile):
    # Block size must be smaller than the image size, and for geotiffs must be divisible by 16
    # Fix for small images.
    if profile['blockxsize'] > profile['width']:
        if profile['width'] % 16 == 0 or profile['width'] < 16:
            profile['blockxsize'] = profile['width']
        else:
            profile['blockxsize'] = 16

    if profile['blockysize'] > profile['height']:
        if profile['height'] % 16 == 0 or profile['height'] < 16:
            profile['blockysize'] = profile['height']
        else:
            profile['blockysize'] = 16


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
