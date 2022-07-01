# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Useful functions for Datacube users

Not used internally, those should go in `utils.py`
"""

import numpy as np

DEFAULT_PROFILE = {
    'blockxsize': 256,
    'blockysize': 256,
    'compress': 'lzw',
    'driver': 'GTiff',
    'interleave': 'band',
    'nodata': 0.0,
    'tiled': True}


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
