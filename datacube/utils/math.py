# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from typing import Tuple, Union, Optional, Any
from math import ceil

import numpy
import xarray as xr
import odc.geo.math as geomath
from odc.geo.xr import spatial_dims as xr_spatial_dims

from datacube.migration import ODC2DeprecationWarning
from deprecat import deprecat


def unsqueeze_data_array(da: xr.DataArray,
                         dim: str,
                         pos: int,
                         coord: Any = 0,
                         attrs: Optional[dict] = None) -> xr.DataArray:
    """
    Add a 1-length dimension to a data array.

    :param da: array to add a 1-length dimension
    :param dim: name of new dimension
    :param pos: position of dim
    :param coord: label of the coordinate on the unsqueezed dimension
    :param attrs: attributes for the coordinate dimension
    :return: A new xarray with a dimension added
    """
    new_dims = list(da.dims)
    new_dims.insert(pos, dim)
    new_shape = da.data.shape[:pos] + (1,) + da.data.shape[pos:]
    new_data = da.data.reshape(new_shape)
    new_coords = {k: v for k, v in da.coords.items()}
    new_coords[dim] = xr.DataArray([coord], dims=[dim], attrs=attrs)
    return xr.DataArray(new_data, dims=new_dims, coords=new_coords, attrs=da.attrs)


def unsqueeze_dataset(ds: xr.Dataset, dim: str, coord: int = 0, pos: int = 0) -> xr.Dataset:
    ds = ds.map(unsqueeze_data_array, dim=dim, pos=pos, keep_attrs=True, coord=coord)
    return ds


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def spatial_dims(xx: Union[xr.DataArray, xr.Dataset],
                 relaxed: bool = False) -> Optional[Tuple[str, str]]:
    return xr_spatial_dims(xx, relaxed)


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def maybe_zero(x: float, tol: float) -> float:
    return geomath.maybe_zero(x, tol)


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def maybe_int(x: float, tol: float) -> Union[int, float]:
    return geomath.maybe_int(x, tol)


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def snap_scale(s, tol=1e-6):
    return geomath.snap_scale(s, tol)


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def clamp(x, lo, up):
    return geomath.clamp(x, lo, up)


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def is_almost_int(x: float, tol: float):
    return geomath.is_almost_int(x, tol)


def dtype_is_float(dtype) -> bool:
    """
    Check if `dtype` is floating-point.
    """
    return numpy.dtype(dtype).kind == 'f'


def valid_mask(xx, nodata):
    """
    Compute mask such that xx[mask] contains only valid pixels.
    """
    if dtype_is_float(xx.dtype):
        if nodata is None or numpy.isnan(nodata):
            return ~numpy.isnan(xx)
        return ~numpy.isnan(xx) & (xx != nodata)

    if nodata is None:
        return numpy.full_like(xx, True, dtype=bool)
    return xx != nodata


def invalid_mask(xx, nodata):
    """
    Compute mask such that xx[mask] contains only invalid pixels.
    """
    if dtype_is_float(xx.dtype):
        if nodata is None or numpy.isnan(nodata):
            return numpy.isnan(xx)
        return numpy.isnan(xx) | (xx == nodata)

    if nodata is None:
        return numpy.full_like(xx, False, dtype=bool)
    return xx == nodata


def num2numpy(x, dtype, ignore_range=None):
    """
    Cast python numeric value to numpy.

    :param x int|float: Numerical value to convert to numpy.type
    :param dtype str|numpy.dtype|numpy.type: Destination dtype
    :param ignore_range: If set to True skip range check and cast anyway (for example: -1 -> 255)

    :returns: None if x is None
    :returns: None if x is outside the valid range of dtype and ignore_range is not set
    :returns: dtype.type(x) if x is within range or ignore_range=True
    """
    if x is None:
        return None

    if isinstance(dtype, (str, type)):
        dtype = numpy.dtype(dtype)

    if ignore_range or dtype.kind == 'f':
        return dtype.type(x)

    info = numpy.iinfo(dtype)
    if info.min <= x <= info.max:
        return dtype.type(x)

    return None


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def data_resolution_and_offset(data, fallback_resolution=None):
    return geomath.data_resolution_and_offset(data, fallback_resolution)


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def affine_from_axis(xx, yy, fallback_resolution=None):
    return geomath.affine_from_axis(xx, yy, fallback_resolution)


def iter_slices(shape, chunk_size):
    """
    Generate slices for a given shape.

    E.g. ``shape=(4000, 4000), chunk_size=(500, 500)``
    Would yield 64 tuples of slices, each indexing 500x500.

    If the shape is not divisible by the chunk_size, the last chunk in each dimension will be smaller.

    :param tuple(int) shape: Shape of an array
    :param tuple(int) chunk_size: length of each slice for each dimension
    :return: Yields slices that can be used on an array of the given shape

    >>> list(iter_slices((5,), (2,)))
    [(slice(0, 2, None),), (slice(2, 4, None),), (slice(4, 5, None),)]
    """
    assert len(shape) == len(chunk_size)
    num_grid_chunks = [int(ceil(s / float(c))) for s, c in zip(shape, chunk_size)]
    for grid_index in numpy.ndindex(*num_grid_chunks):
        yield tuple(
            slice(min(d * c, stop), min((d + 1) * c, stop)) for d, c, stop in zip(grid_index, chunk_size, shape))
