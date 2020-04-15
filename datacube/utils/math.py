from typing import Tuple, Optional, Any, cast
from math import ceil, fmod

import numpy
import xarray as xr
from affine import Affine


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
    ds = ds.apply(unsqueeze_data_array, dim=dim, pos=pos, keep_attrs=True, coord=coord)
    return ds


def spatial_dims(xx: xr.DataArray, relaxed: bool = False) -> Optional[Tuple[str, str]]:
    """ Find spatial dimensions of `xx`.

        Checks for presence of dimensions named:
          y, x | latitude, longitude | lat, lon

        Returns
        =======
        None -- if no dimensions with expected names are found
        ('y', 'x') | ('latitude', 'longitude') | ('lat', 'lon')

        If *relaxed* is True and none of the above dimension names are found,
        assume that last two dimensions are spatial dimensions.
    """
    guesses = [('y', 'x'),
               ('latitude', 'longitude'),
               ('lat', 'lon')]

    dims = set(xx.dims)
    for guess in guesses:
        if dims.issuperset(guess):
            return guess

    if relaxed and len(xx.dims) >= 2:
        return cast(Tuple[str, str], xx.dims[-2:])

    return None


def clamp(x, l, u):
    """
    clamp x to be l <= x <= u

    >>> clamp(5, 1, 10)
    5
    >>> clamp(-1, 1, 10)
    1
    >>> clamp(12, 1, 10)
    10
    """
    assert l <= u
    return l if x < l else u if x > u else x


def is_almost_int(x: float, tol: float):
    """
    Check if number is close enough to an integer
    """
    x = abs(fmod(x, 1))
    if x > 0.5:
        x = 1 - x
    return x < tol


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
        return numpy.full_like(xx, True, dtype=numpy.bool)
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
        return numpy.full_like(xx, False, dtype=numpy.bool)
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


def data_resolution_and_offset(data, fallback_resolution=None):
    """ Compute resolution and offset from x/y axis data.

        Only uses first two coordinate values, assumes that data is regularly
        sampled.

        Returns
        =======
        (resolution: float, offset: float)
    """
    if data.size < 2:
        if data.size < 1:
            raise ValueError("Can't calculate resolution for empty data")
        if fallback_resolution is None:
            raise ValueError("Can't calculate resolution with data size < 2")
        res = fallback_resolution
    else:
        res = (data[data.size - 1] - data[0]) / (data.size - 1.0)
        res = res.item()

    off = data[0] - 0.5 * res
    return res, off.item()


def affine_from_axis(xx, yy, fallback_resolution=None):
    """ Compute Affine transform from pixel to real space given X,Y coordinates.

        :param xx: X axis coordinates
        :param yy: Y axis coordinates
        :param fallback_resolution: None|float|(resx:float, resy:float) resolution to
                                    assume for single element axis.

        (0, 0) in pixel space is defined as top left corner of the top left pixel
            \
            `` 0   1
             +---+---+
           0 |   |   |
             +---+---+
           1 |   |   |
             +---+---+

        Only uses first two coordinate values, assumes that data is regularly
        sampled.

        raises ValueError when any axis is empty
        raises ValueError when any axis has single value and fallback resolution was not supplied.
    """
    if fallback_resolution is not None:
        if isinstance(fallback_resolution, (float, int)):
            frx, fry = fallback_resolution, fallback_resolution
        else:
            frx, fry = fallback_resolution
    else:
        frx, fry = None, None

    xres, xoff = data_resolution_and_offset(xx, frx)
    yres, yoff = data_resolution_and_offset(yy, fry)

    return Affine.translation(xoff, yoff) * Affine.scale(xres, yres)


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
