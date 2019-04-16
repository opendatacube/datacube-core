import itertools
from math import ceil, fmod

import numpy
import xarray


def unsqueeze_data_array(da, dim, pos, coord=0, attrs=None):
    """
    Add a 1-length dimension to a data array.

    :param xarray.DataArray da: array to add a 1-length dimension
    :param str dim: name of new dimension
    :param int pos: position of dim
    :param coord: label of the coordinate on the unsqueezed dimension
    :param attrs: attributes for the coordinate dimension
    :return: A new xarray with a dimension added
    :rtype: xarray.DataArray
    """
    new_dims = list(da.dims)
    new_dims.insert(pos, dim)
    new_shape = da.data.shape[:pos] + (1,) + da.data.shape[pos:]
    new_data = da.data.reshape(new_shape)
    new_coords = {k: v for k, v in da.coords.items()}
    new_coords[dim] = xarray.DataArray([coord], dims=[dim], attrs=attrs)
    return xarray.DataArray(new_data, dims=new_dims, coords=new_coords, attrs=da.attrs)


def unsqueeze_dataset(ds, dim, coord=0, pos=0):
    ds = ds.apply(unsqueeze_data_array, dim=dim, pos=pos, keep_attrs=True, coord=coord)
    return ds


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


def valid_mask(xx, nodata):
    """
    Compute mask such that xx[mask] contains valid pixels.
    """
    if nodata is None:
        return numpy.ones(xx.shape, dtype='bool')
    if numpy.isnan(nodata):
        return ~numpy.isnan(xx)
    return xx != nodata


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


def data_resolution_and_offset(data):
    """
    >>> data_resolution_and_offset(numpy.array([1.5, 2.5, 3.5]))
    (1.0, 1.0)
    >>> data_resolution_and_offset(numpy.array([5, 3, 1]))
    (-2.0, 6.0)
    """
    res = (data[data.size - 1] - data[0]) / (data.size - 1.0)
    off = data[0] - 0.5 * res
    return res.item(), off.item()


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


def _tuplify(keys, values, defaults):
    assert not set(values.keys()) - set(keys), 'bad keys'
    return tuple(values.get(key, default) for key, default in zip(keys, defaults))


def _slicify(step, size):
    return (slice(i, min(i + step, size)) for i in range(0, size, step))


def _block_iter(steps, shape):
    return itertools.product(*(_slicify(step, size) for step, size in zip(steps, shape)))


def tile_iter(tile, chunk_size):
    """
    Return the sequence of chunks to split a tile into computable regions.

    :param tile: a tile of `.shape` size containing `.dim` dimensions
    :param chunk_size: dict of dimension sizes
    :return: Sequence of chunks to iterate across the entire tile
    """
    steps = _tuplify(tile.dims, chunk_size, tile.shape)
    return _block_iter(steps, tile.shape)
