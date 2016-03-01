#    Copyright 2015 Geoscience Australia
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


"""
Indexing utilities
"""


from __future__ import absolute_import, division, print_function

import numpy

from datacube.model import Range


def range_to_index(coord, range_):
    """
    Convert coordinate label range to slice object
    :param coord: array of coordinate labels
    :type coord: numpy.ndarray
    :type range_: Range
    :rtype slice

    >>> range_to_index(numpy.arange(10, 20), Range(12, 15))
    slice(2, 6, 1)
    >>> range_to_index(numpy.arange(20, 10, -1), Range(12, 15))
    slice(5, 9, 1)
    """
    size = coord.size

    if size > 1 and coord[0] > coord[1]:  # reversed
        return slice(size-numpy.searchsorted(coord[::-1], range_.end, side='right') if range_.end else 0,
                     size-numpy.searchsorted(coord[::-1], range_.begin) if range_.begin else size, 1)
    else:
        return slice(numpy.searchsorted(coord, range_.begin) if range_.begin else 0,
                     numpy.searchsorted(coord, range_.end, side='right') if range_.end else size, 1)


def _expand_index(coord, slice_):
    return slice(slice_.start or 0, slice_.stop or coord.size, slice_.step or 1)


def normalize_index(coord, index):
    """
    Fill in None fields of slice or Range object
    :param coord: datacube.access.core.Coordinate
    :type index: slice | Range
    :rtype slice | Range

    >>> from .core import Coordinate
    >>> normalize_index(Coordinate(numpy.int, 10, 20, 10, '1'), None)
    slice(0, 10, 1)
    >>> normalize_index(Coordinate(numpy.int, 10, 20, 10, '1'), slice(3))
    slice(0, 3, 1)
    >>> normalize_index(Coordinate(numpy.int, 10, 20, 10, '1'), Range(None, None))
    Range(begin=10, end=20)
    """
    if index is None:
        return slice(0, coord.length, 1)
    if isinstance(index, slice):
        return slice(index.start or 0, index.stop or coord.length, index.step or 1)
    if isinstance(index, Range):
        return Range(index.begin or coord.begin, index.end or coord.end)
    raise TypeError("this type is not supported")


def make_index(coord, index):
    """
    Convert slice or Range object to slice
    :param coord: array of coordinate labels
    :type coord: numpy.ndarray
    :type index: slice | Range
    :rtype slice
    """
    if index is None:
        return slice(0, coord.size, 1)
    if isinstance(index, slice):
        return _expand_index(coord, index)
    if isinstance(index, Range):
        return range_to_index(coord, index)
    raise TypeError("this type is not supported")


def index_shape(index):
    """
    Calculate the shape of the index
    :type index: List[slice]
    :rtype: tuple

    >>> index_shape((slice(0,3,1), slice(3,5,1)))
    (3, 2)
    """
    # TODO: single index
    # TODO: step
    assert all(i.step in [None, 1] for i in index)
    return tuple(i.stop-i.start for i in index)
