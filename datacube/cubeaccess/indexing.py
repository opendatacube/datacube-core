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


from __future__ import absolute_import, division, print_function

import numpy
from collections import namedtuple


Range = namedtuple('Range', ('begin', 'end'))


def _range_to_index(coord, range_):
    size = coord.size

    if size > 1 and coord[0] > coord[1]:  # reversed
        return slice(size-numpy.searchsorted(coord[::-1], range_.end, side='right') if range_.end else 0,
                     size-numpy.searchsorted(coord[::-1], range_.begin) if range_.begin else size, 1)
    else:
        return slice(numpy.searchsorted(coord, range_.begin) if range_.begin else 0,
                     numpy.searchsorted(coord, range_.end, side='right') if range_.end else size, 1)


def _expand_index(coord, slice_):
    return slice(slice_.start or 0, slice_.stop or coord.size, slice_.step or 1)


def make_index(data, idx):
    if idx is None:
        return slice(0, data.size, 1)
    if isinstance(idx, slice):
        return _expand_index(data, idx)
    if isinstance(idx, Range):
        return _range_to_index(data, idx)
    raise TypeError("this type is not supported")


def index_shape(idx):
    # TODO: single index
    # TODO: step
    assert all(i.step == 1 for i in idx)
    return tuple(i.stop-i.start for i in idx)
