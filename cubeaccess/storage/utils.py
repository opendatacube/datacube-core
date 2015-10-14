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


import numpy


def coord2index(coord, slice_):
    len_ = len(coord)

    if not slice_:
        return slice(0, len_)

    if coord[0] > coord[1]:  # reversed
        return slice(len_-numpy.searchsorted(coord[::-1], slice_.stop, side='right') if slice_.stop else 0,
                     len_-numpy.searchsorted(coord[::-1], slice_.start) if slice_.start else len_)
    else:
        return slice(numpy.searchsorted(coord, slice_.start) if slice_.start else 0,
                     numpy.searchsorted(coord, slice_.stop, side='right') if slice_.stop else len_)
