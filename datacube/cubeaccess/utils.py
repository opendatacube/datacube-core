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


def merge_unique(ars, kind='mergesort', reverse=False):
    c = numpy.concatenate(ars)
    c[::-1 if reverse else 1].sort(kind=kind)
    flag = numpy.ones(len(c), dtype=bool)
    numpy.not_equal(c[1:], c[:-1], out=flag[1:])
    return c[flag]
