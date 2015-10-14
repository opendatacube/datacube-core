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
from builtins import *

import numpy
from collections import OrderedDict


class DataArray(object):
    def __init__(self, data, coords=None, dims=None, attrs=None):
        self.values = data
        self.dims = dims or ["dim_%s" % i for i in xrange(data.ndim)]
        if coords:
            self.coords = OrderedDict(zip(self.dims, coords))
        else:
            self.coords = OrderedDict(zip(self.dims, (numpy.arange(0, l) for l in data.shape)))
        self.attrs = attrs or OrderedDict()

    def __repr__(self):
        summary = [
            'DataArray ' + ', '.join("%s: %s" % x for x in zip(self.dims, self.values.shape)),
            repr(self.values), 'Coordinates:'
        ]
        for name, data in self.coords.items():
            summary.append(" * "+name+": "+repr(data))
        return '\n'.join(summary)


class Index(object):
    def __init__(self):
        self.bounds = None
        self._data = []

    def insert(self, id_, bounds):
        if self.bounds:
            self.bounds = [min(self.bounds[i], bounds[i]) for i in xrange(len(self.bounds)//2)] \
                        + [max(self.bounds[len(self.bounds)//2+i], bounds[len(self.bounds)//2+i]) for i in xrange(len(self.bounds)//2)]
        else:
            self.bounds = bounds
        self._data.append((id_, bounds))

    def intersection(self, query):
        ndim = len(self.bounds)//2
        result = [id_ for id_, bounds in self._data if
                  all(bounds[i] <= query[ndim+i] and bounds[ndim+i] >= query[i] for i in xrange(ndim))]
        return result
