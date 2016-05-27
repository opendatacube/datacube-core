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

from datacube.api import API


def test_get_descriptor_no_data():
    from mock import MagicMock

    mock_index = MagicMock()

    api = API(index=mock_index)

    descriptor = api.get_descriptor({})

    assert descriptor == {}


def test_get_descriptor_some_data():
    from mock import MagicMock

    band_10 = MagicMock(dtype='int16', )
    my_dict = {'band10': band_10}

    def getitem(name):
        return my_dict[name]

    def setitem(name, val):
        my_dict[name] = val

    mock_measurements = MagicMock()
    mock_measurements.__getitem__.side_effect = getitem
    mock_measurements.__setitem__.side_effect = setitem

    su = MagicMock()
    su.storage_type.dimensions.return_value = ['t', 'x', 'y']
    su.storage_type.measurements = mock_measurements
    su.coordinates.items
    su.storage_type.name
    su.variables.values.return_value = ['t', 'x', 'y']
    mock_index = MagicMock()

    # mock_index.datasets.get_fields.return_value = dict(product=None)
    mock_index.storage.search.return_value = [su]

    api = API(index=mock_index)

    descriptor = api.get_descriptor({})

    assert descriptor == {}
