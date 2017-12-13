from __future__ import absolute_import, division, print_function

from datacube.api import API
from datacube.drivers.manager import DriverManager

from mock import MagicMock


class PickableMock(MagicMock):
    def __reduce__(self):
        return MagicMock, ()


def test_get_descriptor_no_data():
    mock_index = PickableMock()

    api = API(index=mock_index)

    descriptor = api.get_descriptor({})

    assert descriptor == {}


def test_get_descriptor_some_data():
    from mock import MagicMock, Mock

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
    mock_index = PickableMock()
    DriverManager(index=mock_index)

    # mock_index.datasets.get_fields.return_value = dict(product=None)
    mock_index.storage.search.return_value = [su]

    api = API(index=mock_index)

    descriptor = api.get_descriptor({})

    assert descriptor == {}
