# coding=utf-8
"""
Module
"""

from copy import deepcopy

import pytest

from datacube.model import DatasetType
from datacube.utils import InvalidDocException

only_mandatory_fields = {
    'name': 'ls7_nbar',
    'description': 'description',
    'metadata_type': 'eo',
    'metadata': {'product_type': 'test'}
}


@pytest.mark.parametrize("valid_dataset_type_update", [
    {},
    {'storage': {'crs': 'EPSG:3577'}},
    # With the optional properties
    {'measurements': [{'name': 'band_70', 'dtype': 'int16', 'nodata': -999, 'units': '1'}]}
])
def test_accepts_valid_docs(valid_dataset_type_update):
    doc = deepcopy(only_mandatory_fields)
    doc.update(valid_dataset_type_update)
    # Should have no errors.
    DatasetType.validate(doc)


def test_incomplete_dataset_type_invalid():
    # Invalid: An empty doc.
    with pytest.raises(InvalidDocException) as e:
        DatasetType.validate({})


# Changes to the above dict that should render it invalid.
@pytest.mark.parametrize("invalid_dataset_type_update", [
    # Mandatory
    {'name': None},
    # Should be an object
    {'storage': 's'},
    # Should be a string
    {'description': 123},
    # Unknown property
    {'asdf': 'asdf'},
    # Name must have alphanumeric & underscores only.
    {'name': ' whitespace '},
    {'name': 'with-dashes'},
    # Mappings
    {'mappings': {}},
    {'mappings': ''}
])
def test_rejects_invalid_docs(invalid_dataset_type_update):
    mapping = deepcopy(only_mandatory_fields)
    mapping.update(invalid_dataset_type_update)
    with pytest.raises(InvalidDocException) as e:
        DatasetType.validate(mapping)


@pytest.mark.parametrize("valid_dataset_type_measurement", [
    {
        'name': '1',
        'dtype': 'int16',
        'units': '1',
        'nodata': -999
    },
    # With the optional properties
    {
        'name': 'red',
        'nodata': -999,
        'units': '1',
        'dtype': 'int16',
        # TODO: flags/spectral
    },
])
def test_accepts_valid_measurements(valid_dataset_type_measurement):
    mapping = deepcopy(only_mandatory_fields)
    mapping['measurements'] = [valid_dataset_type_measurement]
    # Should have no errors.
    DatasetType.validate(mapping)


# Changes to the above dict that should render it invalid.
@pytest.mark.parametrize("invalid_dataset_type_measurement", [
    # no name
    {'nodata': -999},
    # nodata must be numeric
    {'name': 'red', 'nodata': '-999'},
    # Limited dtype options
    {'name': 'red', 'dtype': 'asdf'},
    {'name': 'red', 'dtype': 'intt13'},
    {'name': 'red', 'dtype': 13},
    # Unknown property
    {'name': 'red', 'asdf': 'asdf'},
])
def test_rejects_invalid_measurements(invalid_dataset_type_measurement):
    mapping = deepcopy(only_mandatory_fields)
    mapping['measurements'] = {'10': invalid_dataset_type_measurement}
    with pytest.raises(InvalidDocException) as e:
        DatasetType.validate(mapping)
