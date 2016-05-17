# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from copy import deepcopy

import pytest

from datacube.index._datasets import _ensure_valid
from datacube.index.fields import InvalidDocException

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
    {'measurements': {'band_70': {'dtype': 'int16', 'nodata': -999, 'units': '1'}}}
])
def test_accepts_valid_docs(valid_dataset_type_update):
    doc = deepcopy(only_mandatory_fields)
    doc.update(valid_dataset_type_update)
    # Should have no errors.
    _ensure_valid(doc)


def test_incomplete_dataset_type_invalid():
    # Invalid: An empty doc.
    with pytest.raises(InvalidDocException) as e:
        _ensure_valid({})


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
        _ensure_valid(mapping)


@pytest.mark.parametrize("valid_dataset_type_measurement", [
    {
        'dtype': 'int16',
        'units': '1',
        'nodata': -999
    },
    # With the optional properties
    {
        'nodata': -999,
        'units': '1',
        'dtype': 'int16',
        # TODO: flags/spectral
    },
])
def test_accepts_valid_measurements(valid_dataset_type_measurement):
    mapping = deepcopy(only_mandatory_fields)
    mapping['measurements'] = {'10': valid_dataset_type_measurement}
    # Should have no errors.
    _ensure_valid(mapping)


# Changes to the above dict that should render it invalid.
@pytest.mark.parametrize("invalid_dataset_type_measurement", [
    # nodata must be numeric
    {'nodata': '-999'},
    # Limited dtype options
    {'dtype': 'asdf'},
    {'dtype': 'intt13'},
    {'dtype': 13},
    # Unknown property
    {'asdf': 'asdf'},
])
def test_rejects_invalid_measurements(invalid_dataset_type_measurement):
    mapping = deepcopy(only_mandatory_fields)
    mapping['measurements'] = {'10': invalid_dataset_type_measurement}
    with pytest.raises(InvalidDocException) as e:
        _ensure_valid(mapping)
