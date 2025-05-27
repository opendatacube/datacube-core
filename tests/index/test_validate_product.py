# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Module
"""

from copy import deepcopy

import pytest

from datacube.model import Product
from datacube.utils import InvalidDocException

only_mandatory_fields = {
    "name": "ls7_nbar",
    "description": "description",
    "metadata_type": "eo",
    "metadata": {"product_type": "test"},
}


@pytest.mark.parametrize(
    "valid_product_update",
    [
        {},
        {"storage": {"crs": "EPSG:3577"}},
        # With the optional properties
        {
            "measurements": [
                {"name": "band_70", "dtype": "int16", "nodata": -999, "units": "1"}
            ]
        },
    ],
)
def test_accepts_valid_docs(valid_product_update) -> None:
    doc = deepcopy(only_mandatory_fields)
    doc.update(valid_product_update)
    # Should have no errors.
    Product.validate(doc)


def test_incomplete_product_is_invalid() -> None:
    # Invalid: An empty doc.
    with pytest.raises(InvalidDocException):
        Product.validate({})


# Changes to the above dict that should render it invalid.
@pytest.mark.parametrize(
    "invalid_product_update",
    [
        # Mandatory
        {"name": None},
        # Should be an object
        {"storage": "s"},
        # Should be a string
        {"description": 123},
        # Unknown property
        {"asdf": "asdf"},
        # Name must have alphanumeric & underscores only.
        {"name": " whitespace "},
        {"name": "with-dashes"},
        # Mappings
        {"mappings": {}},
        {"mappings": ""},
    ],
)
def test_rejects_invalid_docs(invalid_product_update) -> None:
    mapping = deepcopy(only_mandatory_fields)
    mapping.update(invalid_product_update)
    with pytest.raises(InvalidDocException):
        Product.validate(mapping)


@pytest.mark.parametrize(
    "valid_product_measurement",
    [
        {"name": "1", "dtype": "int16", "units": "1", "nodata": -999},
        # With the optional properties
        {
            "name": "red",
            "nodata": -999,
            "units": "1",
            "dtype": "int16",
            # TODO: flags/spectral
        },
    ],
)
def test_accepts_valid_measurements(valid_product_measurement) -> None:
    mapping = deepcopy(only_mandatory_fields)
    mapping["measurements"] = [valid_product_measurement]
    # Should have no errors.
    Product.validate(mapping)


# Changes to the above dict that should render it invalid.
@pytest.mark.parametrize(
    "invalid_product_measurement",
    [
        # no name
        {"nodata": -999},
        # nodata must be numeric
        {"name": "red", "nodata": "-999"},
        # Limited dtype options
        {"name": "red", "dtype": "asdf"},
        {"name": "red", "dtype": "intt13"},
        {"name": "red", "dtype": 13},
        # Unknown property
        {"name": "red", "asdf": "asdf"},
    ],
)
def test_rejects_invalid_measurements(invalid_product_measurement) -> None:
    mapping = deepcopy(only_mandatory_fields)
    mapping["measurements"] = {"10": invalid_product_measurement}
    with pytest.raises(InvalidDocException):
        Product.validate(mapping)
