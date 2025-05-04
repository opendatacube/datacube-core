# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from collections import OrderedDict
from pathlib import Path

import pytest

from datacube.model import Product
from datacube.utils.documents import read_documents

PROJECT_ROOT = Path(__file__).parents[1]
GEDI_PRODUCT = (
    PROJECT_ROOT / "tests" / "data" / "lbg" / "gedi" / "GEDI02_B_3d_format.yaml"
)
# GEDI product yaml path


@pytest.fixture
def cover_z_product():
    """The GEDI product for cover_z."""
    for doc in read_documents(GEDI_PRODUCT):
        if doc[1]["name"] == "gedi_l2b_cover_z":
            yield doc[1]
            break
    # pytest will raise an error if nothing was yielded


def test_extra_dimensions(eo3_metadata, cover_z_product) -> None:
    """Check the ExtraDimensions class."""
    product = Product(eo3_metadata, cover_z_product)

    # Check dims
    assert product.extra_dimensions.dims == OrderedDict(
        [("z", {"name": "z", "values": list(range(5, 151, 5)), "dtype": "float64"})]
    )

    # Check original slice
    assert product.extra_dimensions.dim_slice == {"z": (0, 30)}

    # Check measurements values
    assert product.extra_dimensions.measurements_values("z") == list(range(5, 151, 5))

    # User-selected slicing
    sliced = product.extra_dimensions[{"z": (5, 12)}]
    assert sliced.dims == OrderedDict(
        [("z", {"name": "z", "values": [5, 10], "dtype": "float64"})]
    )

    # Check measurements_slice
    assert product.extra_dimensions.measurements_slice("z") == slice(0, 30, None)

    # Check measurements index
    assert product.extra_dimensions.measurements_index("z") == (0, 30)

    # Check index_of
    assert product.extra_dimensions.index_of("z", 50) == 9

    # Check coord slice
    assert product.extra_dimensions.coord_slice("z", 50) == (9, 10)
    assert product.extra_dimensions.coord_slice("z", 48.3) == (9, 9)
    assert product.extra_dimensions.coord_slice("z", (48.3, 62)) == (9, 12)
    assert product.extra_dimensions.coord_slice("z", (148.3, 162)) == (29, 30)
    assert product.extra_dimensions.coord_slice("z", 1000) == (30, 30)
    assert product.extra_dimensions.coord_slice("z", (1000, 2000)) == (30, 30)

    # Check chunk size
    assert product.extra_dimensions.chunk_size() == (("z",), (30,))

    # String representation
    assert str(product.extra_dimensions) == f"{product.extra_dimensions!r}"


def test_extra_dimensions_exceptions(eo3_metadata, cover_z_product) -> None:
    """Test exceptions on invalid input."""
    product = Product(eo3_metadata, cover_z_product)

    # Unknown keys
    with pytest.raises(KeyError) as exc_info:
        product.extra_dimensions[{"x": (5, 12)}]
    # For some reason the exception message has double quotes around it
    assert str(exc_info.value).strip('"') == "Found unknown keys {'x'} in dim_slices"

    # Bogus measurements_value
    with pytest.raises(ValueError) as exc_info:
        product.extra_dimensions.measurements_values("x")
    assert str(exc_info.value) == "Dimension x not found."

    # Bogus measurements_slice
    with pytest.raises(ValueError) as exc_info:
        product.extra_dimensions.measurements_slice("x")
    assert str(exc_info.value) == "Dimension x not found."

    # Bogus measurements_index
    with pytest.raises(ValueError) as exc_info:
        product.extra_dimensions.measurements_index("x")
    assert str(exc_info.value) == "Dimension x not found."

    # Bogus measurements_index
    with pytest.raises(ValueError) as exc_info:
        product.extra_dimensions.index_of("x", 50)
    assert str(exc_info.value) == "Dimension x not found."
