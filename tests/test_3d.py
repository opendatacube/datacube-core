# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from collections import OrderedDict
from pathlib import Path

import pytest
from datacube.model import DatasetType
from datacube.utils.documents import read_documents

PROJECT_ROOT = Path(__file__).parents[1]
GEDI_PRODUCT = (
    PROJECT_ROOT / "tests" / "data" / "lbg" / "gedi" / "GEDI02_B_3d_format.yaml"
)
# GEDI dataset type yaml path


@pytest.fixture
def cover_z_dataset_type():
    """The GEDI dataset type for cover_z."""
    for doc in read_documents(GEDI_PRODUCT):
        if doc[1]["name"] == "gedi_l2b_cover_z":
            yield doc[1]
            break
    # pytest will raise an error if nothing was yielded


def test_extra_dimensions(eo3_metadata, cover_z_dataset_type):
    """Check the ExtraDimensions class."""
    dt = DatasetType(eo3_metadata, cover_z_dataset_type)

    # Check dims
    dt.extra_dimensions.dims == OrderedDict(
        [("z", {"name": "z", "values": list(range(5, 151, 5)), "dtype": "float64"})]
    )

    # Check original slice
    assert dt.extra_dimensions.dim_slice == {"z": (0, 30)}

    # Check measurements values
    assert dt.extra_dimensions.measurements_values("z") == list(range(5, 151, 5))

    # User-selected slicing
    sliced = dt.extra_dimensions[{"z": (5, 12)}]
    assert sliced.dims == OrderedDict(
        [("z", {"name": "z", "values": [5, 10], "dtype": "float64"})]
    )

    # Check measurements_slice
    assert dt.extra_dimensions.measurements_slice("z") == slice(0, 30, None)

    # Check measurements index
    assert dt.extra_dimensions.measurements_index("z") == (0, 30)

    # Check index_of
    assert dt.extra_dimensions.index_of("z", 50) == 9

    # Check coord slice
    assert dt.extra_dimensions.coord_slice("z", 50) == (9, 10)
    assert dt.extra_dimensions.coord_slice("z", 48.3) == (9, 9)
    assert dt.extra_dimensions.coord_slice("z", (48.3, 62)) == (9, 12)
    assert dt.extra_dimensions.coord_slice("z", (148.3, 162)) == (29, 30)
    assert dt.extra_dimensions.coord_slice("z", 1000) == (30, 30)
    assert dt.extra_dimensions.coord_slice("z", (1000, 2000)) == (30, 30)

    # Check chunk size
    assert dt.extra_dimensions.chunk_size() == (("z",), (30,))

    # String representation
    readable = (
        "ExtraDimensions(extra_dim={'z': {'name': 'z', 'values': [5, 10, 15, "
        "20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, "
        "105, 110, 115, 120, 125, 130, 135, 140, 145, 150], 'dtype': "
        "'float64'}}, dim_slice={'z': (0, 30)} coords={'z': <xarray.DataArray "
        "'z' (z: 30)>\narray([  5.,  10.,  15.,  20.,  25.,  30.,  35.,  "
        "40.,  45.,  50.,  55.,\n        60.,  65.,  70.,  75.,  80.,  85.,  "
        "90.,  95., 100., 105., 110.,\n       115., 120., 125., 130., 135., "
        "140., 145., 150.])\nCoordinates:\n  * z        (z) int64 5 10 15 20 "
        "25 30 35 40 ... 120 125 130 135 140 145 150} )"
    )
    assert str(dt.extra_dimensions) == readable
    assert f"{dt.extra_dimensions!r}" == readable


def test_extra_dimensions_exceptions(eo3_metadata, cover_z_dataset_type):
    """Test exceptions on invalid input."""
    dt = DatasetType(eo3_metadata, cover_z_dataset_type)

    # Unknown keys
    with pytest.raises(KeyError) as exc_info:
        dt.extra_dimensions[{"x": (5, 12)}]
    # For some reason the exception message has double quotes around it
    assert str(exc_info.value).strip('"') == "Found unknown keys {'x'} in dim_slices"

    # Bogus measurements_value
    with pytest.raises(ValueError) as exc_info:
        dt.extra_dimensions.measurements_values("x")
    assert str(exc_info.value) == "Dimension x not found."

    # Bogus measurements_slice
    with pytest.raises(ValueError) as exc_info:
        dt.extra_dimensions.measurements_slice("x")
    assert str(exc_info.value) == "Dimension x not found."

    # Bogus measurements_index
    with pytest.raises(ValueError) as exc_info:
        dt.extra_dimensions.measurements_index("x")
    assert str(exc_info.value) == "Dimension x not found."

    # Bogus measurements_index
    with pytest.raises(ValueError) as exc_info:
        dt.extra_dimensions.index_of("x", 50)
    assert str(exc_info.value) == "Dimension x not found."
