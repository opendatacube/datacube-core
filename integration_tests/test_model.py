# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest

from datacube.model import Dataset, DatasetType
from typing import List


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_crs_parse(indexed_ls5_scene_products: List[DatasetType]) -> None:
    product = indexed_ls5_scene_products[2]

    # Explicit CRS, should load fine.
    # Taken from LS8_OLI_NBAR_3577_-14_-11_20140601021126000000.nc
    d = Dataset(product, {
        "grid_spatial": {
            "projection": {
                "valid_data": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-1396453.986271351, -1100000.0], [-1400000.0, -1100000.0],
                         [-1400000.0, -1053643.4714392645], [-1392296.4215373022, -1054399.795365491],
                         [-1390986.9858215596, -1054531.808155645],
                         [-1390806.366757733, -1054585.3982497198],
                         [-1396453.986271351, -1100000.0]]
                    ]
                },
                "geo_ref_points": {
                    "ll": {"x": -1400000.0, "y": -1100000.0},
                    "lr": {"x": -1300000.0, "y": -1100000.0},
                    "ul": {"x": -1400000.0, "y": -1000000.0},
                    "ur": {"x": -1300000.0, "y": -1000000.0}},
                "spatial_reference": "EPSG:3577"
            }
        }

    })
    assert str(d.crs) == 'EPSG:3577'
    assert d.extent is not None

    # No projection specified in the dataset
    ds = Dataset(product, {})
    assert ds.crs is None
    assert ds.extent is None
