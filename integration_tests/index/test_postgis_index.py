# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest

from datacube.index import Index
from datacube.model import Dataset
from datacube.model import Product
from datacube.utils.geometry import CRS

@pytest.mark.parametrize('datacube_env_name', ('experimental', ))
def test_spatial_index(index: Index):
    assert list(index.spatial_indexes()) == []
    # WKT CRS which cannot be mapped to an EPSG number.
    assert not index.create_spatial_index(CRS('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Weird",22.3],UNIT["Degree",0.017453292519943295]]'))
    assert index.create_spatial_index(CRS("EPSG:4326"))
    assert list(index.spatial_indexes()) == [CRS("EPSG:4326")]
    assert index.create_spatial_index(CRS("EPSG:3577"))
    assert index.create_spatial_index(CRS("WGS-84"))
    assert set(index.spatial_indexes(refresh=True)) == {CRS("EPSG:3577"), CRS("EPSG:4326")}

