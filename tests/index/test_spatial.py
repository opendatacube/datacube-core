# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from odc.geo.crs import CRS
from odc.geo.geom import box


def test_extract_geom() -> None:
    p1 = box(-122.2, 44.7, -120.8, 45.0, crs="epsg:4326")
    p2 = box(-102.5, 44.1, -100.2, 46.2, crs="epsg:4326")
    p3 = box(-112.7, 42.4, -109.6, 45.1, crs="epsg:4326")

    from datacube.index._spatial import extract_geom_from_query

    geom = extract_geom_from_query(geopolygon=[p1, p2, p3])
    assert geom.contains(p1)
    assert geom.contains(p2)
    assert geom.contains(p3)
    geom = extract_geom_from_query(lon=(-122.3, -100.2))
    assert geom.crs == CRS("epsg:4326")
    assert geom.contains(p1)
    assert geom.contains(p2)
    assert geom.contains(p3)

    with pytest.raises(ValueError):
        geom = extract_geom_from_query(geopolygon=p3, crs="epsg:3577")

    with pytest.raises(ValueError):
        geom = extract_geom_from_query(
            latitude=(22, 23), y=(10011.1, 585585.5), crs="epsg:3577"
        )

    with pytest.raises(ValueError):
        geom = extract_geom_from_query(
            lon=(22, 23), x=(10011.1, 585585.5), crs="epsg:3577"
        )
