# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Module
"""

from sqlalchemy.dialects.postgresql import Range as PgRange

from datacube.drivers.postgres._fields import (
    EqualsExpression,
    NumericRangeDocField,
    RangeBetweenExpression,
    SimpleDocField,
)
from datacube.index.fields import to_expressions
from datacube.model import Range


def test_build_query_expressions() -> None:
    _sat_field = SimpleDocField("platform", None, None, None)
    _sens_field = SimpleDocField("instrument", None, None, None)
    _lat_field = NumericRangeDocField("lat", None, None, None)
    _fields = {"platform": _sat_field, "instrument": _sens_field, "lat": _lat_field}

    assert [EqualsExpression(_sat_field, "LANDSAT_8")] == to_expressions(
        _fields.get, platform="LANDSAT_8"
    )
    assert [
        RangeBetweenExpression(_lat_field, 4, 23.0, _range_class=PgRange)
    ] == to_expressions(_fields.get, lat=Range(4, 23))
