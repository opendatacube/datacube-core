# coding=utf-8
"""
Module
"""

from psycopg2.extras import NumericRange

from datacube.drivers.postgres._fields import SimpleDocField, RangeBetweenExpression, EqualsExpression, \
    NumericRangeDocField
from datacube.index.fields import to_expressions
from datacube.model import Range


def test_build_query_expressions():
    _sat_field = SimpleDocField('platform', None, None, None)
    _sens_field = SimpleDocField('instrument', None, None, None)
    _lat_field = NumericRangeDocField('lat', None, None, None)
    _fields = {
        'platform': _sat_field,
        'instrument': _sens_field,
        'lat': _lat_field
    }

    assert [EqualsExpression(_sat_field, "LANDSAT_8")] == to_expressions(_fields.get, platform="LANDSAT_8")
    assert [RangeBetweenExpression(
        _lat_field, 4, 23.0, _range_class=NumericRange
    )] == to_expressions(_fields.get, lat=Range(4, 23))
