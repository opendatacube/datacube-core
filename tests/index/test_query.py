# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import pytest

from datacube.index.fields import _build_expressions
from datacube.index.postgres._fields import SimpleDocField, RangeDocField, RangeBetweenExpression, EqualsExpression
from datacube.model import Range
from datacube.scripts.search_tool import parse_expressions, UnknownFieldException

_sat_field = SimpleDocField('satellite', {}, None)
_sens_field = SimpleDocField('sensor', {}, None)
_lat_field = RangeDocField('lat', {}, None)
_fields = {
    'satellite': _sat_field,
    'sensor': _sens_field,
    'lat': _lat_field
}


def test_parse_expression():
    assert [EqualsExpression(
        _sat_field,
        4
    )] == parse_expressions(_fields.get, 'satellite = 4')

    assert [EqualsExpression(
        _sat_field,
        'LANDSAT_8'
    )] == parse_expressions(_fields.get, 'satellite = "LANDSAT_8"')

    between_exp = [RangeBetweenExpression(_lat_field, 4, 6)]
    assert between_exp == parse_expressions(_fields.get, '4<lat<6')
    assert between_exp == parse_expressions(_fields.get, '6 > lat > 4')


def test_parse_multiple_expressions():
    # Multiple expressions in one command-line statement.
    # Mixed whitespace:
    between_exp = parse_expressions(_fields.get, 'satellite=LS8 4<lat<23 sensor="OTHER"')
    assert between_exp == [
        EqualsExpression(
            _sat_field,
            'LS8'
        ),
        RangeBetweenExpression(_lat_field, 4, 23.0),
        EqualsExpression(
            _sens_field,
            'OTHER'
        )
    ]


def test_build_query_expressions():
    assert [EqualsExpression(_sat_field, "LANDSAT_8")] == _build_expressions(_fields.get, satellite="LANDSAT_8")
    assert [RangeBetweenExpression(_lat_field, 4, 23.0)] == _build_expressions(_fields.get, lat=Range(4, 23))


def test_unknown_field():
    with pytest.raises(UnknownFieldException) as e:
        parse_expressions(_fields.get, 'unknown=3')

    with pytest.raises(UnknownFieldException) as e:
        parse_expressions(_fields.get, '2<unknown<5')
