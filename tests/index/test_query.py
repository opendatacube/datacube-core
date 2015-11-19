# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import pytest

from datacube.index.db._fields import SimpleField, RangeField, RangeBetweenExpression, EqualsExpression
from datacube.scripts.search_tool import parse_expressions, UnknownFieldException

_sat_field = SimpleField('satellite', {})
_sens_field = SimpleField('sensor', {})
_lat_field = RangeField('lat', {})
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


def test_unknown_field():
    with pytest.raises(UnknownFieldException) as e:
        parse_expressions(_fields.get, 'unknown=3')

    with pytest.raises(UnknownFieldException) as e:
        parse_expressions(_fields.get, '2<unknown<5')
