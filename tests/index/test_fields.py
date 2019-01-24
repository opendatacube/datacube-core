# coding=utf-8
"""
Module
"""

from datacube.drivers.postgres._fields import SimpleDocField, NumericRangeDocField, parse_fields, RangeDocField, \
    IntDocField
from datacube.drivers.postgres._schema import DATASET
from datacube.model import Range


def _assert_same(obj1, obj2):
    assert obj1.__class__ == obj2.__class__
    assert obj1.__dict__ == obj2.__dict__


def test_get_single_field():
    fields = parse_fields({
        'platform': {
            'description': 'Satellite',
            'offset': ['platform', 'code']
        },
        'instrument': {
            'offset': ['instrument', 'name']
        }
    }, DATASET.c.metadata)
    assert set(fields.keys()) == {'platform', 'instrument'}
    field = fields['platform']
    _assert_same(
        field,
        SimpleDocField(
            'platform', 'Satellite',
            DATASET.c.metadata,
            True,
            offset=['platform', 'code']
        )
    )
    assert isinstance(field, SimpleDocField)
    assert field.extract({'platform': {'code': 'turtle'}}) == 'turtle'
    assert field.extract({'platform': {'code': None}}) is None
    assert field.extract({}) is None


def test_get_multi_field():
    fields = parse_fields({
        'orbit': {
            'description': 'Orbit number',
            'type': 'integer',
            'offset': [
                ['acquisition', 'platform_orbit'],
                ['orbit']
            ]
        }
    }, DATASET.c.metadata)
    assert set(fields.keys()) == {'orbit'}

    field = fields['orbit']
    _assert_same(
        field,
        IntDocField(
            'orbit', 'Orbit number',
            DATASET.c.metadata,
            True,
            offset=[
                ['acquisition', 'platform_orbit'],
                ['orbit']
            ]
        )
    )
    assert isinstance(field, SimpleDocField)
    assert field.extract({'platform': {'code': 'turtle'}}) is None
    assert field.extract({'acquisition': {'platform_orbit': 5}}) == 5
    assert field.extract({'orbit': 10}) == 10
    # It chooses the first listed field with a non-null value
    assert field.extract({'orbit': 10, 'acquisition': {'platform_orbit': 5}}) == 5


def test_get_range_field():
    storage_fields = parse_fields({
        'lat': {
            'type': 'float-range',
            'max_offset': [['extents', 'geospatial_lat_max']],
            'min_offset': [
                ['extents', 'geospatial_lat_other'],
                ['extents', 'geospatial_lat_min']
            ],
        },
    }, DATASET.c.metadata)
    field = storage_fields['lat']
    _assert_same(
        field,
        NumericRangeDocField(
            'lat', None,
            DATASET.c.metadata,
            True,
            max_offset=[
                ['extents', 'geospatial_lat_max']
            ],
            min_offset=[
                ['extents', 'geospatial_lat_other'],
                ['extents', 'geospatial_lat_min']
            ],
        )
    )
    assert isinstance(field, RangeDocField)
    extracted = field.extract({'extents': {'geospatial_lat_min': 2, 'geospatial_lat_max': 4}})
    assert extracted == Range(begin=2, end=4)
