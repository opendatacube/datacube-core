# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from datacube.index.postgres._fields import SimpleDocField, NumericRangeDocField, parse_fields, RangeDocField
from datacube.index.postgres.tables import DATASET
from datacube.model import Range


def _assert_same(obj1, obj2):
    assert obj1.__class__ == obj2.__class__
    assert obj1.__dict__ == obj2.__dict__


def test_get_field():
    fields = parse_fields({
        'platform': {
            'description': 'Satellite',
            'offset': ['platform', 'code']
        },
        'instrument': {
            'offset': ['instrument', 'name']
        }
    }, DATASET.c.metadata)

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
