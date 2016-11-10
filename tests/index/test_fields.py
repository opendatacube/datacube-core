# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from datacube.index.postgres._fields import SimpleDocField, NumericRangeDocField, parse_fields
from datacube.index.postgres.tables import DATASET


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

    _assert_same(
        fields['platform'],
        SimpleDocField(
            'platform', 'Satellite',
            DATASET.c.metadata,
            True,
            offset=['platform', 'code']
        )
    )

    storage_fields = parse_fields({
        'lat': {
            'type': 'float-range',
            'max_offset': [['extents', 'geospatial_lat_max']],
            'min_offset': [['extents', 'geospatial_lat_min']],
        },
    }, DATASET.c.metadata)
    _assert_same(
        storage_fields['lat'],
        NumericRangeDocField(
            'lat', None,
            DATASET.c.metadata,
            True,
            max_offset=[['extents', 'geospatial_lat_max']],
            min_offset=[['extents', 'geospatial_lat_min']],
        )
    )
