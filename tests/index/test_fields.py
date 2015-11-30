# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from datacube.index.postgres._fields import SimpleDocField, FloatRangeDocField, NativeField, parse_fields
from datacube.index.postgres.tables import DATASET
from datacube.index.postgres.tables._storage import STORAGE_UNIT


def _assert_same(obj1, obj2):
    assert obj1.__class__ == obj2.__class__
    assert obj1.__dict__ == obj2.__dict__


def test_get_field():
    fields = parse_fields({
        'satellite': {
            'offset': ['platform', 'code']
        },
        'sensor': {
            'offset': ['instrument', 'name']
        }
    },
        DATASET.c.metadata
    )

    _assert_same(
        fields['satellite'],
        SimpleDocField(
            'satellite',
            DATASET.c.metadata,
            offset=['platform', 'code']
        )
    )

    storage_fields = parse_fields({
        'lat': {
            'type': 'float-range',
            'max_offset': [['extents', 'geospatial_lat_max']],
            'min_offset': [['extents', 'geospatial_lat_min']],
        },
    },
        STORAGE_UNIT.c.descriptor
    )
    _assert_same(
        storage_fields['lat'],
        FloatRangeDocField(
            'lat',
            STORAGE_UNIT.c.descriptor,
            max_offset=[['extents', 'geospatial_lat_max']],
            min_offset=[['extents', 'geospatial_lat_min']],
        )
    )

