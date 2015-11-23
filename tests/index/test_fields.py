# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from datacube.index.postgres._fields import FieldCollection, SimpleDocField, FloatRangeDocField, NativeField
from datacube.index.postgres.tables import DATASET
from datacube.index.postgres.tables._storage import STORAGE_UNIT


def _assert_same(obj1, obj2):
    assert obj1.__class__ == obj2.__class__
    assert obj1.__dict__ == obj2.__dict__


def test_get_field():
    collection = FieldCollection()

    assert collection.get('eo', 'dataset', 'wrong-field') is None
    assert collection.get('eo', 'dataset', 'satellite') is None

    collection.load_from_doc(
        {
            'eo': {
                'dataset': {
                    'satellite': {
                        'offset': ['platform', 'code']
                    },
                    'sensor': {
                        'offset': ['instrument', 'name']
                    }
                },
                'storage_unit': {
                    'lat': {
                        'type': 'float-range',
                        'max': [['extents', 'geospatial_lat_max']],
                        'min': [['extents', 'geospatial_lat_min']],
                    },
                }
            }
        }
    )

    _assert_same(
        collection.get('eo', 'dataset', 'satellite'),
        SimpleDocField('satellite', {'offset': ['platform', 'code']}, DATASET.c.metadata)
    )

    assert collection.get('eo', 'dataset', 'wrong-field') is None

    _assert_same(
        collection.get('eo', 'storage_unit', 'lat'),
        FloatRangeDocField(
            'lat',
            {
                'type': 'float-range',
                'max': [['extents', 'geospatial_lat_max']],
                'min': [['extents', 'geospatial_lat_min']],
            },
            STORAGE_UNIT.c.descriptor
        )
    )


def test_has_defaults():
    collection = FieldCollection()

    _assert_same(
        collection.get('eo', 'dataset', 'id'),
        NativeField('id', {}, DATASET.c.id)
    )
    _assert_same(
        collection.get('eo', 'storage_unit', 'path'),
        NativeField('path', {}, STORAGE_UNIT.c.path)
    )
