# coding=utf-8
"""
Build and index fields within documents.
"""
from __future__ import absolute_import

from string import lower

from sqlalchemy import cast, Index, TIMESTAMP
from sqlalchemy import func
from sqlalchemy.dialects import postgresql as postgres

from datacube.index.tables import DATASET


class Field(object):
    def __init__(self, name, descriptor):
        self.name = name
        self.descriptor = descriptor

    @property
    def alchemy_expression(self):
        raise NotImplementedError('alchemy expression')

    @property
    def alchemy_jsonb_field(self):
        return DATASET.c.metadata

    @property
    def postgres_index_type(self):
        return 'btree'

    @property
    def alchemy_index(self):
        return Index(
            'ix_dataset_md_' + lower(self.name),
            self.alchemy_expression,
            postgresql_using=self.postgres_index_type
        )


class ScalarField(Field):
    @property
    def alchemy_casted_type(self):
        # Default no cast: string
        return None

    @property
    def offset(self):
        return self.descriptor['offset']

    @property
    def alchemy_expression(self):
        _field = self.alchemy_jsonb_field[self.offset].astext
        return cast(_field, self.alchemy_casted_type) if self.alchemy_casted_type else _field


class RangeField(Field):
    @property
    def alchemy_range_type(self):
        raise NotImplementedError('range type')

    @property
    def min_offsets(self):
        return self.descriptor['min']

    @property
    def max_offsets(self):
        return self.descriptor['max']

    @property
    def alchemy_casted_type(self):
        # Default no cast: string
        return None

    @property
    def postgres_index_type(self):
        return 'gist'

    def _get_expr(self, doc_offsets, agg_function, casted_type):
        fields = [self.alchemy_jsonb_field[offset].astext for offset in doc_offsets]

        if casted_type:
            fields = [cast(field, casted_type) for field in fields]

        # If there's multiple fields, we aggregate them (eg. "min()"). Otherwise use the one.
        return func.least(*fields) if len(fields) > 1 else fields[0]

    @property
    def alchemy_expression(self):
        return self.alchemy_range_type(
            self._get_expr(self.min_offsets, func.least, self.alchemy_casted_type),
            self._get_expr(self.max_offsets, func.greatest, self.alchemy_casted_type),
            # Inclusive on both sides.
            '[]'
        )


class FloatRangeField(RangeField):
    @property
    def alchemy_casted_type(self):
        return postgres.NUMERIC

    @property
    def alchemy_range_type(self):
        return func.numrange


class DateRangeField(RangeField):
    @property
    def alchemy_casted_type(self):
        return TIMESTAMP(timezone=True)

    @property
    def alchemy_range_type(self):
        return func.tstzrange


def parse_doc(doc):
    """
    Parse a field spec document into objects.

    Example document:
    ::

        {
            # Field name:
            'lat': {
                # Field type & properties.
                'type': 'float-range',
                'min': [
                    # Offsets within a dataset document for this field.
                    ['extent', 'coord', 'ul', 'lat'],
                    ['extent', 'coord', 'll', 'lat']
                ],
                'max': [
                    ['extent', 'coord', 'ur', 'lat'],
                    ['extent', 'coord', 'lr', 'lat']
                ]
            }
        }

    :type doc: dict
    :rtype: dict[str, Field]
    """

    def _get_field(name, descriptor):
        type_map = {
            'float-range': FloatRangeField,
            'datetime-range': DateRangeField,
            'string': ScalarField
        }
        type_name = descriptor.get('type') or 'string'
        return type_map.get(type_name)(name, descriptor)

    return {name: _get_field(name, descriptor) for name, descriptor in doc.items()}
