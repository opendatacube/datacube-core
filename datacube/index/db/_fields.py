# coding=utf-8
"""
Build and index fields within documents.
"""
from __future__ import absolute_import

from string import lower

import yaml
from pathlib import Path
from psycopg2._range import NumericRange
from sqlalchemy import cast, Index, TIMESTAMP
from sqlalchemy import func
from sqlalchemy.dialects import postgresql as postgres

from .tables import DATASET


class Field(object):
    """
    A field within a Postgres JSONB document.
    """

    def __init__(self, name, descriptor):
        self.name = name
        self.descriptor = descriptor

    @property
    def alchemy_expression(self):
        """
        Get an SQLAlchemy expression for accessing this field.
        :return:
        """
        raise NotImplementedError('alchemy expression')

    @property
    def alchemy_jsonb_column(self):
        """
        The underlying table column.
        :return:
        """
        return DATASET.c.metadata

    @property
    def postgres_index_type(self):
        return 'btree'

    @property
    def alchemy_index(self):
        """
        Build an SQLAlchemy index for this field.
        """
        return Index(
            'ix_dataset_md_' + lower(self.name),
            self.alchemy_expression,
            postgresql_using=self.postgres_index_type
        )

    def __eq__(self, value):
        """
        :rtype: Expression
        """
        raise NotImplementedError('equals expression')

    def between(self, low, high):
        """
        :rtype: Expression
        """
        raise NotImplementedError('between expression')


class SimpleField(Field):
    """
    A field with a single value (eg. String, int)
    """

    @property
    def alchemy_casted_type(self):
        # Default no cast: string
        return None

    @property
    def offset(self):
        return self.descriptor['offset']

    @property
    def alchemy_expression(self):
        _field = self.alchemy_jsonb_column[self.offset].astext
        return cast(_field, self.alchemy_casted_type) if self.alchemy_casted_type else _field

    def __eq__(self, value):
        """
        :rtype: Expression
        """
        return EqualsExpression(self, value)

    def between(self, low, high):
        """
        :rtype: Expression
        """
        raise NotImplementedError('Simple field between expression')

class RangeField(Field):
    """
    A range of values. Has min and max values, which may be calculated from multiple
    values in the document.
    """

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
        fields = [self.alchemy_jsonb_column[offset].astext for offset in doc_offsets]

        if casted_type:
            fields = [cast(field, casted_type) for field in fields]

        # If there's multiple fields, we aggregate them (eg. "min()"). Otherwise use the one.
        return agg_function(*fields) if len(fields) > 1 else fields[0]

    @property
    def alchemy_expression(self):
        return self.alchemy_range_type(
            self._get_expr(self.min_offsets, func.least, self.alchemy_casted_type),
            self._get_expr(self.max_offsets, func.greatest, self.alchemy_casted_type),
            # Inclusive on both sides.
            '[]'
        )

    def __eq__(self, value):
        """
        :rtype: Expression
        """
        raise NotImplementedError('range equals expression')

    def between(self, low, high):
        """
        :rtype: Expression
        """
        return RangeBetweenExpression(self, low, high)


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


class Expression(object):
    @property
    def alchemy_expression(self):
        """
        Get an SQLAlchemy expression for accessing this field.
        :return:
        """
        raise NotImplementedError('alchemy expression')


class RangeBetweenExpression(Expression):
    def __init__(self, field, low_value, high_value):
        self.low_value = low_value
        self.high_value = high_value
        self.field = field

    @property
    def alchemy_expression(self):
        return self.field.alchemy_expression.contained_by(
            NumericRange(self.low_value, self.high_value)
        )


class EqualsExpression(Expression):
    def __init__(self, field, value):
        self.field = field
        self.value = value

    @property
    def alchemy_expression(self):
        return self.field.alchemy_expression == self.value


def load_fields():
    # TODO: Store in DB? This doesn't change often, so is hardcoded for now.
    doc = yaml.load(Path(__file__).parent.joinpath('dataset-fields.yaml').open('r'))
    return _parse_doc(doc['eo'])


def _parse_doc(doc):
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
            'string': SimpleField
        }
        type_name = descriptor.get('type') or 'string'
        return type_map.get(type_name)(name, descriptor)

    return {name: _get_field(name, descriptor) for name, descriptor in doc.items()}
