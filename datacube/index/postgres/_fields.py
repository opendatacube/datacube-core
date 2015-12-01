# coding=utf-8
# pylint: disable=abstract-method
"""
Build and index fields within documents.
"""
from __future__ import absolute_import

import functools

from psycopg2.extras import NumericRange
from sqlalchemy import cast, TIMESTAMP
from sqlalchemy import func
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import NUMRANGE, TSTZRANGE

from datacube.index.fields import Expression, Field


class PgField(Field):
    """
    Postgres implementation of a searchable field. May be a value inside
    a JSONB column.
    """

    def __init__(self, name, description, collection_id, alchemy_column):
        super(PgField, self).__init__(name, description)
        self.collection_id = collection_id

        # The underlying SQLAlchemy column. (eg. DATASET.c.metadata)
        self.alchemy_column = alchemy_column

    @property
    def required_alchemy_table(self):
        return self.alchemy_column.table

    @property
    def alchemy_expression(self):
        """
        Get an SQLAlchemy expression for accessing this field.
        :return:
        """
        raise NotImplementedError('alchemy expression')

    @property
    def postgres_index_type(self):
        return 'btree'

    def __eq__(self, value):
        """
        :rtype: Expression
        """
        return EqualsExpression(self, value)

    def between(self, low, high):
        """
        :rtype: Expression
        """
        raise NotImplementedError('between expression')


class NativeField(PgField):
    """
    Fields hard-coded into the schema. (not user configurable)
    """

    def __init__(self, name, description, collection_id, alchemy_column, alchemy_expression=None):
        super(NativeField, self).__init__(name, description, collection_id, alchemy_column)
        self._expression = alchemy_expression

    @property
    def alchemy_expression(self):
        return self._expression or self.alchemy_column

    @property
    def postgres_index_type(self):
        # Don't add extra indexes for native fields.
        return None


class SimpleDocField(PgField):
    """
    A field with a single value (eg. String, int)
    """

    def __init__(self, name, description, collection_id, alchemy_column, offset=None):
        super(SimpleDocField, self).__init__(name, description, collection_id, alchemy_column)
        self.offset = offset

    @property
    def alchemy_casted_type(self):
        # Default no cast: string
        return None

    @property
    def alchemy_expression(self):
        _field = self.alchemy_column[self.offset].astext
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


class RangeDocField(PgField):
    """
    A range of values. Has min and max values, which may be calculated from multiple
    values in the document.
    """

    def __init__(self, name, description, collection_id, alchemy_column, min_offset=None, max_offset=None):
        super(RangeDocField, self).__init__(name, description, collection_id, alchemy_column)
        self.min_offset = min_offset
        self.max_offset = max_offset

    @property
    def alchemy_create_range(self):
        raise NotImplementedError('range type')

    @property
    def alchemy_casted_type(self):
        # Default no cast: string
        return None

    @property
    def postgres_index_type(self):
        return 'gist'

    def _get_expr(self, doc_offsets, agg_function, casted_type):
        fields = [self.alchemy_column[offset].astext for offset in doc_offsets]

        if casted_type:
            fields = [cast(field, casted_type) for field in fields]

        # If there's multiple fields, we aggregate them (eg. "min()"). Otherwise use the one.
        return agg_function(*fields) if len(fields) > 1 else fields[0]

    @property
    def alchemy_expression(self):
        return self.alchemy_create_range(
            self._get_expr(self.min_offset, func.least, self.alchemy_casted_type),
            self._get_expr(self.max_offset, func.greatest, self.alchemy_casted_type),
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


class FloatRangeDocField(RangeDocField):
    @property
    def alchemy_casted_type(self):
        return postgres.NUMERIC

    @property
    def alchemy_create_range(self):
        # Call the postgres 'numrange()' function, hinting to SQLAlchemy that it returns a NUMRANGE.
        return functools.partial(func.numrange, type_=NUMRANGE)


class DateRangeDocField(RangeDocField):
    @property
    def alchemy_casted_type(self):
        return TIMESTAMP(timezone=True)

    @property
    def alchemy_create_range(self):
        # Call the postgres 'tstzrange()' function, hinting to SQLAlchemy that it returns a TSTZRANGE.
        return functools.partial(func.tstzrange, type_=TSTZRANGE)


class PgExpression(Expression):
    def __init__(self, field):
        super(PgExpression, self).__init__()
        #: :type: PgField
        self.field = field

    @property
    def alchemy_expression(self):
        """
        Get an SQLAlchemy expression for accessing this field.
        :return:
        """
        raise NotImplementedError('alchemy expression')


class RangeBetweenExpression(PgExpression):
    def __init__(self, field, low_value, high_value):
        super(RangeBetweenExpression, self).__init__(field)
        self.low_value = low_value
        self.high_value = high_value

    @property
    def alchemy_expression(self):
        return self.field.alchemy_expression.overlaps(
            NumericRange(self.low_value, self.high_value)
        )


class EqualsExpression(PgExpression):
    def __init__(self, field, value):
        super(EqualsExpression, self).__init__(field)
        self.value = value

    @property
    def alchemy_expression(self):
        return self.field.alchemy_expression == self.value


def parse_fields(doc, collection_id, table_column):
    """
    Parse a field spec document into objects.

    Example document:
    :param collection_id:
    ::

        {
            # Field name:
            'lat': {
                # Field type & properties.
                'type': 'float-range',
                'min_offset': [
                    # Offsets within a dataset document for this field.
                    ['extent', 'coord', 'ul', 'lat'],
                    ['extent', 'coord', 'll', 'lat']
                ],
                'max_offset': [
                    ['extent', 'coord', 'ur', 'lat'],
                    ['extent', 'coord', 'lr', 'lat']
                ]
            }
        }

    :param table_column: SQLAlchemy jsonb column for the document we're reading fields from.
    :type doc: dict
    :rtype: dict[str, PgField]
    """

    def _get_field(name, collection_id, descriptor, column):
        """

        :type name: str
        :type descriptor: dict
        :param column: SQLAlchemy table column
        :rtype: PgField
        """
        type_map = {
            'float-range': FloatRangeDocField,
            'datetime-range': DateRangeDocField,
            'string': SimpleDocField
        }
        type_name = descriptor.pop('type', 'string')
        description = descriptor.pop('description', None)

        field_class = type_map.get(type_name)
        try:
            return field_class(name, description, collection_id, column, **descriptor)
        except TypeError as e:
            raise RuntimeError(
                'Field {name} has unexpected argument for a {type}'.format(
                    name=name, type=type_name
                ), e
            )

    return {name: _get_field(name, collection_id, descriptor, table_column) for name, descriptor in doc.items()}
