# coding=utf-8
# pylint: disable=abstract-method
"""
Build and index fields within documents.
"""
from __future__ import absolute_import

from datetime import datetime
from functools import partial
from typing import Any
from typing import Callable
from typing import Tuple

from dateutil import tz
from psycopg2.extras import NumericRange, DateTimeTZRange
from sqlalchemy import cast, func, and_
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.dialects.postgresql import NUMRANGE, TSTZRANGE
from sqlalchemy.dialects.postgresql.base import DOUBLE_PRECISION
from sqlalchemy.sql import ColumnElement

from datacube import compat
from datacube.index.fields import Expression, Field
from datacube.index.postgres.tables import FLOAT8RANGE
from datacube.model import Range
from datacube.utils import get_doc_offset


class PgField(Field):
    """
    Postgres implementation of a searchable field. May be a value inside
    a JSONB column.
    """

    def __init__(self, name, description, alchemy_column, indexed):
        super(PgField, self).__init__(name, description)

        # The underlying SQLAlchemy column. (eg. DATASET.c.metadata)
        self.alchemy_column = alchemy_column
        self.indexed = indexed

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
    def sql_expression(self):
        """
        Get the raw SQL expression for this field as a string.
        :rtype: str
        """
        return str(self.alchemy_expression.compile(
            dialect=postgres.dialect(),
            compile_kwargs={"literal_binds": True}
        ))

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

    def __init__(self, name, description, alchemy_column, alchemy_expression=None,
                 # Should this be selected by default when selecting all fields?
                 affects_row_selection=False):
        super(NativeField, self).__init__(name, description, alchemy_column, False)
        self._expression = alchemy_expression
        self.affects_row_selection = affects_row_selection

    @property
    def alchemy_expression(self):
        expression = self._expression if self._expression is not None else self.alchemy_column
        return expression.label(self.name)

    @property
    def postgres_index_type(self):
        # Don't add extra indexes for native fields.
        return None


class PgDocField(PgField):
    """
    A field extracted from inside a (jsonb) document.
    """

    def extract(self, document):
        """
        Extract a value from the given document in pure python (no postgres).
        """
        raise NotImplementedError("extract()")

    def value_to_alchemy(self, value):
        """
        Wrap the given value with any necessary type casts/conversions for this field.

        Overridden by other classes as needed.
        """
        # Default do nothing (eg. string datatypes)
        return value

    def _alchemy_offset_value(self, doc_offsets, agg_function):
        # type: (Tuple[Tuple[str]], Callable[[Any], ColumnElement]) -> ColumnElement
        """
        Get an sqlalchemy value for the given offsets of this field's sqlalchemy column.
        If there are multiple they will be combined using the given aggregate function.

        Offsets can either be single:
            ('platform', 'code')
        Or multiple:
            (('platform', 'code'), ('satellite', 'name'))

        In the latter case, the multiple values are combined using the given aggregate function
        (defaults to using coalesce: grab the first non-null value)
        """
        if not doc_offsets:
            raise ValueError("Value requires at least one offset")

        if isinstance(doc_offsets[0], compat.string_types):
            # It's a single offset.
            doc_offsets = [doc_offsets]

        alchemy_values = [self.value_to_alchemy(self.alchemy_column[offset].astext) for offset in doc_offsets]
        # If there's multiple fields, we aggregate them (eg. "min()"). Otherwise use the one.
        return agg_function(*alchemy_values) if len(alchemy_values) > 1 else alchemy_values[0]


class SimpleDocField(PgDocField):
    """
    A field with a single value (eg. String, int) calculated as an offset inside a (jsonb) document.
    """

    def __init__(self, name, description, alchemy_column, indexed, offset=None, agg_function=func.coalesce):
        super(SimpleDocField, self).__init__(name, description, alchemy_column, indexed)
        self.offset = offset
        self.agg_function = agg_function

    def from_string(self, s):
        """
        Parse the value from a string. May be overridden by subclasses.
        """
        return s

    @property
    def alchemy_expression(self):
        return self._alchemy_offset_value(self.offset, self.agg_function)

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

    def extract(self, document):
        v = get_doc_offset(self.offset, document)
        if v is None:
            return None

        return self.from_string(v)

    def evaluate(self, ctx):
        return self.extract(ctx)


class IntDocField(SimpleDocField):
    def value_to_alchemy(self, value):
        return cast(value, postgres.INTEGER)

    def between(self, low, high):
        return ValueBetweenExpression(self, low, high)

    def from_string(self, s):
        return int(s)


class DoubleDocField(SimpleDocField):
    def value_to_alchemy(self, value):
        return cast(value, postgres.DOUBLE_PRECISION)

    def between(self, low, high):
        return ValueBetweenExpression(self, low, high)

    def from_string(self, s):
        return float(s)


class RangeDocField(PgDocField):
    """
    A range of values. Has min and max values, which may be calculated from multiple
    values in the document.
    """

    def __init__(self, name, description, alchemy_column, indexed, min_offset=None, max_offset=None):
        super(RangeDocField, self).__init__(name, description, alchemy_column, indexed)
        self.min_offset = min_offset
        self.max_offset = max_offset

    @property
    def alchemy_create_range(self):
        raise NotImplementedError('range type')

    @property
    def postgres_index_type(self):
        return 'gist'

    @property
    def alchemy_expression(self):
        return self.alchemy_create_range(
            self._alchemy_offset_value(self.min_offset, func.least),
            self._alchemy_offset_value(self.max_offset, func.greatest),
            # Inclusive on both sides.
            '[]'
        )

    def __eq__(self, value):
        """
        :rtype: Expression
        """
        return RangeContainsExpression(self, self.value_to_alchemy(value))

    def extract(self, document):
        def safe_get_doc_offset(offset, document):
            try:
                return get_doc_offset(offset, document)
            except KeyError:
                return None

        min_vals = [v for v in (safe_get_doc_offset(offset, document) for offset in self.min_offset) if v]
        max_vals = [v for v in (safe_get_doc_offset(offset, document) for offset in self.max_offset) if v]

        min_val = min(min_vals) if min_vals else None
        max_val = max(max_vals) if max_vals else None

        if not min_val and not max_val:
            return None

        return Range(min_val, max_val)


class NumericRangeDocField(RangeDocField):
    def value_to_alchemy(self, value):
        return cast(value, postgres.NUMERIC)

    @property
    def alchemy_create_range(self):
        # Call the postgres 'numrange()' function, hinting to SQLAlchemy that it returns a NUMRANGE.
        return partial(func.numrange, type_=NUMRANGE)

    def between(self, low, high):
        """
        :rtype: Expression
        """
        return RangeBetweenExpression(self, low, high, _range_class=NumericRange)


class IntRangeDocField(RangeDocField):
    def value_to_alchemy(self, value):
        return cast(value, postgres.INTEGER)

    @property
    def alchemy_create_range(self):
        return partial(func.numrange, type_=INT4RANGE)

    def between(self, low, high):
        """
        :rtype: Expression
        """
        return RangeBetweenExpression(self, low, high, _range_class=NumericRange)


class DoubleRangeDocField(RangeDocField):
    @classmethod
    def value_to_alchemy(cls, value):
        return cast(value, DOUBLE_PRECISION)

    @property
    def alchemy_create_range(self):
        return partial(func.agdc.float8range, type_=FLOAT8RANGE)

    def between(self, low, high):
        """
        :rtype: Expression
        """
        return RangeBetweenExpression(self, low, high, _range_class=NumericRange)


class DateRangeDocField(RangeDocField):
    def value_to_alchemy(self, value):
        if isinstance(value, datetime):
            return self._default_utc(value)
        return func.agdc.common_timestamp(value)

    @property
    def alchemy_create_range(self):
        # Call the postgres 'tstzrange()' function, hinting to SQLAlchemy that it returns a TSTZRANGE.
        return partial(func.tstzrange, type_=TSTZRANGE)

    def _default_utc(self, d):
        if d.tzinfo is None:
            return d.replace(tzinfo=tz.tzutc())
        return d

    def between(self, low, high):
        """
        :rtype: Expression
        """
        return RangeBetweenExpression(
            self,
            self._default_utc(low),
            self._default_utc(high),
            _range_class=DateTimeTZRange
        )


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


class ValueBetweenExpression(PgExpression):
    def __init__(self, field, low_value, high_value):
        super(ValueBetweenExpression, self).__init__(field)
        self.low_value = low_value
        self.high_value = high_value

    @property
    def alchemy_expression(self):
        if self.low_value is not None and self.high_value is not None:
            return and_(self.field.alchemy_expression >= self.low_value,
                        self.field.alchemy_expression < self.high_value)
        if self.low_value is not None:
            return self.field.alchemy_expression >= self.low_value
        if self.high_value is not None:
            return self.field.alchemy_expression < self.high_value


class RangeBetweenExpression(PgExpression):
    def __init__(self, field, low_value, high_value, _range_class):
        super(RangeBetweenExpression, self).__init__(field)
        self.low_value = low_value
        self.high_value = high_value
        self._range_class = _range_class

    @property
    def alchemy_expression(self):
        return self.field.alchemy_expression.overlaps(
            self._range_class(self.low_value, self.high_value)
        )


class RangeContainsExpression(PgExpression):
    def __init__(self, field, value):
        super(RangeContainsExpression, self).__init__(field)
        self.value = value

    @property
    def alchemy_expression(self):
        return self.field.alchemy_expression.contains(self.value)


class EqualsExpression(PgExpression):
    def __init__(self, field, value):
        super(EqualsExpression, self).__init__(field)
        self.value = value

    @property
    def alchemy_expression(self):
        return self.field.alchemy_expression == self.value

    def evaluate(self, ctx):
        return self.field.evaluate(ctx) == self.value


def parse_fields(doc, table_column):
    """
    Parse a field spec document into objects.

    Example document:

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

    def _get_field(name, descriptor, column):
        """

        :type name: str
        :type descriptor: dict
        :param column: SQLAlchemy table column
        :rtype: PgField
        """
        type_map = {
            'numeric-range': NumericRangeDocField,
            'double-range': DoubleRangeDocField,
            'integer-range': IntRangeDocField,
            'datetime-range': DateRangeDocField,
            'string': SimpleDocField,
            'integer': IntDocField,
            'double': DoubleDocField,
            # For backwards compatibility
            'float-range': NumericRangeDocField,
        }
        ctorargs = descriptor.copy()
        type_name = ctorargs.pop('type', 'string')
        description = ctorargs.pop('description', None)
        indexed_val = ctorargs.pop('indexed', "true")
        indexed = indexed_val.lower() == 'true' if isinstance(indexed_val, compat.string_types) else indexed_val

        field_class = type_map.get(type_name)
        if not field_class:
            raise ValueError(('Field %r has unknown type %r.'
                              ' Available types are: %r') % (name, type_name, list(type_map.keys())))
        try:
            return field_class(name, description, column, indexed, **ctorargs)
        except TypeError as e:
            raise RuntimeError(
                'Field {name} has unexpected argument for a {type}'.format(
                    name=name, type=type_name
                ), e
            )

    return {name: _get_field(name, descriptor, table_column) for name, descriptor in doc.items()}
