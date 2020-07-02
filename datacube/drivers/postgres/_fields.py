# coding=utf-8
# pylint: disable=abstract-method
"""
Build and index fields within documents.
"""
from collections import namedtuple
from datetime import datetime, date
from decimal import Decimal

from dateutil import tz
from psycopg2.extras import NumericRange, DateTimeTZRange
from sqlalchemy import cast, func, and_
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.dialects.postgresql import NUMRANGE, TSTZRANGE
from sqlalchemy.sql import ColumnElement

from datacube import utils
from datacube.model.fields import Expression, Field
from datacube.model import Range
from datacube.utils import get_doc_offset_safe
from .sql import FLOAT8RANGE

from typing import Any, Callable, Tuple, Union


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

    def parse_value(self, value):
        """
        Parse the value from a string. May be overridden by subclasses.
        """
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

        if isinstance(doc_offsets[0], str):
            # It's a single offset.
            doc_offsets = [doc_offsets]

        alchemy_values = [self.value_to_alchemy(self.alchemy_column[offset].astext) for offset in doc_offsets]
        # If there's multiple fields, we aggregate them (eg. "min()"). Otherwise use the one.
        return agg_function(*alchemy_values) if len(alchemy_values) > 1 else alchemy_values[0]

    def _extract_offset_value(self, doc, doc_offsets, agg_function):
        """
        Extract a value for the given document offsets.

        Same as _alchemy_offset_value(), but returns the value instead of an sqlalchemy expression to calc the value.
        """
        if not doc_offsets:
            raise ValueError("Value requires at least one offset")

        if isinstance(doc_offsets[0], str):
            # It's a single offset.
            doc_offsets = [doc_offsets]

        values = (get_doc_offset_safe(offset, doc) for offset in doc_offsets)
        values = [self.parse_value(v) for v in values if v is not None]

        if not values:
            return None
        if len(values) == 1:
            return values[0]
        return agg_function(*values)


class SimpleDocField(PgDocField):
    """
    A field with a single value (eg. String, int) calculated as an offset inside a (jsonb) document.
    """

    def __init__(self, name, description, alchemy_column, indexed, offset=None, selection='first'):
        super(SimpleDocField, self).__init__(name, description, alchemy_column, indexed)
        self.offset = offset
        if selection not in SELECTION_TYPES:
            raise ValueError(
                "Unknown field selection type %s. Expected one of: %r" % (selection, (SELECTION_TYPES,),)
            )
        self.aggregation = SELECTION_TYPES[selection]

    @property
    def alchemy_expression(self):
        return self._alchemy_offset_value(self.offset, self.aggregation.pg_calc)

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
        return self._extract_offset_value(document, self.offset, self.aggregation.calc)

    def evaluate(self, ctx):
        return self.extract(ctx)


class IntDocField(SimpleDocField):
    type_name = 'integer'

    def value_to_alchemy(self, value):
        return cast(value, postgres.INTEGER)

    def between(self, low, high):
        return ValueBetweenExpression(self, low, high)

    def parse_value(self, value):
        return int(value)


class NumericDocField(SimpleDocField):
    type_name = 'numeric'

    def value_to_alchemy(self, value):
        return cast(value, postgres.NUMERIC)

    def between(self, low, high):
        return ValueBetweenExpression(self, low, high)

    def parse_value(self, value):
        return Decimal(value)


class DoubleDocField(SimpleDocField):
    type_name = 'double'

    def value_to_alchemy(self, value):
        return cast(value, postgres.DOUBLE_PRECISION)

    def between(self, low, high):
        return ValueBetweenExpression(self, low, high)

    def parse_value(self, value):
        return float(value)


class DateDocField(SimpleDocField):
    type_name = 'datetime'

    def value_to_alchemy(self, value):
        # type: (Union[datetime, date, str, ColumnElement]) -> Union[datetime, date, str, ColumnElement]
        """
        Wrap a value as needed for this field type.
        """
        if isinstance(value, datetime):
            return _default_utc(value)
        # SQLAlchemy expression or string are parsed in pg as dates.
        elif isinstance(value, (ColumnElement, str)):
            return func.agdc.common_timestamp(value)
        else:
            raise ValueError("Value not readable as date: %r" % (value,))

    def between(self, low, high):
        return ValueBetweenExpression(self, low, high)

    def parse_value(self, value):
        return utils.parse_time(value)

    @property
    def day(self):
        """Get field truncated to the day"""
        return NativeField(
            '{}_day'.format(self.name),
            'Day of {}'.format(self.description),
            self.alchemy_column,
            alchemy_expression=cast(func.date_trunc('day', self.alchemy_expression), postgres.TIMESTAMP)
        )


class RangeDocField(PgDocField):
    """
    A range of values. Has min and max values, which may be calculated from multiple
    values in the document.
    """
    FIELD_CLASS = SimpleDocField

    def __init__(self, name, description, alchemy_column, indexed, min_offset=None, max_offset=None):
        super(RangeDocField, self).__init__(name, description, alchemy_column, indexed)
        self.lower = self.FIELD_CLASS(
            name + '_lower',
            description,
            alchemy_column,
            indexed=False,
            offset=min_offset,
            selection='least'
        )
        self.greater = self.FIELD_CLASS(
            name + '_greater',
            description,
            alchemy_column,
            indexed=False,
            offset=max_offset,
            selection='greatest'
        )

    def value_to_alchemy(self, value):
        raise NotImplementedError('range type')

    @property
    def postgres_index_type(self):
        return 'gist'

    @property
    def alchemy_expression(self):
        return self.value_to_alchemy((self.lower.alchemy_expression, self.greater.alchemy_expression))

    def __eq__(self, value):
        """
        :rtype: Expression
        """
        # Lower and higher are interchangeable here: they're the same type.
        casted_val = self.lower.value_to_alchemy(value)
        return RangeContainsExpression(self, casted_val)

    def extract(self, document):
        min_val = self.lower.extract(document)
        max_val = self.greater.extract(document)
        if not min_val and not max_val:
            return None
        return Range(min_val, max_val)


class NumericRangeDocField(RangeDocField):
    FIELD_CLASS = NumericDocField
    type_name = 'numeric-range'

    def value_to_alchemy(self, value):
        low, high = value
        return func.numrange(
            low, high,
            # Inclusive on both sides.
            '[]',
            type_=NUMRANGE,
        )

    def between(self, low, high):
        """
        :rtype: Expression
        """
        return RangeBetweenExpression(self, low, high, _range_class=NumericRange)


class IntRangeDocField(RangeDocField):
    FIELD_CLASS = IntDocField
    type_name = 'integer-range'

    def value_to_alchemy(self, value):
        low, high = value
        return func.numrange(
            low, high,
            # Inclusive on both sides.
            '[]',
            type_=INT4RANGE,
        )

    def between(self, low, high):
        """
        :rtype: Expression
        """
        return RangeBetweenExpression(self, low, high, _range_class=NumericRange)


class DoubleRangeDocField(RangeDocField):
    FIELD_CLASS = DoubleDocField
    type_name = 'double-range'

    def value_to_alchemy(self, value):
        low, high = value
        return func.agdc.float8range(
            low, high,
            # Inclusive on both sides.
            '[]',
            type_=FLOAT8RANGE,
        )

    def between(self, low, high):
        """
        :rtype: Expression
        """
        return RangeBetweenExpression(self, low, high, _range_class=NumericRange)


class DateRangeDocField(RangeDocField):
    FIELD_CLASS = DateDocField
    type_name = 'datetime-range'

    def value_to_alchemy(self, value):
        low, high = value
        return func.tstzrange(
            low, high,
            # Inclusive on both sides.
            '[]',
            type_=TSTZRANGE,
        )

    def between(self, low, high):
        """
        :rtype: Expression
        """
        low = _number_implies_year(low)
        high = _number_implies_year(high)

        if isinstance(low, datetime) and isinstance(high, datetime):
            return RangeBetweenExpression(
                self,
                _default_utc(low),
                _default_utc(high),
                _range_class=DateTimeTZRange
            )
        else:
            raise ValueError("Unknown comparison type for date range: "
                             "expecting datetimes, got: (%r, %r)" % (low, high))


def _number_implies_year(v):
    # type: (Union[int, datetime]) -> datetime
    """
    >>> _number_implies_year(1994)
    datetime.datetime(1994, 1, 1, 0, 0)
    >>> _number_implies_year(datetime(1994, 4, 4))
    datetime.datetime(1994, 4, 4, 0, 0)
    """
    if isinstance(v, int):
        return datetime(v, 1, 1)
    # The expression module parses all number ranges as floats.
    if isinstance(v, float):
        return datetime(int(v), 1, 1)

    return v


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

        raise ValueError('Expect at least one of [low,high] to be set')


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

    # Implementations of fields for this driver
    types = {
        SimpleDocField,
        IntDocField,
        DoubleDocField,
        DateDocField,

        NumericRangeDocField,
        IntRangeDocField,
        DoubleRangeDocField,
        DateRangeDocField,
    }
    type_map = {f.type_name: f for f in types}
    # An alias for backwards compatibility
    type_map['float-range'] = NumericRangeDocField

    # No later field should have overridden string
    assert type_map['string'] == SimpleDocField

    def _get_field(name, descriptor, column):
        """
        :type name: str
        :type descriptor: dict
        :param column: SQLAlchemy table column
        :rtype: PgField
        """
        ctorargs = descriptor.copy()
        type_name = ctorargs.pop('type', 'string')
        description = ctorargs.pop('description', None)
        indexed_val = ctorargs.pop('indexed', "true")
        indexed = indexed_val.lower() == 'true' if isinstance(indexed_val, str) else indexed_val

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


def _coalesce(*values):
    """
    Return first non-none value.
    Return None if all values are None, or there are no values passed in.

    >>> _coalesce(1, 2)
    1
    >>> _coalesce(None, 2, 3)
    2
    >>> _coalesce(None, None, 3, None, 5)
    3
    """
    for v in values:
        if v is not None:
            return v
    return None


def _default_utc(d):
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d


# How to choose/combine multiple doc values.
ValueAggregation = namedtuple('ValueAggregation', ('calc', 'pg_calc'))
SELECTION_TYPES = {
    # First non-null
    'first': ValueAggregation(_coalesce, func.coalesce),
    # min/max
    'least': ValueAggregation(min, func.least),
    'greatest': ValueAggregation(max, func.greatest),
}
