# coding=utf-8
# pylint: disable=abstract-method
"""
Build and index fields within documents.
"""
from __future__ import absolute_import

import functools
from collections import defaultdict
from string import lower

import yaml
from pathlib import Path
from psycopg2.extras import NumericRange
from sqlalchemy import cast, Index, TIMESTAMP
from sqlalchemy import func
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import NUMRANGE, TSTZRANGE

from datacube.index.fields import Expression, Field
from datacube.index.postgres.tables import DATASET, STORAGE_UNIT

DEFAULT_FIELDS_FILE = Path(__file__).parent.joinpath('document-fields.yaml')


class PgField(Field):
    """
    Postgres implementation of a searchable field. May be a value inside
    a JSONB column.
    """

    def __init__(self, name, descriptor, alchemy_column):
        super(PgField, self).__init__(name)
        self.descriptor = descriptor
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

    def as_alchemy_index(self, prefix):
        """
        Build an SQLAlchemy index for this field.

        :type prefix: str
        """
        return Index(
            'ix_field_{prefix}_{name}'.format(
                prefix=lower(prefix),
                name=lower(self.name),
            ),
            self.alchemy_expression,
            postgresql_using=self.postgres_index_type
        )

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
    @property
    def alchemy_expression(self):
        return self.alchemy_column

    def as_alchemy_index(self, prefix):
        # Don't add extra indexes for native fields.
        return None


class SimpleDocField(PgField):
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

    @property
    def alchemy_create_range(self):
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
        fields = [self.alchemy_column[offset].astext for offset in doc_offsets]

        if casted_type:
            fields = [cast(field, casted_type) for field in fields]

        # If there's multiple fields, we aggregate them (eg. "min()"). Otherwise use the one.
        return agg_function(*fields) if len(fields) > 1 else fields[0]

    @property
    def alchemy_expression(self):
        return self.alchemy_create_range(
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


class FieldCollection(object):
    def __init__(self):
        # Supported document types:
        self.document_types = {
            'dataset': (
                DATASET.c.metadata,
                # Native search fields.
                {
                    'id': NativeField('id', {}, DATASET.c.id),
                    'metadata_path': NativeField('metadata_path', {}, DATASET.c.metadata_path)
                }
            ),
            'storage_unit': (
                STORAGE_UNIT.c.descriptor,
                # Native search fields.
                {
                    'id': NativeField('id', {}, STORAGE_UNIT.c.id),
                    'path': NativeField('path', {}, STORAGE_UNIT.c.path)
                }
            ),
        }

        # Three-level dict: metadata_type, doc_type, field_info
        # eg. 'eo' -> 'dataset' -> 'lat'
        #  or 'eo' -> 'storage' -> time
        self.docs = defaultdict(self._metadata_type_defaults)

    def _metadata_type_defaults(self):
        return dict([(name, default[1].copy()) for name, default in self.document_types.items()])

    def load_from_file(self, path_):
        """
        :type path_: pathlib.Path
        """
        self.load_from_doc(yaml.load(path_.open('r')))

    def load_from_doc(self, doc):
        """
        :type doc: dict
        """
        for metadata_type, doc_types in doc.items():
            for doc_type, fields in doc_types.items():
                table_field, defaults = self.document_types.get(doc_type)
                if table_field is None:
                    raise RuntimeError('Unknown document type %r. Expected one of %r' %
                                       (doc_type, self.document_types.keys()))

                self.docs[metadata_type][doc_type].update(_parse_fields(fields, table_field))

    def items(self):
        for metadata_type, doc_types in self.docs.items():
            for doc_type, fields in doc_types.items():
                for name, field in fields.items():
                    yield (metadata_type, doc_type, field)

    def get(self, metadata_type, document_type, name):
        """
        :type metadata_type: str
        :type document_type: str
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        return self.docs[metadata_type][document_type].get(name)


def _parse_fields(doc, table_column):
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

    def _get_field(name, descriptor, column):
        type_map = {
            'float-range': FloatRangeDocField,
            'datetime-range': DateRangeDocField,
            'string': SimpleDocField
        }
        type_name = descriptor.get('type') or 'string'
        return type_map.get(type_name)(name, descriptor, column)

    return {name: _get_field(name, descriptor, table_column) for name, descriptor in doc.items()}
