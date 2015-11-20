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
from psycopg2._range import NumericRange
from sqlalchemy import cast, Index, TIMESTAMP
from sqlalchemy import func
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import NUMRANGE, TSTZRANGE

from datacube.index.fields import Expression, Field
from datacube.index.postgres.tables import DATASET, STORAGE_UNIT

DEFAULT_FIELDS_FILE = Path(__file__).parent.joinpath('document-fields.yaml')


class PgField(Field):
    """
    A field within a Postgres JSONB document.
    """

    def __init__(self, name, descriptor, jsonb_column):
        super(PgField, self).__init__(name)
        self.descriptor = descriptor
        # The underlying SQLAlchemy JSONB column. (eg. DATASET.c.metadata)
        self.alchemy_jsonb_column = jsonb_column

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


class SimpleField(PgField):
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


class RangeField(PgField):
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
        fields = [self.alchemy_jsonb_column[offset].astext for offset in doc_offsets]

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


class FloatRangeField(RangeField):
    @property
    def alchemy_casted_type(self):
        return postgres.NUMERIC

    @property
    def alchemy_create_range(self):
        # Call the postgres 'numrange()' function, hinting to SQLAlchemy that it returns a NUMRANGE.
        return functools.partial(func.numrange, type_=NUMRANGE)


class DateRangeField(RangeField):
    @property
    def alchemy_casted_type(self):
        return TIMESTAMP(timezone=True)

    @property
    def alchemy_create_range(self):
        # Call the postgres 'tstzrange()' function, hinting to SQLAlchemy that it returns a TSTZRANGE.
        return functools.partial(func.tstzrange, type_=TSTZRANGE)


class PgExpression(Expression):
    @property
    def alchemy_expression(self):
        """
        Get an SQLAlchemy expression for accessing this field.
        :return:
        """
        raise NotImplementedError('alchemy expression')


class RangeBetweenExpression(PgExpression):
    def __init__(self, field, low_value, high_value):
        self.low_value = low_value
        self.high_value = high_value
        self.field = field

    @property
    def alchemy_expression(self):
        return self.field.alchemy_expression.overlaps(
            NumericRange(self.low_value, self.high_value)
        )


class EqualsExpression(PgExpression):
    def __init__(self, field, value):
        self.field = field
        self.value = value

    @property
    def alchemy_expression(self):
        return self.field.alchemy_expression == self.value


class FieldCollection(object):
    def __init__(self):
        # Three-level dict: metadata_type, doc_type, field_info
        # eg. 'eo' -> 'dataset' -> 'lat'
        #  or 'eo' -> 'storage' -> time
        self.docs = defaultdict(functools.partial(defaultdict, dict))

        # Supported document types:
        self.document_types = {
            'dataset': DATASET.c.metadata,
            'storage_unit': STORAGE_UNIT.c.descriptor,
        }

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
                table_field = self.document_types.get(doc_type)
                if table_field is None:
                    raise RuntimeError('Unknown document type %r. Expected one of %r' %
                                       (doc_type, self.document_types.keys()))

                self.docs[metadata_type][doc_type].update(_parse_fields(fields, table_field))

    def get(self, metadata_type, document_type, name):
        """
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
            'float-range': FloatRangeField,
            'datetime-range': DateRangeField,
            'string': SimpleField
        }
        type_name = descriptor.get('type') or 'string'
        return type_map.get(type_name)(name, descriptor, column)

    return {name: _get_field(name, descriptor, table_column) for name, descriptor in doc.items()}
