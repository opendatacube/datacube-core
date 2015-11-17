# coding=utf-8
"""
Query datasets and storage units.
"""
from __future__ import absolute_import

from sqlalchemy import cast, Index
from sqlalchemy import func
from sqlalchemy.dialects import postgresql as postgres

from datacube.index.tables import DATASET


class Expression(object):
    def __init__(self, field_name):
        """
        :type field_name: str
        :type between_values:
        """
        self.field_name = field_name

    def as_db_expression(self):
        pass


class BetweenExpression(Expression):
    def __init__(self, field_name, min_value, max_value):
        super(BetweenExpression, self).__init__(field_name)
        self.min_value = min_value
        self.max_value = max_value


class EqualsExpression(Expression):
    def __init__(self, field_name, value):
        super(EqualsExpression, self).__init__(field_name)
        self.value = value


class Field(object):
    def __init__(self, name, descriptor):
        self.name = name
        self.descriptor = descriptor


class RangeField(Field):
    def __init__(self, min_offsets, max_offsets, name, descriptor):
        super(RangeField, self).__init__(name, descriptor)
        self.min_offsets = min_offsets
        self.max_offsets = max_offsets


class FloatRangeField(object):
    def __init__(self, name, descriptor):
        self.name = name
        self.descriptor = descriptor

    @property
    def min_offsets(self):
        return self.descriptor['min']

    @property
    def max_offsets(self):
        return self.descriptor['max']

    def _get_expr(self, doc_offsets, agg_function, casted_type):
        fields = [cast(DATASET.c.metadata[offset], casted_type) for offset in doc_offsets]
        # If there's multiple fields, we aggregate them (eg. "min()"). Otherwise use the one.
        return func.least(*fields) if len(fields) > 1 else fields[0]

    def get_alchemy_expression(self):
        return func.numrange(
            self._get_expr(self.min_offsets, func.least, postgres.NUMERIC),
            self._get_expr(self.max_offsets, func.greatest, postgres.NUMERIC),
            '[]'
        )


DATASET_QUERY_FIELDS = {
    'eo': {
        'lat': {
            'type': 'float-range',
            'min': [
                ['extent', 'coord', 'ul', 'lat'],
                ['extent', 'coord', 'll', 'lat']
            ],
            'max': [
                ['extent', 'coord', 'ur', 'lat'],
                ['extent', 'coord', 'lr', 'lat']
            ]
        },
        'lon': {
            # numeric-range?
            'type': 'float-range',
            'min': [
                ['extent', 'coord', 'll', 'lon'],
                ['extent', 'coord', 'lr', 'lon']
            ],
            'max': [
                ['extent', 'coord', 'ul', 'lon'],
                ['extent', 'coord', 'ur', 'lon']
            ]
        },
        't': {
            'type': 'datetime-range',
            'min': [
                ['extent', 'from_dt']
            ],
            'max': [
                ['extent', 'to_dt']
            ]
        },
        # Default to string type.
        'satellite': {
            'offset': ['platform', 'code']
        },
        'sensor': {
            'offset': ['instrument', 'name']
        },
        'gsi': {
            'label': 'Groundstation identifier',
            'offset': ['acquisition', 'groundstation', 'code']
        },
        'sat_path': {
            'type': 'float-range',
            'min': [
                ['image', 'satellite_ref_point_start', 'x']
            ],
            'max': [
                ['image', 'satellite_ref_point_end', 'x']
            ]
        },
        'sat_row': {
            'type': 'float-range',
            'min': [
                ['image', 'satellite_ref_point_start', 'y']
            ],
            'max': [
                ['image', 'satellite_ref_point_end', 'y']
            ]
        }
    }
}

func.numrange(
    func.least(
        cast(DATASET.c.metadata[('extent', 'coord', 'ul', 'lat')].astext, postgres.NUMERIC),
        cast(DATASET.c.metadata[('extent', 'coord', 'll', 'lat')].astext, postgres.NUMERIC)
    ),
    func.greatest(
        cast(DATASET.c.metadata[('extent', 'coord', 'ur', 'lat')].astext, postgres.NUMERIC),
        cast(DATASET.c.metadata[('extent', 'coord', 'lr', 'lat')].astext, postgres.NUMERIC)
    ),
    # Inclusive on both sides.
    '[]'
)
lat_range_index = Index(
    'ix_dataset_metadata_lat_range',
    func.numrange(
        func.least(
            cast(DATASET.c.metadata[('extent', 'coord', 'ul', 'lat')].astext, postgres.NUMERIC),
            cast(DATASET.c.metadata[('extent', 'coord', 'll', 'lat')].astext, postgres.NUMERIC)
        ),
        func.greatest(
            cast(DATASET.c.metadata[('extent', 'coord', 'ur', 'lat')].astext, postgres.NUMERIC),
            cast(DATASET.c.metadata[('extent', 'coord', 'lr', 'lat')].astext, postgres.NUMERIC)
        ),
        # Inclusive on both sides.
        '[]'
    ),
    postgresql_using='gist'
)


def _yield_all_expressions_to_index(query_fields):
    """

    :type query_fields: dict
    :rtype: list[(type, offset)]
    """
    # Yield Alchemy expressions to index?
    pass


class DataQuery(object):
    def __init__(self, db):
        """
        :type db: datacube.index._core_db.Db
        """
        self.db = db

    def search_datasets(self, *expressions):
        """
        :type expressions: list[Expression]
        """
