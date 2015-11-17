# coding=utf-8
"""
Query datasets and storage units.
"""
from __future__ import absolute_import

from sqlalchemy import cast
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


DATASET_QUERY_FIELDS = {
    'eo': {
        'lat': {
            'type': 'float',
            'offsets': {
                'min': [
                    ['extent', 'coord', 'ul', 'lat'],
                    ['extent', 'coord', 'll', 'lat']
                ],
                'max': [
                    ['extent', 'coord', 'ur', 'lat'],
                    ['extent', 'coord', 'lr', 'lat']
                ]
            }
        },
        'lon': {
            # numeric-range?
            'type': 'float',
            'offsets': {
                'min': [
                    ['extent', 'coord', 'll', 'lon'],
                    ['extent', 'coord', 'lr', 'lon']
                ],
                'max': [
                    ['extent', 'coord', 'ul', 'lon'],
                    ['extent', 'coord', 'ur', 'lon']
                ]
            }
        },
        't': {
            'type': 'datetime',
            'offsets': {
                'min': [
                    ['extent', 'from_dt']
                ],
                'max': [
                    ['extent', 'to_dt']
                ]
            }
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
            'offsets': {
                'min': [
                    ['image', 'satellite_ref_point_start', 'x']
                ],
                'max': [
                    ['image', 'satellite_ref_point_end', 'x']
                ]
            }
        },
        'sat_row': {
            'offsets': {
                'min': [
                    ['image', 'satellite_ref_point_start', 'y']
                ],
                'max': [
                    ['image', 'satellite_ref_point_end', 'y']
                ]
            }
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
