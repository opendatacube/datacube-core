# coding=utf-8
"""
Query datasets and storage units.
"""
from __future__ import absolute_import


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
