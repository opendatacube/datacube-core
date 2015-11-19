# coding=utf-8
"""
Common datatypes for DB drivers.
"""
from __future__ import absolute_import


# For the search API.


class Field(object):
    """
    A searchable field within a dataset/storage metadata document.
    """

    def __init__(self, name):
        self.name = name

    def __eq__(self, value):
        """
        Is this field equal to a value?
        :rtype: Expression
        """
        raise NotImplementedError('equals expression')

    def between(self, low, high):
        """
        Is this field in a range?
        :rtype: Expression
        """
        raise NotImplementedError('between expression')


class Expression(object):
    # No properties at the moment. These are built and returned by the
    # DB driver (from Field methods), so they're mostly an opaque token.

    # A simple equals implementation for comparison in test code.
    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return self.__dict__ == other.__dict__
