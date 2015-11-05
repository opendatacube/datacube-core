# coding=utf-8
"""
Modules for interfacing with the index/database.
"""
from __future__ import absolute_import

from ._add import add_dataset_simple


# Dummy implementation: TODO.
def contains_dataset(dataset):
    """
    Have we already indexed this dataset?

    :type dataset: datacube.model.Dataset
    """
    # We haven't indexed anything.
    return False


__all__ = ['add_dataset_simple', 'contains_dataset']
