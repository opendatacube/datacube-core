from __future__ import absolute_import

from .index import S3BlockIndex


def index_driver_init():
    return S3BlockIndex
