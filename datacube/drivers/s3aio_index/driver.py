from __future__ import absolute_import

from .index import S3AIOIndex


class S3IndexDriver(object):
    @staticmethod
    def connect_to_index(config, application_name=None, validate_connection=True):
        return S3AIOIndex.from_config(config, application_name, validate_connection)


def index_driver_init():
    return S3IndexDriver()
