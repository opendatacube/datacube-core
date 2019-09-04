"""
Utility functions
"""
import os
from .documents import read_documents, validate_document


class DatacubeException(Exception):
    """Your Data Cube has malfunctioned"""
    pass


def schema_validated(schema):
    """
    Decorate a class to enable validating its definition against a JSON Schema file.

    Adds a self.validate() method which takes a dict used to populate the instantiated class.

    :param pathlib.Path schema: filename of the json schema, relative to `SCHEMA_PATH`
    :return: wrapped class
    """

    def validate(cls, document):
        return validate_document(document, cls.schema, schema.parent)

    def decorate(cls):
        cls.schema = next(iter(read_documents(schema)))[1]
        cls.validate = classmethod(validate)
        return cls

    return decorate


def gen_password(num_random_bytes=12):
    """
    Generate random password
    """
    import base64
    return base64.urlsafe_b64encode(os.urandom(num_random_bytes)).decode('utf-8')


def _readable_offset(offset):
    return '.'.join(map(str, offset))
