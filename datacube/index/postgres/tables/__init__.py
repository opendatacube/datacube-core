# coding=utf-8
"""
SQL Alchemy table definitions.
"""
from __future__ import absolute_import

from ._core import ensure_db, database_exists, schema_is_latest, update_schema
from ._core import schema_qualified, has_role, grant_role, create_user, drop_user, USER_ROLES
from ._schema import DATASET, DATASET_SOURCE, DATASET_LOCATION, DATASET_TYPE, METADATA_TYPE
from ._sql import CreateView, FLOAT8RANGE, PGNAME


def _pg_exists(conn, name):
    """
    Does a postgres object exist?
    :rtype bool
    """
    return conn.execute("SELECT to_regclass(%s)", name).scalar() is not None
