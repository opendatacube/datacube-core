# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Literal

from sqlalchemy import Connection, text
from sqlalchemy.exc import ProgrammingError

from ._utils import as_role

_LOG = logging.getLogger(__name__)


def transfers_required(
    conn: Connection,
    new_owner: str,
    schema: str,
    object_type: Literal["tables", "matviews", "views"],
    objects: list[str] | None = None,
    prefix: str | None = None,
) -> list[tuple[str, str]]:
    """
    Determine which objects in a schema need to be transferred to a new owner.

    One and only one of objects or prefix must be specified.

    :param conn: A SQLAlchemy connection object
    :param new_owner: The new owner, the database role that should own the objects.
    :param schema: The schema containing the objects.
    :param object_type: The type of objects to check. One of "tables", "matviews", "views".
    :param objects: A list of object names to check.
    :param prefix: An object name prefix to check.

    :return: A list of (name, old_owner) tuples of matching objects.
    """
    if objects is None == prefix is None:
        raise ValueError("Must specify one of either objects or prefix")

    transfers: list[tuple[str, str]] = []
    defs = {
        "tables": ("tablename", "tableowner", "pg_tables"),
        "matviews": ("matviewname", "matviewowner", "pg_matviews"),
        "views": ("viewname", "viewowner", "pg_views"),
    }
    n, o, t = defs[object_type]
    sql = f"select {n}, {o} from {t} where schemaname = '{schema}'"
    if objects is not None:
        sql += f" and {n} in {tuple(objects)}"
    else:
        sql += f" and {n} like '{prefix}%'"
    for row in conn.execute(text(sql)):
        if row[1] != new_owner:
            transfers.append((row[0], row[1]))
    return transfers


def transfer_ownership(
    conn: Connection,
    schema: str,
    obj_name: str,
    current_owner: str,
    new_owner: str,
    object_type: Literal["tables", "matviews", "views"],
) -> bool:
    """
    Transfer ownership of a database object to a new owner.

    :param conn: A SQLAlchemy connection object
    :param schema: The schema containing the object.
    :param obj_name: The name of the object.
    :param current_owner: The current owner of the object.
    :param new_owner: The desired new owner of the object.
    :param object_type: The type of object, one of "tables", "matviews", "views".
    :return: True if the transfer succeeded, False otherwise.
    """
    objs = {
        "tables": "table",
        "matviews": "materialized view",
        "views": "view",
    }
    sql = f"alter {objs[object_type]} {schema}.{obj_name} owner to {new_owner}"
    try:
        # Attempt as session user
        # (hopefully we're a superuser or have both roles and required perms)
        conn.execute(text(sql))
        return True
    except ProgrammingError:
        _LOG.info(
            "Cannot transfer ownership as session user.  Trying with appropriate role..."
        )

    matviews = object_type == "matviews"
    desired_role = new_owner if matviews else current_owner
    try:
        with as_role(conn, desired_role) as attempt_conn:
            attempt_conn.execute(text(sql))
        return True
    except ProgrammingError:
        _LOG.warning(
            f"Cannot transfer ownership of {objs[object_type]} {obj_name} "
            f"from {current_owner} to {new_owner}: session user is not a "
            f"superuser or session user cannot become {desired_role} or "
            f"{desired_role} does not have CREATE permission on {schema} schema."
        )
        return False
