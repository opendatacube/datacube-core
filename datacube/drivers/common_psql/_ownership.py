# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Literal

from sqlalchemy import Connection, text
from sqlalchemy.exc import ProgrammingError

from ._connection import as_role

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
    Returns a list of (name, old_owner) tuples for objects that need to be transferred to the new owner.
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
    objs = {
        "tables": "table",
        "matviews": "materialized view",
        "views": "view",
    }
    sql = f"alter {objs[object_type]} {schema}.{obj_name} owner to {new_owner}"
    try:
        # Attempt as session user (hopefully we're a superuser or have both roles and required perms)
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
            f"Cannot transfer ownership of {objs[object_type]} {obj_name} from {current_owner} to {new_owner}: "
            f"session user is not a superuser or session user cannot become {desired_role} or "
            f"{desired_role} does not have CREATE permission on cubedash schema."
        )
        return False
