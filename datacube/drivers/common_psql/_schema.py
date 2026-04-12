# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.schema import DropSchema

TYPE_CHECKING = False
if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine


def has_schema(engine: Engine, schema_name: str) -> bool:
    """
    Check if a schema exists.
    :param engine: An SQLAlchemy engine
    :param schema_name: The name of the schema to check
    :return: True if the schema exists
    """
    return inspect(engine).has_schema(schema_name)


def create_schema(
    conn: Connection, name: str, if_exists: bool = True, owner: str | None = None
) -> None:
    """
    Create a schema.

    :param conn: An SQLAlchemy connection object.
    :param name: Name of schema to create
    :param if_exists: If true, ignore if schema already exists (default True)
    :param owner: Owner of new schema. If None (the default), owner will be the
                  active role of conn.
    """
    if_not_exists_clause = "if not exists" if if_exists else ""
    owner_clause = f"authorization {owner}" if owner is not None else ""
    conn.execute(text(f"create schema {if_not_exists_clause} {name} {owner_clause}"))


def drop_schema(connection: Connection, schema_name: str) -> None:
    """
    Drop a schema and all its contents (if it exists).

    :param connection: An SQLAlchemy connection object.
    :param schema_name: The name of the schema to drop.
    """
    connection.execute(DropSchema(schema_name, cascade=True, if_exists=True))
