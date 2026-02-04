# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import contextlib
from collections.abc import Generator

from sqlalchemy import Connection, text


def escape_pg_identifier(conn: Connection, name: str) -> str:
    """
    Escape identifiers for inclusion in SQL statements.

    In ODC only used for user names.

    :param conn: A SQLAlchemy connection object.
    :param name: The unescaped identifier/username
    :return: The escaped identifier/username
    """
    # psycopg2 and psycopg3 both support this via the `quote_ident()` function.
    # We'll ask the server to escape instead to avoid conditional imports, as these are
    # not performance-sensitive.
    return str(conn.execute(text(f"select quote_ident('{name}')")).scalar())


def get_connection_info(conn: Connection) -> tuple[str, str]:
    """
    Return the database name and currently active role of an SQLAlchemy connection.
    :param conn: The SQLAlchemy connection object.
    :return: a string tuple (database name, role name)
    """
    row = conn.execute(
        text("select quote_ident(current_database()), quote_ident(current_user)")
    ).fetchone()
    assert row is not None  # Reassure mypy that this cannot return None
    return tuple(row)


def ensure_extension(conn: Connection, extension_name: str) -> None:
    """
    Ensure the database has the given extension installed (e.g. the PostGIS extension).

    :param conn: An SQLAlchemy connection object.
    :param extension_name: The extension name, e.g. "postgis"
    """
    sql = text(f"create extension if not exists {extension_name}")
    conn.execute(sql)


@contextlib.contextmanager
def as_role(conn: Connection, role: str | None) -> Generator[Connection]:
    """
    Context manager to temporarily switch database roles.

    E.g.::
        with engine.connect() as conn:
            # This is executed as the session role configured into the engine
            conn.execute(query1)
            with as_role(conn, "odc_admin") as conn:
                # This is executed as the "odc_admin" role
                conn.execute(query2)
            # This is executed as the session role configured into the engine
            conn.execute(query3)

    :param conn: The base SQLAlchemy connection object.
    :param role: The role to switch to, or None to continue using the currently active
        role
    :return: The SQLAlchemy connection object, switched to the specified role.
    """
    if role is None:
        yield conn
    else:
        _, db_user = get_connection_info(conn)
        try:
            conn.execute(text(f"set role {role}"))
            yield conn
        finally:
            conn.execute(text(f"set role {db_user}"))
