# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Connection, text


def escape_pg_identifier(conn: Connection, name: str):
    """
    Escape identifiers (tables, fields, roles, etc) for inclusion in SQL statements.

    psycopg2 can safely merge query arguments, but cannot do the same for dynamically
    generating queries.

    See http://initd.org/psycopg/docs/sql.html for more information.
    """
    # psycopg2 and psycopg3 both support this via the `quote_ident()` function.
    # We'll ask the server to escape instead to avoid conditional imports, as these are
    # not performance sensitive.
    return conn.execute(text(f"select quote_ident('{name}')")).scalar()


def get_connection_info(conn: Connection) -> tuple[str, str]:
    row = conn.execute(
        text("select quote_ident(current_database()), quote_ident(current_user)")
    ).fetchone()
    assert row is not None  # Reassure mypy that this cannot return None
    db, user = row
    return db, user


def ensure_extension(conn: Connection, extension_name: str = "POSTGIS") -> None:
    sql = text(f"create extension if not exists {extension_name}")
    conn.execute(sql)
