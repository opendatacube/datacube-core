# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Custom types for postgres & sqlalchemy
"""

from __future__ import annotations

import warnings

from sqlalchemy import inspect, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.expression import ClauseElement, Executable

TYPE_CHECKING = False
if TYPE_CHECKING:
    from sqlalchemy import Connection

INSTALL_TRIGGER_SQL_TEMPLATE = [
    "drop trigger if exists row_update_time_{table} on {schema}.{table}",
    """
    create trigger row_update_time_{table}
    before update on {schema}.{table}
    for each row
    execute procedure {schema}.set_row_update_time();
    """,
]


class CreateView(Executable, ClauseElement):
    inherit_cache = True

    def __init__(self, name: str, select) -> None:
        self.name = name
        self.select = select


@compiles(CreateView)
def visit_create_view(element, compiler, **kw) -> str:
    return f"CREATE VIEW {element.name} AS {compiler.process(element.select, literal_binds=True)}"


class PGNAME(sqltypes.Text):
    """Postgres 'NAME' type."""

    __visit_name__ = "NAME"


@compiles(PGNAME)
def visit_name(element, compiler, **kw) -> str:
    return "NAME"


def pg_exists(conn, name: str) -> bool:
    """
    Does a postgres object exist?
    """
    return conn.execute(text(f"SELECT to_regclass('{name}')")).scalar() is not None


def pg_column_exists(
    conn: Connection, table: str, column: str, schema: str | None, odc_schema: str
) -> bool:
    """
    Does a table column exist?
    """
    if table.startswith((f"{odc_schema}.", f"'{odc_schema}.", f'"{odc_schema}.')):
        warnings.warn(
            f"Call pg_column_exists with a table name without {odc_schema}.",
            stacklevel=2,
        )
        table = table.replace(f"{odc_schema}.", "")
    return column in [x.get("name") for x in inspect(conn).get_columns(table, schema)]
