# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Custom types for postgres & sqlalchemy
"""

from sqlalchemy import text
from sqlalchemy.dialects.postgresql.ranges import AbstractRange, Range
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.expression import ClauseElement, Executable
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import Double

SCHEMA_NAME = "odc"


class CreateView(Executable, ClauseElement):
    inherit_cache = True

    def __init__(self, name: str, select) -> None:
        self.name = name
        self.select = select


@compiles(CreateView)
def visit_create_view(element, compiler, **kw) -> str:
    return f"CREATE VIEW {element.name} AS {compiler.process(element.select, literal_binds=True)}"


UPDATE_TIMESTAMP_SQL: str = f"""
create or replace function {SCHEMA_NAME}.set_row_update_time()
returns trigger as $$
begin
  new.updated = now();
  return new;
end;
$$ language plpgsql;
"""
INSTALL_TRIGGER_SQL_TEMPLATE = [
    "drop trigger if exists row_update_time_{table} on {schema}.{table}",
    """
    create trigger row_update_time_{table}
    before update on {schema}.{table}
    for each row
    execute procedure {schema}.set_row_update_time();
    """,
]

TYPES_INIT_SQL: list[str] = [
    f"drop function if exists {SCHEMA_NAME}.common_timestamp(text)",
    f"""
    create type {SCHEMA_NAME}.float8range as range (
        subtype = float8,
        subtype_diff = float8mi
    )
    """,
]


# pylint: disable=abstract-method
class FLOAT8RANGE(AbstractRange[Range[Double]]):
    __visit_name__ = "FLOAT8RANGE"


@compiles(FLOAT8RANGE)
def visit_float8range(element, compiler, **kw) -> str:
    return "FLOAT8RANGE"


# pylint: disable=too-many-ancestors
class Float8Range(GenericFunction):
    type = FLOAT8RANGE  # type: ignore[assignment]
    package = "odc"
    identifier = "float8range"
    inherit_cache = False

    name = "float8range"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.packagenames = (f"{SCHEMA_NAME}",)


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


def pg_column_exists(conn, table, column) -> bool:
    """
    Does a postgres object exist?
    """
    return (
        conn.execute(
            text(f"""
                        SELECT 1 FROM pg_attribute
                        WHERE attrelid = to_regclass('{table}')
                        AND attname = '{column}'
                        AND NOT attisdropped
                        """)
        ).scalar()
        is not None
    )


def escape_pg_identifier(engine, name: str):
    """
    Escape identifiers (tables, fields, roles, etc) for inclusion in SQL statements.

    psycopg2 can safely merge query arguments, but cannot do the same for dynamically
    generating queries.

    See http://initd.org/psycopg/docs/sql.html for more information.
    """
    # New (2.7+) versions of psycopg2 have function: extensions.quote_ident()
    # But it's too bleeding edge right now. We'll ask the server to escape instead, as
    # these are not performance sensitive.
    return engine.execute(text(f"select quote_ident('{name}')")).scalar()
