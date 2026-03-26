# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Custom types for postgres & sqlalchemy
"""

from sqlalchemy import TIMESTAMP, Connection
from sqlalchemy.dialects.postgresql.ranges import AbstractRange, Range
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import Double

import datacube.drivers.common_psql.sql as common_psql

SCHEMA_NAME = "agdc"


# Make old names available from this module.
from datacube.drivers.common_psql.sql import (  # noqa: E402, F401
    INSTALL_TRIGGER_SQL_TEMPLATE,
    CreateView,
    visit_create_view,
)

UPDATE_TIMESTAMP_SQL: str = f"""
create or replace function {SCHEMA_NAME}.set_row_update_time()
returns trigger as $$
begin
  new.updated = now();
  return new;
end;
$$ language plpgsql;
"""

UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE = """
alter table {schema}.{table} add column if not exists updated
timestamptz default now();
"""

UPDATE_COLUMN_INDEX_SQL_TEMPLATE = """
create index if not exists ix_{table}_updated
on {schema}.{table}(updated);
"""

ADDED_COLUMN_MIGRATE_SQL_TEMPLATE = """
alter table {schema}.{table} add column if not exists added
timestamptz default now();
"""

ADDED_COLUMN_INDEX_SQL_TEMPLATE = """
create index if not exists ix_{table}_added
on {schema}.{table}(added);
"""

TYPES_INIT_SQL: list[str] = [
    f"""
    create or replace function {SCHEMA_NAME}.common_timestamp(text)
    returns timestamp with time zone as $$
    select ($1)::timestamp at time zone 'utc';
    $$ language sql immutable returns null on null input;
    """,
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


# Register the function with SQLAlchemy.
# pylint: disable=too-many-ancestors
class CommonTimestamp(GenericFunction):
    type = TIMESTAMP(timezone=True)
    package = "agdc"
    identifier = "common_timestamp"
    inherit_cache = False

    name = "common_timestamp"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.packagenames = (f"{SCHEMA_NAME}",)


# pylint: disable=too-many-ancestors
class Float8Range(GenericFunction):
    type = FLOAT8RANGE  # type: ignore[assignment]
    package = "agdc"
    identifier = "float8range"
    inherit_cache = False

    name = "float8range"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.packagenames = (f"{SCHEMA_NAME}",)


# Make old names available from this module.
from datacube.drivers.common_psql.sql import (  # noqa: E402, F401
    PGNAME,
    pg_exists,
    visit_name,
)


def pg_column_exists(
    conn: Connection, table: str, column: str, schema: str | None = SCHEMA_NAME
) -> bool:
    """
    Does a table column exist?
    """
    return common_psql.pg_column_exists(conn, table, column, schema, SCHEMA_NAME)
