# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Custom types for postgres & sqlalchemy
"""

from __future__ import annotations

from sqlalchemy.dialects.postgresql.ranges import AbstractRange, Range
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import Double

import datacube.drivers.common_psql.sql as common_psql

TYPE_CHECKING = False
if TYPE_CHECKING:
    from sqlalchemy import Connection


SCHEMA_NAME = "odc"


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
