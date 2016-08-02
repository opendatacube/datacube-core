# coding=utf-8
"""
Custom types for postgres & sqlalchemy
"""
from __future__ import absolute_import

from sqlalchemy import TIMESTAMP
from sqlalchemy.dialects.postgresql.ranges import RangeOperators
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy.sql.functions import GenericFunction

SCHEMA_NAME = 'agdc'


class CreateView(Executable, ClauseElement):
    def __init__(self, name, select):
        self.name = name
        self.select = select


@compiles(CreateView)
def visit_create_view(element, compiler, **kw):
    return "CREATE VIEW %s AS %s" % (
        element.name,
        compiler.process(element.select, literal_binds=True)
    )


TYPES_INIT_SQL = """
create or replace function {schema}.common_timestamp(text)
returns timestamp with time zone as $$
select ($1)::timestamp at time zone 'utc';
$$ language sql immutable returns null on null input;

create type {schema}.float8range as range (
    subtype = float8,
    subtype_diff = float8mi
);
""".format(schema=SCHEMA_NAME)


# pylint: disable=abstract-method
class FLOAT8RANGE(RangeOperators, sqltypes.TypeEngine):
    __visit_name__ = 'FLOAT8RANGE'


@compiles(FLOAT8RANGE)
def visit_float8range(element, compiler, **kw):
    return "FLOAT8RANGE"


# Register the function with SQLAlchemhy.
# pylint: disable=too-many-ancestors
class CommonTimestamp(GenericFunction):
    type = TIMESTAMP(timezone=True)
    package = 'agdc'
    identifier = 'common_timestamp'

    name = '%s.common_timestamp' % SCHEMA_NAME


# pylint: disable=too-many-ancestors
class Float8Range(GenericFunction):
    type = FLOAT8RANGE
    package = 'agdc'
    identifier = 'float8range'

    name = '%s.float8range' % SCHEMA_NAME


class PGNAME(sqltypes.Text):
    """Postgres 'NAME' type."""
    __visit_name__ = 'NAME'


@compiles(PGNAME)
def visit_name(element, compiler, **kw):
    return "NAME"
