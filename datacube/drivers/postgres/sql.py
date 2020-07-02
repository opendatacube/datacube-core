# coding=utf-8
"""
Custom types for postgres & sqlalchemy
"""

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

    name = 'common_timestamp'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.packagenames = ['%s' % SCHEMA_NAME]


# pylint: disable=too-many-ancestors
class Float8Range(GenericFunction):
    type = FLOAT8RANGE
    package = 'agdc'
    identifier = 'float8range'

    name = 'float8range'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.packagenames = ['%s' % SCHEMA_NAME]


class PGNAME(sqltypes.Text):
    """Postgres 'NAME' type."""
    __visit_name__ = 'NAME'


@compiles(PGNAME)
def visit_name(element, compiler, **kw):
    return "NAME"


def pg_exists(conn, name):
    """
    Does a postgres object exist?
    :rtype bool
    """
    return conn.execute("SELECT to_regclass(%s)", name).scalar() is not None


def pg_column_exists(conn, table, column):
    """
    Does a postgres object exist?
    :rtype bool
    """
    return conn.execute("""
                        SELECT 1 FROM pg_attribute
                        WHERE attrelid = to_regclass(%s)
                        AND attname = %s
                        AND NOT attisdropped
                        """, table, column).scalar() is not None


def escape_pg_identifier(engine, name):
    """
    Escape identifiers (tables, fields, roles, etc) for inclusion in SQL statements.

    psycopg2 can safely merge query arguments, but cannot do the same for dynamically
    generating queries.

    See http://initd.org/psycopg/docs/sql.html for more information.
    """
    # New (2.7+) versions of psycopg2 have function: extensions.quote_ident()
    # But it's too bleeding edge right now. We'll ask the server to escape instead, as
    # these are not performance sensitive.
    return engine.execute("select quote_ident(%s)", name).scalar()
