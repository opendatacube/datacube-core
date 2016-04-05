# coding=utf-8
"""
Core SQL schema settings.
"""
from __future__ import absolute_import

import logging

from sqlalchemy import MetaData, TIMESTAMP
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy.sql.functions import GenericFunction

SQL_NAMING_CONVENTIONS = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
SCHEMA_NAME = 'agdc'
METADATA = MetaData(naming_convention=SQL_NAMING_CONVENTIONS, schema=SCHEMA_NAME)

_LOG = logging.getLogger(__name__)


def schema_qualified(name):
    """
    >>> schema_qualified('dataset')
    'agdc.dataset'
    """
    return '{}.{}'.format(SCHEMA_NAME, name)


def _get_connection_info(connection):
    db, user = connection.execute("select current_database(), current_user").fetchall()[0]
    return db, user


def ensure_db(engine):
    """
    Initialise the db if needed.
    """
    is_new = False
    c = engine.connect()

    db_name, db_user = _get_connection_info(c)

    _ensure_role(c, 'agdc_user')
    _ensure_role(c, 'agdc_ingest', inherits_from='agdc_user')
    _ensure_role(c, 'agdc_management', inherits_from='agdc_ingest')
    _ensure_role(c, 'agdc_admin', inherits_from='agdc_management', add_user=True)

    c.execute("""
    grant all on database {db} to agdc_admin;
    """.format(db=db_name))

    if not has_schema(engine, c):
        is_new = True
        try:
            # Switch to 'agdc_admin', so that all items are owned by them.
            c.execute('set role agdc_admin')
            c.execute(CreateSchema(SCHEMA_NAME))
            c.execute(_FUNCTIONS)
            METADATA.create_all(c)
        finally:
            c.execute('set role ' + db_user)

    c.execute("""
    grant usage on schema {schema} to agdc_user;
    grant select on all tables in schema {schema} to agdc_user;
    grant execute on function {schema}.common_timestamp(text) to agdc_user;

    grant insert on {schema}.dataset,
                    {schema}.dataset_location,
                    {schema}.dataset_source,
                    {schema}.dataset_storage,
                    {schema}.storage_unit to agdc_ingest;

    grant insert on {schema}.storage_type,
                    {schema}.collection,
                    {schema}.metadata_type to agdc_management
    """.format(schema=SCHEMA_NAME))

    c.close()

    return is_new


def _ensure_role(engine, name, inherits_from=None, add_user=False, create_db=False):
    if _has_role(engine, name):
        _LOG.debug('Role exists: %s', name)
        return

    sql = [
        'create role %s nologin inherit' % name,
        'createuser' if add_user else 'nocreateuser',
        'createdb' if create_db else 'nocreatedb'
    ]
    if inherits_from:
        sql.append('in role ' + inherits_from)
    engine.execute(' '.join(sql))


def _has_role(engine, role_name):
    return bool(engine.execute('select rolname from pg_roles where rolname=%s', role_name).fetchall())


def has_schema(engine, connection):
    return engine.dialect.has_schema(connection, SCHEMA_NAME)


def drop_db(connection):
    connection.execute('drop schema if exists %s cascade;' % SCHEMA_NAME)


class View(Executable, ClauseElement):
    def __init__(self, name, select):
        self.name = name
        self.select = select


@compiles(View)
def visit_create_view(element, compiler, **kw):
    return "CREATE VIEW %s AS %s" % (
        element.name,
        compiler.process(element.select, literal_binds=True)
    )


_FUNCTIONS = """
create or replace function {schema}.common_timestamp(text)
returns timestamp with time zone as $$
select ($1)::timestamp at time zone 'utc';
$$ language sql immutable returns null on null input;
""".format(schema=SCHEMA_NAME)


# Register the function with SQLAlchemhy.
# pylint: disable=too-many-ancestors
class CommonTimestamp(GenericFunction):
    type = TIMESTAMP(timezone=True)
    package = 'agdc'
    identifier = 'common_timestamp'

    name = '%s.common_timestamp' % SCHEMA_NAME
