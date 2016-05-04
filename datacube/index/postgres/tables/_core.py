# coding=utf-8
"""
Core SQL schema settings.
"""
from __future__ import absolute_import

import logging

import sqlalchemy.sql.sqltypes
from sqlalchemy import MetaData, TIMESTAMP
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy.sql.functions import GenericFunction

USER_ROLES = ('agdc_user', 'agdc_ingest', 'agdc_manage', 'agdc_admin')

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


def ensure_db(engine, with_permissions=True):
    """
    Initialise the db if needed.
    """
    is_new = False
    c = engine.connect()

    db_name, db_user = _get_connection_info(c)

    if with_permissions:
        _LOG.info('Ensuring user roles.')
        _ensure_role(c, 'agdc_user')
        _ensure_role(c, 'agdc_ingest', inherits_from='agdc_user')
        _ensure_role(c, 'agdc_manage', inherits_from='agdc_ingest')
        _ensure_role(c, 'agdc_admin', inherits_from='agdc_manage', add_user=True)

        c.execute("""
        grant all on database {db} to agdc_admin;
        """.format(db=db_name))

    if not has_schema(engine, c):
        is_new = True
        try:
            c.execute('begin')
            if with_permissions:
                # Switch to 'agdc_admin', so that all items are owned by them.
                c.execute('set role agdc_admin')
            _LOG.info('Creating schema.')
            c.execute(CreateSchema(SCHEMA_NAME))
            _LOG.info('Creating tables.')
            c.execute(_FUNCTIONS)
            METADATA.create_all(c)
            c.execute('commit')
        except:
            c.execute('rollback')
            raise
        finally:
            if with_permissions:
                # psycopg doesn't have an equivalent to server-side quote_ident(). ?
                quoted_user = db_user.replace('"', '""')
                c.execute('set role "{}"'.format(quoted_user))

    if with_permissions:
        _LOG.info('Adding role grants.')
        c.execute("""
        grant usage on schema {schema} to agdc_user;
        grant select on all tables in schema {schema} to agdc_user;
        grant execute on function {schema}.common_timestamp(text) to agdc_user;

        grant insert on {schema}.dataset,
                        {schema}.dataset_location,
                        {schema}.dataset_source to agdc_ingest;
        grant usage, select on all sequences in schema {schema} to agdc_ingest;

        -- (We're only granting deletion of types that have nothing written yet: they can't delete the data itself)
        grant insert, delete on {schema}.dataset_type,
                                {schema}.metadata_type to agdc_manage;
        """.format(schema=SCHEMA_NAME))

    c.close()

    return is_new


def _pg_exists(conn, name):
    """
    Does a postgres object exist?
    :rtype bool
    """
    return conn.execute("SELECT to_regclass(%s)", name).scalar() is not None


def database_exists(engine):
    """
    Have they init'd this database?
    """
    return has_schema(engine, engine)


def schema_is_latest(engine):
    """
    Is the schema up-to-date?
    """
    # We may have versioned schema in the future.
    # For now, we know updates ahve been applied if the dataset_type table exists,
    return _pg_exists(engine, schema_qualified('dataset_type'))


def _ensure_role(engine, name, inherits_from=None, add_user=False, create_db=False):
    if has_role(engine, name):
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


def create_user(engine, username, key, role):
    if role not in USER_ROLES:
        raise ValueError('Unknown role %r. Expected one of %r' % (role, USER_ROLES))

    engine.execute(
        'create user {username} password %s in role {role}'.format(username=username, role=role),
        key
    )


def grant_role(engine, role, users):
    if role not in USER_ROLES:
        raise ValueError('Unknown role %r. Expected one of %r' % (role, USER_ROLES))

    engine.execute(
        'grant {role} to {users}'.format(users=', '.join(users), role=role)
    )


def has_role(engine, role_name):
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


class PGNAME(sqlalchemy.sql.sqltypes.Text):
    """Postgres 'NAME' type."""
    __visit_name__ = 'NAME'


@compiles(PGNAME)
def visit_name(element, compiler, **kw):
    return "NAME"
