# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Core SQL schema settings.
"""

import logging

from datacube.drivers.postgres.sql import (INSTALL_TRIGGER_SQL_TEMPLATE,
                                           SCHEMA_NAME, TYPES_INIT_SQL,
                                           UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE,
                                           ADDED_COLUMN_MIGRATE_SQL_TEMPLATE,
                                           UPDATE_TIMESTAMP_SQL,
                                           escape_pg_identifier,
                                           pg_column_exists)
from sqlalchemy import MetaData, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateSchema, DropSchema


USER_ROLES = ('agdc_user', 'agdc_ingest', 'agdc_manage', 'agdc_admin')

SQL_NAMING_CONVENTIONS = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
    # Other prefixes handled outside of sqlalchemy:
    # dix: dynamic-index, those indexes created automatically based on search field configuration.
    # tix: test-index, created by hand for testing, particularly in dev.
}

METADATA = MetaData(naming_convention=SQL_NAMING_CONVENTIONS, schema=SCHEMA_NAME)

_LOG = logging.getLogger(__name__)


def install_timestamp_trigger(connection):
    from . import _schema
    TABLE_NAMES = [  # noqa: N806
        _schema.METADATA_TYPE.name,
        _schema.PRODUCT.name,
        _schema.DATASET.name,
    ]
    # Create trigger capture function
    connection.execute(text(UPDATE_TIMESTAMP_SQL))

    for name in TABLE_NAMES:
        # Add update columns
        connection.execute(text(UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE.format(schema=SCHEMA_NAME, table=name)))
        connection.execute(text(INSTALL_TRIGGER_SQL_TEMPLATE.format(schema=SCHEMA_NAME, table=name)))


def install_added_column(connection):
    from . import _schema
    TABLE_NAME = _schema.DATASET_LOCATION.name  # noqa: N806
    connection.execute(text(ADDED_COLUMN_MIGRATE_SQL_TEMPLATE.format(schema=SCHEMA_NAME, table=TABLE_NAME)))


def schema_qualified(name):
    """
    >>> schema_qualified('dataset')
    'agdc.dataset'
    """
    return '{}.{}'.format(SCHEMA_NAME, name)


def _get_quoted_connection_info(connection):
    db, user = connection.execute(text("select quote_ident(current_database()), quote_ident(current_user)")).fetchone()
    return db, user


def ensure_db(engine, with_permissions=True):
    """
    Initialise the db if needed.

    Ensures standard users exist.

    Create the schema if it doesn't exist.
    """
    is_new = not has_schema(engine)
    with engine.connect() as c:
        #  NB. Using default SQLA2.0 auto-begin commit-as-you-go behaviour
        quoted_db_name, quoted_user = _get_quoted_connection_info(c)

        if with_permissions:
            _LOG.info('Ensuring user roles.')
            _ensure_role(c, 'agdc_user')
            _ensure_role(c, 'agdc_ingest', inherits_from='agdc_user')
            _ensure_role(c, 'agdc_manage', inherits_from='agdc_ingest')
            _ensure_role(c, 'agdc_admin', inherits_from='agdc_manage', add_user=True)

            c.execute(text("""
            grant all on database {db} to agdc_admin;
            """.format(db=quoted_db_name)))
            c.commit()

        if is_new:
            if with_permissions:
                # Switch to 'agdc_admin', so that all items are owned by them.
                c.execute(text('set role agdc_admin'))
            _LOG.info('Creating schema.')
            c.execute(CreateSchema(SCHEMA_NAME))
            _LOG.info('Creating types.')
            c.execute(text(TYPES_INIT_SQL))
            _LOG.info('Creating tables.')
            METADATA.create_all(c)
            _LOG.info("Creating triggers.")
            install_timestamp_trigger(c)
            _LOG.info("Creating added column.")
            install_added_column(c)
            if with_permissions:
                c.execute(text('set role {}'.format(quoted_user)))
            c.commit()

        if with_permissions:
            _LOG.info('Adding role grants.')
            c.execute(text("""
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
            -- Allow creation of indexes, views
            grant create on schema {schema} to agdc_manage;
            """.format(schema=SCHEMA_NAME)))
            c.commit()

    return is_new


def database_exists(engine):
    """
    Have they init'd this database?
    """
    return has_schema(engine)


def schema_is_latest(engine: Engine) -> bool:
    """
    Is the current schema up-to-date?

    This is run when a new connection is established to see if it's compatible.

    It should be runnable by unprivileged users. If it returns false, their
    connection will be rejected and they will be told to get an administrator
    to apply updates.

    See the ``update_schema()`` function below for actually applying the updates.
    """
    # In lieu of a versioned schema, we typically check by seeing if one of the objects
    # from the change exists.
    #
    # Eg.
    #     return pg_column_exists(engine, schema_qualified('dataset_location'), 'archived')
    #
    # ie. Does the 'archived' column exist? If so, we know the related schema was applied.

    # No schema changes recently. Everything is perfect.
    return True


def update_schema(engine: Engine):
    """
    Check and apply any missing schema changes to the database.

    This is run by an administrator.

    See the `schema_is_latest()` function above: this should apply updates
    that it requires.
    """
    # This will typically check if something exists (like a newly added column), and
    # run the SQL of the change inside a single transaction.

    # Empty, as no schema changes have been made recently.
    # -> If you need to write one, look at the Git history of this
    #    function for some examples.

    # Post 1.8 DB Incremental Sync triggers
    with engine.connect() as connection:
        if not pg_column_exists(connection, schema_qualified('dataset'), 'updated'):
            _LOG.info("Adding 'updated'/'added' fields and triggers to schema.")
            connection.execute(text('begin'))
            install_timestamp_trigger(connection)
            install_added_column(connection)
            connection.execute(text('commit'))
        else:
            _LOG.info("No schema updates required.")


def _ensure_role(conn, name, inherits_from=None, add_user=False, create_db=False):
    if has_role(conn, name):
        _LOG.debug('Role exists: %s', name)
        return

    sql = [
        'create role %s nologin inherit' % name,
        'createrole' if add_user else 'nocreaterole',
        'createdb' if create_db else 'nocreatedb'
    ]
    if inherits_from:
        sql.append('in role ' + inherits_from)
    conn.execute(text(' '.join(sql)))


def grant_role(conn, role, users):
    if role not in USER_ROLES:
        raise ValueError('Unknown role %r. Expected one of %r' % (role, USER_ROLES))

    users = [escape_pg_identifier(conn, user) for user in users]
    conn.execute(text('revoke {roles} from {users}'.format(users=', '.join(users), roles=', '.join(USER_ROLES))))
    conn.execute(text('grant {role} to {users}'.format(users=', '.join(users), role=role)))


def has_role(conn, role_name):
    res = conn.execute(text(f"SELECT rolname FROM pg_roles WHERE rolname='{role_name}'")).fetchall()
    return bool(res)


def has_schema(engine):
    inspector = inspect(engine)
    return SCHEMA_NAME in inspector.get_schema_names()


def drop_db(connection):
    # if_exists parameter seems to not be working in SQLA1.4?
    if has_schema(connection.engine):
        connection.execute(DropSchema(SCHEMA_NAME, cascade=True, if_exists=True))


def to_pg_role(role):
    """
    Convert a role name to a name for use in PostgreSQL

    There is a short list of valid ODC role names, and they are given
    a prefix inside of PostgreSQL.

    Why are we even doing this? Can't we use the same names internally and externally?

    >>> to_pg_role('ingest')
    'agdc_ingest'
    >>> to_pg_role('fake')
    Traceback (most recent call last):
    ...
    ValueError: Unknown role 'fake'. Expected one of ...
    """
    pg_role = 'agdc_' + role.lower()
    if pg_role not in USER_ROLES:
        raise ValueError(
            'Unknown role %r. Expected one of %r' %
            (role, [r.split('_')[1] for r in USER_ROLES])
        )
    return pg_role


def from_pg_role(pg_role):
    """
    Convert a PostgreSQL role name back to an ODC name.

    >>> from_pg_role('agdc_admin')
    'admin'
    >>> from_pg_role('fake')
    Traceback (most recent call last):
    ...
    ValueError: Not a pg role: 'fake'. Expected one of ...
    """
    if pg_role not in USER_ROLES:
        raise ValueError('Not a pg role: %r. Expected one of %r' % (pg_role, USER_ROLES))

    return pg_role.split('_')[1]
