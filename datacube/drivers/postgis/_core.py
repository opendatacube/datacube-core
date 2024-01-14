# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Core SQL schema settings.
"""

import logging
import os

from sqlalchemy import MetaData, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.ddl import DropSchema
from alembic import command, config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from alembic.runtime.environment import EnvironmentContext

from datacube.drivers.postgis.sql import (INSTALL_TRIGGER_SQL_TEMPLATE,
                                          SCHEMA_NAME, TYPES_INIT_SQL,
                                          UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE,
                                          UPDATE_TIMESTAMP_SQL,
                                          escape_pg_identifier)

USER_ROLES = ('odc_user', 'odc_ingest', 'odc_manage', 'odc_admin')

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

SRC_CODE_ROOT = os.path.dirname(  # Source code root
    os.path.dirname(              # datacube
        os.path.dirname(          # drivers
            os.path.dirname(      # postgis
                __file__          # This file
            )
        )
    )
)

ALEMBIC_INI_LOCATION = os.path.join(SRC_CODE_ROOT, "alembic.ini")

METADATA = MetaData(naming_convention=SQL_NAMING_CONVENTIONS, schema=SCHEMA_NAME)

_LOG = logging.getLogger(__name__)


def install_timestamp_trigger(connection):
    from . import _schema
    TABLE_NAMES = [  # noqa: N806
        _schema.MetadataType.__tablename__,
        _schema.Product.__tablename__,
        _schema.Dataset.__tablename__,
    ]
    # Create trigger capture function
    connection.execute(text(UPDATE_TIMESTAMP_SQL))

    for name in TABLE_NAMES:
        # Add update columns
        connection.execute(text(UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE.format(schema=SCHEMA_NAME, table=name)))
        connection.execute(text(INSTALL_TRIGGER_SQL_TEMPLATE.format(schema=SCHEMA_NAME, table=name)))


def schema_qualified(name):
    """
    >>> schema_qualified('dataset')
    'odc.dataset'
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

        _ensure_extension(c, 'POSTGIS')
        c.commit()

        if with_permissions:
            _LOG.info('Ensuring user roles.')
            _ensure_role(c, 'odc_user')
            _ensure_role(c, 'odc_ingest', inherits_from='odc_user')
            _ensure_role(c, 'odc_manage', inherits_from='odc_ingest')
            _ensure_role(c, 'odc_admin', inherits_from='odc_manage', add_user=True)

            c.execute(text(f"""
            grant all on database {quoted_db_name} to odc_admin;
            """))
            c.commit()

        if is_new:
            sqla_txn = c.begin()
            if with_permissions:
                # Switch to 'odc_admin', so that all items are owned by them.
                c.execute(text('set role odc_admin'))
            _LOG.info('Creating schema.')
            c.execute(CreateSchema(SCHEMA_NAME))
            _LOG.info('Creating types.')
            c.execute(text(TYPES_INIT_SQL))
            from ._schema import orm_registry, ALL_STATIC_TABLES
            _LOG.info('Creating tables.')
            _LOG.info("Dataset indexes: %s", repr(orm_registry.metadata.tables["odc.dataset"].indexes))
            orm_registry.metadata.create_all(c, tables=ALL_STATIC_TABLES)
            _LOG.info("Creating triggers.")
            install_timestamp_trigger(c)
            sqla_txn.commit()
            if with_permissions:
                c.execute(text(f'set role {quoted_user}'))
            c.commit()
            # Stamp with latest Alembic revision
            alembic_cfg = config.Config(ALEMBIC_INI_LOCATION)
            alembic_cfg.attributes["connection"] = c
            command.stamp(alembic_cfg, "head")

        if with_permissions:
            _LOG.info('Adding role grants.')
            c.execute(text(f"""
            grant usage on schema {SCHEMA_NAME} to odc_user;
            grant select on all tables in schema {SCHEMA_NAME} to odc_user;
            grant execute on function {SCHEMA_NAME}.common_timestamp(text) to odc_user;

            grant insert on {SCHEMA_NAME}.dataset,
                            {SCHEMA_NAME}.location,
                            {SCHEMA_NAME}.dataset_lineage to odc_ingest;
            grant usage, select on all sequences in schema {SCHEMA_NAME} to odc_ingest;

            -- (We're only granting deletion of types that have nothing written yet: they can't delete the data itself)
            grant insert, delete on {SCHEMA_NAME}.product,
                                    {SCHEMA_NAME}.metadata_type to odc_manage;
            -- Allow creation of indexes, views
            grant create on schema {SCHEMA_NAME} to odc_manage;
            """))
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

    # No schema changes recently. Everything is perfect.

    cfg = config.Config(ALEMBIC_INI_LOCATION)
    scriptdir = ScriptDirectory.from_config(cfg)
    # NB this assumes a single unbranched migration branch
    # Get Head revision from Alembic environment
    with EnvironmentContext(cfg, scriptdir) as env_ctx:
        latest_rev = env_ctx.get_head_revision()
        # Get current revision from database
        with engine.connect() as conn:
            context = MigrationContext.configure(
                connection=conn,
                environment_context=env_ctx,
                opts={"version_table_schema": "odc"}
            )
            current_rev = context.get_current_revision()

    # Do they match?
    if latest_rev == current_rev:
        return True
    import warnings
    warnings.warn(f"Current Alembic schema revision is {current_rev} expected {latest_rev}")
    return False


def update_schema(engine: Engine):
    """
    Check and apply any missing schema changes to the database.

    This is run by an administrator.

    See the `schema_is_latest()` function above: this should apply updates
    that it requires.
    """
    cfg = config.Config(ALEMBIC_INI_LOCATION)
    with engine.begin() as conn:
        cfg.attributes["connection"] = conn
        print("Running upgrade")
        command.upgrade(cfg, "head")


def _ensure_extension(conn, extension_name="POSTGIS"):
    sql = text(f'create extension if not exists {extension_name}')
    conn.execute(sql)


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
    return bool(
        conn.execute(text(f"SELECT rolname FROM pg_roles WHERE rolname='{role_name}'")).fetchall()
    )


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
    'odc_ingest'
    >>> to_pg_role('fake')
    Traceback (most recent call last):
    ...
    ValueError: Unknown role 'fake'. Expected one of ...
    """
    pg_role = 'odc_' + role.lower()
    if pg_role not in USER_ROLES:
        raise ValueError(
            'Unknown role %r. Expected one of %r' %
            (role, [r.split('_')[1] for r in USER_ROLES])
        )
    return pg_role


def from_pg_role(pg_role):
    """
    Convert a PostgreSQL role name back to an ODC name.

    >>> from_pg_role('odc_admin')
    'admin'
    >>> from_pg_role('fake')
    Traceback (most recent call last):
    ...
    ValueError: Not a pg role: 'fake'. Expected one of ...
    """
    if pg_role not in USER_ROLES:
        raise ValueError('Not a pg role: %r. Expected one of %r' % (pg_role, USER_ROLES))

    return pg_role.split('_')[1]
