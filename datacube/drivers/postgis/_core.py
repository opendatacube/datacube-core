# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Core SQL schema settings.
"""

import contextlib
import logging
import os
from collections.abc import Generator, Iterable
from enum import Enum
from typing import Literal, Union

from alembic import command, config
from alembic.migration import MigrationContext
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from deprecat import deprecat
from sqlalchemy import Connection, MetaData, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.ddl import DropSchema

from datacube.drivers.postgis.sql import (
    INSTALL_TRIGGER_SQL_TEMPLATE,
    SCHEMA_NAME,
    TYPES_INIT_SQL,
    UPDATE_TIMESTAMP_SQL,
    escape_pg_identifier,
)
from datacube.migration import ODC2DeprecationWarning


class UserRole(Enum):
    USER = "odc_user"
    MANAGE = "odc_manage"
    ADMIN = "odc_admin"

    @classmethod
    def to_pg_role(cls, role_str: Literal["user", "manage", "admin"]) -> "UserRole":
        return cls("odc_" + role_str.lower())

    def simple_str(self) -> str:
        return self.value.split("_", 1)[1]

    @classmethod
    def all_roles(cls) -> Generator[str]:
        for role in cls:
            yield role.simple_str()

    def higher_roles(self) -> list["UserRole"]:
        if self == UserRole.USER:
            return [UserRole.MANAGE, UserRole.ADMIN]
        if self == UserRole.MANAGE:
            return [UserRole.ADMIN]
        return []

    def inherits_from(self) -> Union["UserRole", None]:
        if self == UserRole.ADMIN:
            return UserRole.MANAGE
        if self == UserRole.MANAGE:
            return UserRole.USER
        return None

    def can_create_user(self) -> bool:
        return self == UserRole.ADMIN


SQL_NAMING_CONVENTIONS = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
    # Other prefixes handled outside of sqlalchemy:
    # dix: dynamic-index, those indexes created automatically based on search field configuration.
    # tix: test-index, created by hand for testing, particularly in dev.
}

POSTGIS_DRIVER_DIR: str = os.path.dirname(__file__)

ALEMBIC_INI_LOCATION: str = os.path.join(POSTGIS_DRIVER_DIR, "alembic.ini")

METADATA = MetaData(naming_convention=SQL_NAMING_CONVENTIONS, schema=SCHEMA_NAME)

_LOG: logging.Logger = logging.getLogger(__name__)


def install_timestamp_trigger(connection) -> None:
    from . import _schema

    TABLE_NAMES = [  # noqa: N806
        _schema.MetadataType.__tablename__,
        _schema.Product.__tablename__,
        _schema.Dataset.__tablename__,
    ]
    # Create trigger capture function
    connection.execute(text(UPDATE_TIMESTAMP_SQL))

    for name in TABLE_NAMES:
        for s in INSTALL_TRIGGER_SQL_TEMPLATE:
            connection.execute(text(s.format(schema=SCHEMA_NAME, table=name)))


def schema_qualified(name: str) -> str:
    """
    >>> schema_qualified("dataset")
    'odc.dataset'
    """
    return f"{SCHEMA_NAME}.{name}"


def get_connection_info(connection: Connection) -> tuple[str, str]:
    """
    Obtain information about an open database connection
    :param connection: An SQLAlchemy connection
    :return: A tuple consisting of the database name and the user name of the connection
    """
    row = connection.execute(
        text("select quote_ident(current_database()), quote_ident(current_user)")
    ).fetchone()
    # Mypy doesn't understand that the above SQL always returns a row.
    assert row is not None
    db, user = row
    return db, user


def ensure_db(engine: Engine, with_permissions: bool = True) -> bool:
    """
    Initialise the db if needed.

    Ensures standard users exist.

    Create the schema if it doesn't exist.
    """
    is_new = not has_schema(engine)
    with engine.connect() as c:
        #  NB. Using default SQLA2.0 auto-begin commit-as-you-go behaviour
        quoted_db_name, quoted_user = get_connection_info(c)

        _ensure_extension(c, "POSTGIS")
        c.commit()

        if with_permissions:
            _LOG.info("Ensuring user roles.")
            for role in UserRole:
                _ensure_role(c, role)

            c.execute(
                text(f"""
            grant all on database {quoted_db_name} to odc_admin;
            """)
            )
            c.commit()

        if is_new:
            # If NOT new, it is up to the caller to update with alembic
            sqla_txn = c.begin()
            if with_permissions:
                # Switch to 'odc_admin', so that all items are owned by them.
                c.execute(text("set role odc_admin"))
            _LOG.info("Creating schema.")
            c.execute(CreateSchema(SCHEMA_NAME))
            _LOG.info("Creating types.")
            for s in TYPES_INIT_SQL:
                c.execute(text(s))
            from ._schema import orm_registry

            _LOG.info("Creating tables.")
            _LOG.info(
                "Dataset indexes: %s",
                repr(orm_registry.metadata.tables["odc.dataset"].indexes),
            )
            orm_registry.metadata.create_all(c)
            _LOG.info("Creating triggers.")
            install_timestamp_trigger(c)
            sqla_txn.commit()
            c.commit()
            # Stamp with latest Alembic revision
            alembic_cfg = config.Config(ALEMBIC_INI_LOCATION)
            alembic_cfg.attributes["connection"] = c
            command.stamp(alembic_cfg, "head")
            if with_permissions:
                c.execute(text(f"set role {quoted_user}"))

        if with_permissions:
            _LOG.info("Adding role grants.")
            c.execute(text(f"grant usage on schema {SCHEMA_NAME} to odc_user"))
            c.execute(
                text(f"grant select on all tables in schema {SCHEMA_NAME} to odc_user")
            )
            c.execute(text("grant odc_user to odc_manage"))
            c.execute(
                text(
                    f"grant insert on {SCHEMA_NAME}.dataset,"
                    f"{SCHEMA_NAME}.dataset_lineage to odc_manage"
                )
            )
            c.execute(
                text(
                    f"grant usage, select on all sequences in schema {SCHEMA_NAME} to odc_manage"
                )
            )

            # Manage allows deletion of types that have nothing written yet (admin needed to delete the data itself)
            c.execute(
                text(
                    f"grant insert, delete on {SCHEMA_NAME}.product,"
                    f"{SCHEMA_NAME}.metadata_type to odc_manage"
                )
            )
            # Allow creation of indexes, views
            c.execute(text(f"grant create on schema {SCHEMA_NAME} to odc_manage"))
            # Belt and braces to cover corner cases
            c.execute(text("grant odc_manage to odc_admin"))
            c.commit()

    return is_new


def database_exists(engine: Engine) -> bool:
    """
    Have they init'd this database?
    """
    return has_schema(engine)


# MIGRATIONS that are mutually compatible.
# This should become an empty set when the latest migration is not compatible with the previous
COMPATIBLE_MIGRATIONS: set[str] = {"01fa1abedd6d", "d27eed82e1f6"}


def _current_and_latest(engine: Engine) -> tuple[str, str]:
    """
    Return latest schema migration and current migration for engine.
    :param engine: A SQLAlchemy engine
    :return: latest revision, current revision
    """
    cfg = config.Config(ALEMBIC_INI_LOCATION)
    scriptdir = ScriptDirectory.from_config(cfg)
    # NB this assumes a single unbranched migration branch
    # Get Head revision from Alembic environment
    with EnvironmentContext(cfg, scriptdir) as env_ctx:
        latest_rev = env_ctx.get_head_revision()
        assert isinstance(latest_rev, str)
        # Get current revision from database
        with engine.connect() as conn:
            context = MigrationContext.configure(
                connection=conn,
                environment_context=env_ctx,
                opts={"version_table_schema": "odc"},
            )
            current_rev = context.get_current_revision()
            assert isinstance(current_rev, str)
    return latest_rev, current_rev


def schema_is_latest(engine: Engine, compatible=False) -> bool:
    """
    Is the current schema up-to-date?

    This is run when a new connection is established to see if it's compatible.

    It should be runnable by unprivileged users. If it returns false, their
    connection will be rejected and they will be told to get an administrator
    to apply updates.

    See the ``update_schema()`` function below for actually applying the updates.
    :arg compatible: If True, return True if the codebase is compatible with the latest revision.
    """
    latest_rev, current_rev = _current_and_latest(engine)
    # Do they match exactly?
    if latest_rev == current_rev:
        return True

    # Don't match, check for compatibility.
    is_compatible = (
        current_rev in COMPATIBLE_MIGRATIONS and latest_rev in COMPATIBLE_MIGRATIONS
    )

    import warnings

    warnings.warn(
        f"Current Alembic schema revision is {current_rev} {'recommend' if compatible else 'expecting'} {latest_rev}",
        stacklevel=2,
    )
    return is_compatible if compatible else False


def update_schema(engine: Engine) -> None:
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


def _ensure_extension(conn: Connection, extension_name: str = "POSTGIS") -> None:
    sql = text(f"create extension if not exists {extension_name}")
    conn.execute(sql)


def _ensure_role(conn: Connection, role: UserRole) -> None:
    if has_user(conn, role.value):
        _LOG.debug("Role exists: %s", role.value)
        return

    sql = [
        f"create role {role.value} nologin inherit",
        "createrole" if role.can_create_user() else "nocreaterole",
    ]
    if (inherit := role.inherits_from()) is not None:
        sql.append("in role " + inherit.value)
    conn.execute(text(" ".join(sql)))


def grant_role(conn: Connection, role: UserRole, users: Iterable[str]) -> None:
    users = [escape_pg_identifier(conn, user) for user in users]
    with contextlib.suppress(ProgrammingError):
        # Ignore failure to revoke roles that we don't have permission to revoke.
        # e.g. because they were granted by a superuser.
        conn.execute(
            text(
                "revoke {roles} from {users}".format(
                    users=", ".join(users),
                    roles=", ".join(r.value for r in UserRole.higher_roles(role)),
                )
            )
        )

    conn.execute(
        text("grant {role} to {users}".format(users=", ".join(users), role=role.value))
    )


def has_user(conn: Connection, role_name: str) -> bool:
    return bool(
        conn.execute(
            text(f"SELECT rolname FROM pg_roles WHERE rolname='{role_name}'")
        ).fetchall()
    )


def has_schema(engine: Engine, schema_name: str = SCHEMA_NAME) -> bool:
    return inspect(engine).has_schema(schema_name)


def drop_schema(connection: Connection, schema_name: str = SCHEMA_NAME) -> None:
    connection.execute(DropSchema(schema_name, cascade=True, if_exists=True))


@deprecat(
    reason="The 'drop_db' function has been deprecated. "
    "Please use 'drop_schema' instead.",
    version="1.9.10",
    category=ODC2DeprecationWarning,
)
def drop_db(connection: Connection) -> None:
    drop_schema(connection)
