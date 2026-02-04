# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Core SQL schema settings.
"""

import logging
import os

from alembic import command, config
from alembic.migration import MigrationContext
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, text
from sqlalchemy.engine import Engine
from typing_extensions import Self, override

from datacube.drivers.common_psql import (
    UserRoleBase,
    as_role,
    create_schema,
    ensure_extension,
    ensure_role,
    get_connection_info,
    has_schema,
)
from datacube.drivers.postgis.sql import (
    INSTALL_TRIGGER_SQL_TEMPLATE,
    SCHEMA_NAME,
    TYPES_INIT_SQL,
    UPDATE_TIMESTAMP_SQL,
)


class UserRole(UserRoleBase):
    USER = "odc_user"
    MANAGE = "odc_manage"
    ADMIN = "odc_admin"

    @classmethod
    @override
    def to_pg_role(cls, role_str: str) -> Self:
        return cls("odc_" + role_str.lower())

    @override
    def higher_roles(self) -> list[Self]:
        if self == UserRole.USER:
            return [UserRole.MANAGE, UserRole.ADMIN]
        if self == UserRole.MANAGE:
            return [UserRole.ADMIN]
        return []

    @override
    def inherits_from(self) -> Self | None:
        if self == UserRole.ADMIN:
            return UserRole.MANAGE
        if self == UserRole.MANAGE:
            return UserRole.USER
        return None

    @override
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


def ensure_db(engine: Engine, with_permissions: bool = True) -> bool:
    """
    Initialise the db if needed.

    Ensures standard users exist.

    Create the schema if it doesn't exist.
    """
    is_new = not has_schema(engine, SCHEMA_NAME)
    with engine.connect() as c:
        #  NB. Using default SQLA2.0 auto-begin commit-as-you-go behaviour
        quoted_db_name, _ = get_connection_info(c)

        ensure_extension(c, "POSTGIS")
        c.commit()

        if with_permissions:
            _LOG.info("Ensuring user roles.")
            for ur in UserRole:
                ensure_role(c, ur)

            c.execute(
                text(f"""
            grant all on database {quoted_db_name} to odc_admin;
            """)
            )
            for ur in UserRole.ADMIN.lower_roles():
                c.execute(text(f"grant {ur.value} to odc_admin with admin true"))
            c.commit()

        if is_new:
            # If NOT new, it is up to the caller to update with alembic
            sqla_txn = c.begin()
            role = "odc_admin" if with_permissions else None
            with as_role(c, role) as c:
                _LOG.info("Creating schema.")
                create_schema(c, SCHEMA_NAME)
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
            _LOG.info("Adding role grants.")
            with as_role(c, "odc_admin") as c:
                c.execute(text(f"grant usage on schema {SCHEMA_NAME} to odc_user"))
                c.execute(
                    text(
                        f"grant select on all tables in schema {SCHEMA_NAME} to odc_user"
                    )
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
    Have they init'd this database? (Thin wrapper around ``has_schema()``)
    """
    return has_schema(engine, SCHEMA_NAME)


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
