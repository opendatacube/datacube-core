# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Core SQL schema settings.
"""

import contextlib
import logging
from collections.abc import Generator, Iterable
from enum import Enum
from typing import Literal, Union

from deprecat import deprecat
from sqlalchemy import Connection, MetaData, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.schema import CreateSchema, DropSchema

from datacube.drivers.postgres.sql import (
    ADDED_COLUMN_INDEX_SQL_TEMPLATE,
    ADDED_COLUMN_MIGRATE_SQL_TEMPLATE,
    INSTALL_TRIGGER_SQL_TEMPLATE,
    SCHEMA_NAME,
    TYPES_INIT_SQL,
    UPDATE_COLUMN_INDEX_SQL_TEMPLATE,
    UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE,
    UPDATE_TIMESTAMP_SQL,
    escape_pg_identifier,
    pg_column_exists,
)
from datacube.migration import ODC2DeprecationWarning


class UserRole(Enum):
    USER = "agdc_user"
    INGEST = "agdc_ingest"
    MANAGE = "agdc_manage"
    ADMIN = "agdc_admin"

    @classmethod
    def to_pg_role(
        cls, role_str: Literal["user", "ingest", "manage", "admin"]
    ) -> "UserRole":
        return cls("agdc_" + role_str.lower())

    def simple_str(self) -> str:
        return self.value.split("_", 1)[1]

    @classmethod
    def all_roles(cls) -> Generator[str]:
        for role in cls:
            yield role.simple_str()

    def higher_roles(self) -> list["UserRole"]:
        if self == UserRole.USER:
            return [UserRole.INGEST, UserRole.MANAGE, UserRole.ADMIN]
        if self == UserRole.INGEST:
            return [UserRole.MANAGE, UserRole.ADMIN]
        if self == UserRole.MANAGE:
            return [UserRole.ADMIN]
        return []

    def inherits_from(self) -> Union["UserRole", None]:
        if self == UserRole.ADMIN:
            return UserRole.MANAGE
        if self == UserRole.MANAGE:
            return UserRole.INGEST
        if self == UserRole.INGEST:
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

METADATA = MetaData(naming_convention=SQL_NAMING_CONVENTIONS, schema=SCHEMA_NAME)

_LOG: logging.Logger = logging.getLogger(__name__)


def install_timestamp_trigger(connection) -> None:
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
        connection.execute(
            text(
                UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE.format(
                    schema=SCHEMA_NAME, table=name
                )
            )
        )
        for s in INSTALL_TRIGGER_SQL_TEMPLATE:
            connection.execute(text(s.format(schema=SCHEMA_NAME, table=name)))

    # Add indexes for dataset table
    ds_table = _schema.DATASET.name
    connection.execute(
        text(
            UPDATE_COLUMN_INDEX_SQL_TEMPLATE.format(schema=SCHEMA_NAME, table=ds_table)
        )
    )
    connection.execute(
        text(ADDED_COLUMN_INDEX_SQL_TEMPLATE.format(schema=SCHEMA_NAME, table=ds_table))
    )


def install_added_column(connection) -> None:
    from . import _schema

    TABLE_NAME = _schema.DATASET_LOCATION.name  # noqa: N806
    connection.execute(
        text(
            ADDED_COLUMN_MIGRATE_SQL_TEMPLATE.format(
                schema=SCHEMA_NAME, table=TABLE_NAME
            )
        )
    )


def schema_qualified(name: str) -> str:
    """
    >>> schema_qualified("dataset")
    'agdc.dataset'
    """
    return f"{SCHEMA_NAME}.{name}"


def _get_quoted_connection_info(connection) -> tuple:
    db, user = connection.execute(
        text("select quote_ident(current_database()), quote_ident(current_user)")
    ).fetchone()
    return db, user


def ensure_db(engine, with_permissions: bool = True) -> bool:
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
            _LOG.info("Ensuring user roles.")
            for role in UserRole:
                _ensure_role(c, role)

            c.execute(
                text(f"""
            grant all on database {quoted_db_name} to agdc_admin;
            """)
            )
            c.commit()

        if is_new:
            if with_permissions:
                # Switch to 'agdc_admin', so that all items are owned by them.
                c.execute(text("set role agdc_admin"))
            _LOG.info("Creating schema.")
            c.execute(CreateSchema(SCHEMA_NAME))
            _LOG.info("Creating types.")
            for s in TYPES_INIT_SQL:
                c.execute(text(s))
            _LOG.info("Creating tables.")
            METADATA.create_all(c)
            _LOG.info("Creating triggers.")
            install_timestamp_trigger(c)
            _LOG.info("Creating added column.")
            install_added_column(c)
            if with_permissions:
                c.execute(text(f"set role {quoted_user}"))
            c.commit()

        if with_permissions:
            _LOG.info("Adding role grants.")
            c.execute(text(f"grant usage on schema {SCHEMA_NAME} to agdc_user"))
            c.execute(
                text(f"grant select on all tables in schema {SCHEMA_NAME} to agdc_user")
            )
            c.execute(
                text(
                    f"grant execute on function {SCHEMA_NAME}.common_timestamp(text) to agdc_user"
                )
            )

            c.execute(
                text(
                    f"grant insert on {SCHEMA_NAME}.dataset,"
                    f"{SCHEMA_NAME}.dataset_location,"
                    f"{SCHEMA_NAME}.dataset_source to agdc_ingest"
                )
            )
            c.execute(
                text(
                    f"grant usage, select on all sequences in schema {SCHEMA_NAME} to agdc_ingest"
                )
            )

            # (We're only granting deletion of types that have nothing written yet: they can't delete the data itself)
            c.execute(
                text(
                    f"grant insert, delete on {SCHEMA_NAME}.dataset_type,"
                    f"{SCHEMA_NAME}.metadata_type to agdc_manage"
                )
            )
            # Allow creation of indexes, views
            c.execute(text(f"grant create on schema {SCHEMA_NAME} to agdc_manage"))
            c.commit()

    return is_new


def database_exists(engine) -> bool:
    """
    Have they init'd this database?
    """
    return has_schema(engine)


def schema_is_latest(engine: Engine) -> bool:
    """
    Is the current schema up-to-date?

    This is run when a new connection is established to see if it's compatible.

    It should be runnable by unprivileged users. If it returns false, their
    connection will be rejected, and they will be told to get an administrator
    to apply updates.

    See the ``update_schema()`` function below for actually applying the updates.
    """
    # In lieu of a versioned schema, we typically check by seeing if one of the objects
    # from the change exists.
    #
    # E.g.
    #     return pg_column_exists(engine, 'dataset_location', 'archived')
    #
    # i.e. Does the 'archived' column exist? If so, we know the related schema
    # was applied.

    # No schema changes recently. Everything is perfect.
    return True


def update_schema(engine: Engine) -> None:
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
        if not pg_column_exists(connection, "dataset", "updated"):
            _LOG.info("Adding 'updated'/'added' fields and triggers to schema.")
            connection.execute(text("begin"))
            install_timestamp_trigger(connection)
            install_added_column(connection)
            connection.execute(text("commit"))
        else:
            _LOG.info("No schema updates required.")


def _ensure_role(conn, role: UserRole) -> None:
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


def has_user(conn, role_name: str) -> bool:
    res = conn.execute(
        text(f"SELECT rolname FROM pg_roles WHERE rolname='{role_name}'")
    ).fetchall()
    return bool(res)


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
