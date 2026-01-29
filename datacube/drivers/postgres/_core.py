# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Core SQL schema settings.
"""

import logging
from enum import Enum

from sqlalchemy import Connection, MetaData, text
from sqlalchemy.engine import Engine
from typing_extensions import Self, override

from datacube.drivers.common_psql import (
    UserRoleBase,
    as_role,
    create_schema,
    ensure_role,
    get_connection_info,
    has_role,
    has_schema,
)
from datacube.drivers.postgres.sql import (
    ADDED_COLUMN_INDEX_SQL_TEMPLATE,
    ADDED_COLUMN_MIGRATE_SQL_TEMPLATE,
    INSTALL_TRIGGER_SQL_TEMPLATE,
    SCHEMA_NAME,
    TYPES_INIT_SQL,
    UPDATE_COLUMN_INDEX_SQL_TEMPLATE,
    UPDATE_COLUMN_MIGRATE_SQL_TEMPLATE,
    UPDATE_TIMESTAMP_SQL,
    pg_column_exists,
)


class UserRole(UserRoleBase, Enum):
    USER = "agdc_user"
    INGEST = "agdc_ingest"
    MANAGE = "agdc_manage"
    ADMIN = "agdc_admin"

    @classmethod
    @override
    def to_pg_role(cls, role_str: str) -> Self:
        return cls("agdc_" + role_str.lower())

    @override
    def higher_roles(self) -> list[Self]:
        if self == UserRole.USER:
            return [UserRole.INGEST, UserRole.MANAGE, UserRole.ADMIN]
        if self == UserRole.INGEST:
            return [UserRole.MANAGE, UserRole.ADMIN]
        if self == UserRole.MANAGE:
            return [UserRole.ADMIN]
        return []

    @override
    def inherits_from(self) -> Self | None:
        if self == UserRole.ADMIN:
            return UserRole.MANAGE
        if self == UserRole.MANAGE:
            return UserRole.INGEST
        if self == UserRole.INGEST:
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


def ensure_db(engine, with_permissions: bool = True) -> bool:
    """
    Initialise the db if needed.

    Ensures standard users exist.

    Create the schema if it doesn't exist.
    """
    is_new = not has_schema(engine, SCHEMA_NAME)
    with engine.connect() as c:
        #  NB. Using default SQLA2.0 auto-begin commit-as-you-go behaviour
        db_name, _ = get_connection_info(c)

        if with_permissions:
            _LOG.info("Ensuring user roles.")
            for ur in UserRole:
                ensure_role(c, ur)

            c.execute(
                text(f"""
            grant all on database {db_name} to agdc_admin;
            """)
            )
            c.commit()

        if is_new:
            role = "agdc_admin" if with_permissions else None
            with as_role(c, role) as c:
                _LOG.info("Creating schema.")
                create_schema(c, SCHEMA_NAME)
                _LOG.info("Creating types.")
                for s in TYPES_INIT_SQL:
                    c.execute(text(s))
                _LOG.info("Creating tables.")
                METADATA.create_all(c)
                _LOG.info("Creating triggers.")
                install_timestamp_trigger(c)
                _LOG.info("Creating added column.")
                install_added_column(c)
            c.commit()

        if with_permissions:
            _LOG.info("Adding role grants.")
            with as_role(c, "agdc_admin") as c:
                c.execute(text(f"grant usage on schema {SCHEMA_NAME} to agdc_user"))
                c.execute(
                    text(
                        f"grant select on all tables in schema {SCHEMA_NAME} to agdc_user"
                    )
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

                # We're only granting deletion of types that have nothing written yet:
                #   they can't delete the data itself
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
    Have they init'd this database? (Thin wrapper around ``has_schema()``)
    """
    return has_schema(engine, SCHEMA_NAME)


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


def table_transfers_required(
    conn: Connection, new_owner: str, schema: str, tables: list[str]
) -> list[tuple[str, str]]:
    """
    :return: List of tuples of tablename, current_owner of tables requiring transfer
    """
    transfers: list[tuple[str, str]] = []
    for row in conn.execute(
        text(
            f"select tablename, tableowner from pg_tables where schemaname = '{schema}' "
            f"and tablename in {tuple(tables)}"
        )
    ):
        if row.tableowner != new_owner:
            transfers.append((row.tablename, row.tableowner))
    return transfers


def view_transfers_required(
    conn: Connection, new_owner: str, schema: str, prefix: str
) -> list[tuple[str, str]]:
    """
    :return: List of tuples of viewname, current_owner of views requiring transfer
    """
    transfers: list[tuple[str, str]] = []
    for row in conn.execute(
        text(
            f"select viewname, viewowner from pg_views where schemaname = '{schema}' "
            f"and viewname like '{prefix}%'"
        )
    ):
        if row.viewowner != new_owner:
            transfers.append((row.viewname, row.viewowner))
    return transfers


def update_schema(engine: Engine, with_permissions: bool) -> None:
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
        updated = False
        _, user = get_connection_info(connection)

        if with_permissions:
            is_super = has_role(connection, user, superuser=True)
            # ensure tables are all owned by agdc_admin
            transfers = table_transfers_required(
                connection,
                "agdc_admin",
                SCHEMA_NAME,
                [
                    "metadata_type",
                    "dataset_type",
                    "dataset",
                    "dataset_location",
                    "dataset_source",
                ],
            )
            if transfers:
                for table, current_owner in transfers:
                    if is_super or current_owner == user:
                        _LOG.info(f"Transferring ownership of {table} to agdc_admin")
                        connection.execute(
                            text(
                                f"alter table {SCHEMA_NAME}.{table} owner to agdc_admin"
                            )
                        )
                        updated = True
                    else:
                        _LOG.warning(
                            f"Cannot transfer ownership of {table} from {current_owner} to agdc_admin: "
                            f"user {user} is not a superuser or current owner"
                        )

            # ensure dynamic views are all owned by agdc_manage
            transfers = view_transfers_required(
                connection, "agdc_manage", SCHEMA_NAME, "dv_"
            )
            if transfers:
                for view, current_owner in transfers:
                    if is_super or current_owner == user:
                        _LOG.info(f"Transferring ownership of {view} to agdc_manage")
                        connection.execute(
                            text(
                                f"alter view {SCHEMA_NAME}.{view} owner to agdc_manage"
                            )
                        )
                        updated = True
                    else:
                        _LOG.warning(
                            f"Cannot transfer ownership of {view} from {current_owner} to agdc_manage: "
                            f"user {user} is not a superuser or current owner"
                        )

        if not pg_column_exists(connection, "dataset", "updated"):
            _LOG.info("Adding 'updated'/'added' fields and triggers to schema.")
            connection.execute(text("begin"))
            with as_role(connection, "agdc_admin"):
                install_timestamp_trigger(connection)
                install_added_column(connection)
            connection.execute(text("commit"))
            updated = True

        if not updated:
            _LOG.info("No schema updates required.")
