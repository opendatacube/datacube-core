# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from sqlalchemy import text

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Generator

# These tests use an empty/uninitialised database.
# Doesn't matter whether it is postgis or postgres - use postgis for future-proofing


@pytest.fixture
def specific_user(uninitialised_postgres_db, cfg_env) -> Generator[str]:
    from datacube.drivers.common_psql import (
        create_user,
        drop_users,
        ensure_role,
    )
    from datacube.drivers.postgis._core import UserRole

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        ensure_role(conn, UserRole.USER)
        ensure_role(conn, UserRole.MANAGE)
        ensure_role(conn, UserRole.ADMIN)
        create_user(conn, "admin_user", "test_pass", UserRole.ADMIN)
    yield "admin_user"
    with engine.connect() as conn:
        drop_users(conn, ["admin_user"])


@pytest.fixture
def bare_user(uninitialised_postgres_db, cfg_env) -> Generator[str]:
    from datacube.drivers.common_psql import drop_users

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        conn.execute(text("create role bare_user"))
    yield "bare_user"
    with engine.connect() as conn:
        drop_users(conn, ["bare_user"])


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"), indirect=True)
@pytest.mark.parametrize("db_tz", ("UTC",), indirect=True)
def test_transfer_perms(uninitialised_postgres_db, specific_user, bare_user) -> None:
    from datacube.drivers.common_psql import (
        as_role,
        create_schema,
        drop_schema,
        transfer_ownership,
        transfers_required,
    )
    from datacube.drivers.postgis._core import SCHEMA_NAME

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        create_schema(conn, SCHEMA_NAME)
        conn.execute(
            text(f"grant all privileges on schema {SCHEMA_NAME} to {specific_user}")
        )
        conn.execute(text(f"grant create on schema {SCHEMA_NAME} to {specific_user}"))
        conn.commit()
        with as_role(conn, specific_user) as conn:
            conn.execute(
                text(f"create table {SCHEMA_NAME}.test_table (id serial primary key)")
            )
            conn.execute(
                text(f"create table {SCHEMA_NAME}.test_table_2 (id serial primary key)")
            )
            conn.execute(
                text(
                    f"create materialized view {SCHEMA_NAME}.test_mv as select * from {SCHEMA_NAME}.test_table"
                )
            )
        with as_role(conn, bare_user) as conn:
            # should be a no-op, result tested below
            transfer_ownership(
                conn, SCHEMA_NAME, "test_table", specific_user, "odc_admin", "tables"
            )

        with pytest.raises(
            ValueError, match="Must specify one of either objects or prefix"
        ):
            transfers = transfers_required(
                conn,
                "odc_admin",
                SCHEMA_NAME,
                "tables",
            )

        try:
            transfers = transfers_required(
                conn, "odc_admin", SCHEMA_NAME, "tables", prefix="test_"
            )
            assert transfers == [
                ("test_table", specific_user),
                ("test_table_2", specific_user),
            ]

            transfer_ownership(
                conn, SCHEMA_NAME, "test_table", specific_user, "odc_admin", "tables"
            )

            transfers = transfers_required(
                conn, "odc_admin", SCHEMA_NAME, "tables", prefix="test_"
            )
            assert transfers == [("test_table_2", specific_user)]

            transfer_ownership(
                conn, SCHEMA_NAME, "test_mv", specific_user, "odc_admin", "matviews"
            )
            transfers = transfers_required(
                conn, "odc_admin", SCHEMA_NAME, "matviews", prefix="test_"
            )
            assert transfers == []

        finally:
            drop_schema(conn, SCHEMA_NAME)


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"), indirect=True)
@pytest.mark.parametrize("db_tz", ("UTC",), indirect=True)
def test_has_roles(uninitialised_postgres_db, specific_user) -> None:
    from datacube.drivers.common_psql import has_roles

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        assert has_roles(conn, [specific_user, "odc_admin", "odc_manage", "odc_user"])


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"), indirect=True)
@pytest.mark.parametrize("db_tz", ("UTC",), indirect=True)
def test_ensure_role(uninitialised_postgres_db, bare_user) -> None:
    from datacube.drivers.common_psql import as_role, ensure_role, has_role
    from datacube.drivers.postgis._core import UserRole

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        assert has_role(conn, "odc_admin", with_create_role=True)
        conn.execute(text("alter role odc_admin with nocreaterole"))
        assert not has_role(conn, "odc_admin", with_create_role=True)
        ensure_role(conn, UserRole.ADMIN)
        assert has_role(conn, "odc_admin", with_create_role=True)

    with engine.connect() as conn:
        conn.execute(text("alter role odc_admin with nocreaterole"))
        assert not has_role(conn, "odc_admin", with_create_role=True)
        with as_role(conn, bare_user) as conn:
            assert not ensure_role(conn, UserRole.ADMIN)
        assert ensure_role(conn, UserRole.ADMIN)


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"), indirect=True)
@pytest.mark.parametrize("db_tz", ("UTC",), indirect=True)
def test_create_user(uninitialised_postgres_db, specific_user, bare_user) -> None:
    from datacube.drivers.common_psql import as_role, create_user
    from datacube.drivers.postgis._core import UserRole

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        assert not create_user(conn, specific_user, "test_pass", UserRole.ADMIN)

        with as_role(conn, bare_user) as conn:
            assert not create_user(conn, "brand_new_user", "test_pass", UserRole.ADMIN)


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"), indirect=True)
@pytest.mark.parametrize("db_tz", ("UTC",), indirect=True)
def test_drop_users(uninitialised_postgres_db, specific_user, bare_user) -> None:
    from datacube.drivers.common_psql import as_role, drop_users

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn, as_role(conn, bare_user) as conn:
        assert not drop_users(conn, [specific_user])


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"), indirect=True)
@pytest.mark.parametrize("db_tz", ("UTC",), indirect=True)
def test_grant_role(uninitialised_postgres_db, specific_user, bare_user) -> None:
    from datacube.drivers.common_psql import as_role, grant_role
    from datacube.drivers.postgis._core import UserRole

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn, as_role(conn, bare_user) as conn:
        assert not grant_role(conn, UserRole.MANAGE, [specific_user])


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"), indirect=True)
@pytest.mark.parametrize("db_tz", ("UTC",), indirect=True)
def test_as_role(uninitialised_postgres_db, specific_user, bare_user) -> None:
    from datacube.drivers.common_psql import as_role, get_connection_info

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        _, init_user = get_connection_info(conn)
        with as_role(conn, None) as conn:
            _, as_user = get_connection_info(conn)
            assert as_user == init_user
        with as_role(conn, specific_user) as conn:
            _, as_user = get_connection_info(conn)
            assert as_user == specific_user
            with as_role(conn, bare_user) as conn:
                _, as_user = get_connection_info(conn)
                assert as_user == bare_user
            _, as_user = get_connection_info(conn)
            assert as_user == specific_user
        _, as_user = get_connection_info(conn)
        assert as_user == init_user


def test_driver_roles() -> None:
    from datacube.drivers.postgis._core import UserRole as PostgisUserRole
    from datacube.drivers.postgres._core import UserRole as PostgresUserRole

    for UserRole in PostgresUserRole, PostgisUserRole:  # noqa: N806
        for role in UserRole:
            inherit = role.inherits_from()
            assert inherit is None or inherit in role.lower_roles()
            assert inherit not in role.higher_roles()
            if role != UserRole.ADMIN:
                assert role in UserRole.ADMIN.lower_roles()
