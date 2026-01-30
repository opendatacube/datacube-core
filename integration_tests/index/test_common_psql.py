# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from sqlalchemy import text


# These tests use an empty/uninitialised database.  Doesn't matter whether it is postgis or postgres.

@pytest.fixture
def specific_user(uninitialised_postgres_db, cfg_env):
    from datacube.drivers.common_psql import create_schema, transfers_required, as_role, ensure_role, create_user, drop_users
    from datacube.drivers.postgis._core import UserRole, SCHEMA_NAME

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
def bare_user(uninitialised_postgres_db, cfg_env):
    from datacube.drivers.common_psql import drop_users

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        conn.execute(text("create role bare_user"))
    yield "bare_user"
    with engine.connect() as conn:
        drop_users(conn, ["bare_user"])


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"))
def test_transfer_perms(uninitialised_postgres_db, specific_user):
    from datacube.drivers.common_psql import create_schema, drop_schema, transfers_required, as_role, transfer_ownership
    from datacube.drivers.postgis._core import SCHEMA_NAME

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        create_schema(conn, SCHEMA_NAME)
        conn.execute(text(f"grant all privileges on schema {SCHEMA_NAME} to {specific_user}"))
        conn.execute(text(f"grant create on schema {SCHEMA_NAME} to {specific_user}"))
        conn.commit()
        with as_role(conn, specific_user) as conn:
            conn.execute(text(f"create table {SCHEMA_NAME}.test_table (id serial primary key)"))

        try:
            transfers = transfers_required(conn, 'odc_admin', SCHEMA_NAME, "tables", prefix="test_")
            assert transfers == [("test_table", specific_user)]

            transfer_ownership(conn, SCHEMA_NAME, "test_table", specific_user, "odc_admin", "tables")

            transfers = transfers_required(conn, 'odc_admin', SCHEMA_NAME, "tables", prefix="test_")
            assert transfers == []
        finally:
            drop_schema(conn, SCHEMA_NAME)


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"))
def test_has_roles(uninitialised_postgres_db, specific_user):
    from datacube.drivers.common_psql import has_roles
    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        assert has_roles(conn, [specific_user, "odc_admin", "odc_manage", "odc_user"])


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"))
def test_ensure_role(uninitialised_postgres_db, bare_user):
    from datacube.drivers.common_psql import as_role, ensure_role, has_role
    from datacube.drivers.postgis._core import UserRole
    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        assert has_role(conn, "odc_admin", with_create_role=True)
        conn.execute(text(f"alter role odc_admin with nocreaterole"))
        assert not has_role(conn, "odc_admin", with_create_role=True)
        ensure_role(conn, UserRole.ADMIN)
        assert has_role(conn, "odc_admin", with_create_role=True)

    with engine.connect() as conn:
        conn.execute(text(f"alter role odc_admin with nocreaterole"))
        assert not has_role(conn, "odc_admin", with_create_role=True)
        with as_role(conn, bare_user) as conn:
            assert not ensure_role(conn, UserRole.ADMIN)
        assert ensure_role(conn, UserRole.ADMIN)


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"))
def test_create_user(uninitialised_postgres_db, specific_user, bare_user):
    from datacube.drivers.common_psql import create_user, as_role
    from datacube.drivers.postgis._core import UserRole

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        assert not create_user(conn, specific_user, "test_pass", UserRole.ADMIN)

        with as_role(conn, bare_user) as conn:
            assert not create_user(conn, "brand_new_user", "test_pass", UserRole.ADMIN)


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"))
def test_drop_users(uninitialised_postgres_db, specific_user, bare_user):
    from datacube.drivers.common_psql import drop_users, as_role

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        with as_role(conn, bare_user) as conn:
            assert not drop_users(conn, [specific_user])


@pytest.mark.parametrize("datacube_env_name", ("postgis", "postgis3"))
def test_as_role(uninitialised_postgres_db, specific_user, bare_user):
    from datacube.drivers.common_psql import as_role

    engine = uninitialised_postgres_db._engine
    with engine.connect() as conn:
        db_user = conn.execute(text("select quote_ident(current_user)")).scalar()
        with as_role(conn, None) as conn:
            assert conn.execute(text("select quote_ident(current_user)")).scalar() == db_user
        with as_role(conn, specific_user) as conn:
            assert conn.execute(text("select quote_ident(current_user)")).scalar() == specific_user
            with as_role(conn, bare_user) as conn:
               assert conn.execute(text("select quote_ident(current_user)")).scalar() == bare_user
            assert conn.execute(text("select quote_ident(current_user)")).scalar() == specific_user
        assert conn.execute(text("select quote_ident(current_user)")).scalar() == db_user
