# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""Permissions cleanup

Revision ID: 01fa1abedd6d
Revises: d27eed82e1f6
Create Date: 2025-11-04 09:02:19.111741

"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from alembic.util.exc import CommandError

from datacube.model import SCHEMA_PATH

# revision identifiers, used by Alembic.
revision: str = '01fa1abedd6d'
down_revision: str | None = 'd27eed82e1f6'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def confirm_user_can_transfer(
        conn: sa.Connection,
        schema: str,
        tables: list[str]) -> str:
    from datacube.drivers.postgis._core import get_connection_info
    _, user = get_connection_info(conn)
    row = conn.execute(
        sa.text(f"select rolname , rolsuper from pg_roles WHERE rolname = '{user}'")
    ).fetchone()
    assert row is not None  # Mypy doesn't understand that the above SQL always returns a row.
    _, is_super = row
    if is_super:
        # We are a superuser, we can do anything.
        return user
    for row in conn.execute(sa.text(
        f"select tablename, tableowner from pg_tables where schemaname = '{schema}' "
        f"and tablename in {tuple(tables)}"
    )):
        # tableowner is target_user - no change required
        # tableowner is the current user - should be able to transfer
        if row.tableowner not in ('odc_admin', user):
            raise CommandError("Insufficient permissions to upgrade to schema revision 01fa1abedd6d.\n"
                               f"Try running as a database superuser or as {row.tableowner}.")
    return user


def tables_to_transfer(conn: sa.Connection, schema: str) -> list[str]:
    """
    Return list of tables in the odc schema that may not yet belong to odc_admin

    :param conn:
    :return: list of table names
    """
    tables = ["alembic_version",]
    for row in conn.execute(sa.text(f"select table_name from {schema}.spatial_indicies")):
        tables.append(f"{row.table_name}")
    return tables


def upgrade() -> None:
    from datacube.drivers.postgis._core import SCHEMA_NAME
    conn = op.get_bind()
    # Obtain list of tables that potentially need ownership transferred to odc_admin
    tables = tables_to_transfer(conn, SCHEMA_NAME)
    # Check we have necessary permissions to do the ownership transfer
    user = confirm_user_can_transfer(conn, SCHEMA_NAME, tables)
    # Transfer ownership of alembic tables to odc_admin
    for table in tables:
        conn.execute(sa.text(f"alter table {SCHEMA_NAME}.{table} owner to odc_admin"))

    # Enforce hierarchical permissions
    conn.execute(sa.text("grant odc_user to odc_manage"))
    conn.execute(sa.text("grant odc_manage to odc_admin"))


def downgrade() -> None:
    # Ownership of the affected tables was previously "uncontrolled" and simply belonged
    # to the user who first ran `datacube system init`.
    # The downgrade process therefore cannot guarantee previous ownership was restored, so skip.
    #
    # Remove hierarchical permissions
    conn = op.get_bind()
    conn.execute(sa.text("revoke odc_manage from odc_admin"))
    conn.execute(sa.text("revoke odc_user from odc_manage"))
