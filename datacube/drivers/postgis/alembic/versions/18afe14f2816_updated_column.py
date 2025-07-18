# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""Add the 'updated' column

Revision ID: 18afe14f2816
Revises: 48fb71ece268
Create Date: 2024-08-08 04:16:19.168213

"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy import Column, DateTime, inspect
from sqlalchemy.sql import func


# revision identifiers, used by Alembic.
revision: str = '18afe14f2816'
down_revision: Union[str, None] = '48fb71ece268'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def column_exists(table_name, column_name):
    bind = op.get_context().bind
    insp = inspect(bind)
    columns = insp.get_columns(table_name, "odc")
    return any(c["name"] == column_name for c in columns)


def upgrade() -> None:
    if not column_exists("dataset", "updated"):
        op.add_column("dataset", Column("updated", DateTime(timezone=True), server_default=func.now(),
                                        nullable=False, index=True, comment="when last updated"), schema="odc")
    else:
        op.alter_column("dataset", "updated", schema="odc", type_=DateTime(timezone=True),
                        server_default=func.now(), nullable=False, comment="when last updated")  # type: ignore[arg-type] # noqa: E501
    op.create_index("ix_dataset_updated", "dataset", ["updated"], schema="odc", if_not_exists=True)

    if not column_exists("metadata_type", "updated"):
        op.add_column("metadata_type", Column("updated", DateTime(timezone=True), server_default=func.now(),
                                              nullable=False, comment="when last updated"), schema="odc")
    else:
        op.alter_column("metadata_type", "updated", schema="odc", type_=DateTime(timezone=True),
                        server_default=func.now(), nullable=False, comment="when last updated")  # type: ignore[arg-type] # noqa: E501

    if not column_exists("product", "updated"):
        op.add_column("product", Column("updated", DateTime(timezone=True), server_default=func.now(),
                                        nullable=False, comment="when last updated"), schema="odc")
    else:
        op.alter_column("product", "updated", schema="odc", type_=DateTime(timezone=True),
                        server_default=func.now(), nullable=False, comment="when last updated")  # type: ignore[arg-type] # noqa: E501


def downgrade() -> None:
    op.drop_column("dataset", "updated", schema="odc")
    op.drop_column("metadata_type", "updated", schema="odc")
    op.drop_column("product", "updated", schema="odc")

    op.drop_index("ix_dataset_updated", schema="odc", if_exists=True)
