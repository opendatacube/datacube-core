# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""Add the 'updated' column

Revision ID: 18afe14f2816
Revises: 48fb71ece268
Create Date: 2024-08-08 04:16:19.168213

"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy import Column, DateTime
from sqlalchemy.sql import func


# revision identifiers, used by Alembic.
revision: str = '18afe14f2816'
down_revision: Union[str, None] = '48fb71ece268'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("dataset", Column("updated", DateTime(timezone=True), server_default=func.now(),
                                    nullable=False, index=True, comment="when last updated"))
    op.add_column("metadata_type", Column("updated", DateTime(timezone=True), server_default=func.now(),
                                          nullable=False, comment="when last updated"))
    op.add_column("product", Column("updated", DateTime(timezone=True), server_default=func.now(),
                                    nullable=False, comment="when last updated"))


def downgrade() -> None:
    op.drop_column("dataset", "updated")
    op.drop_column("metadata_type", "updated")
    op.drop_column("product", "updated")

    op.drop_index("ix_dataset_updated")
