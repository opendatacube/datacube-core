# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""Make 'added' and 'archived' columns indexed

Revision ID: 610f32dca3cb
Revises: 18afe14f2816
Create Date: 2024-08-08 04:46:55.465365

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '610f32dca3cb'
down_revision: Union[str, None] = '18afe14f2816'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index("ix_dataset_added", "dataset", ["added"], schema="odc", if_not_exists=True)
    op.create_index("ix_dataset_archived", "dataset", ["archived"], schema="odc", if_not_exists=True)


def downgrade() -> None:
    op.drop_index("ix_dataset_added", schema="odc")
    op.drop_index("ix_dataset_archived", schema="odc")
