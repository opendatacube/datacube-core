# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""Initial revision

Revision ID: 48fb71ece268
Revises:
Create Date: 2023-11-21 16:28:45.209473

"""
from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = '48fb71ece268'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# It is helpful for schema management to have an empty initial revision


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
