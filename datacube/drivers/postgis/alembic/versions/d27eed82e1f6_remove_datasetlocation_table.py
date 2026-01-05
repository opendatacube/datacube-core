# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""remove DatasetLocation table

Revision ID: d27eed82e1f6
Revises: 610f32dca3cb
Create Date: 2024-11-13 02:39:55.671819

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.sql import func
from datacube.drivers.postgis._core import METADATA
from sqlalchemy.exc import ProgrammingError


# revision identifiers, used by Alembic.
revision: str = 'd27eed82e1f6'
down_revision: Union[str, None] = '610f32dca3cb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    try:
        op.add_column("dataset",
                      Column("uri_scheme", String, comment="The scheme of the uri."),
                      schema="odc")
        op.add_column("dataset",
                      Column("uri_body", String, comment="The body of the uri."),
                      schema="odc")
    except ProgrammingError:
        print("Columns uri_scheme and uri_body already exist in dataset table.")
    # select first active location from DatasetLocation and insert into Dataset
    conn = op.get_bind()
    conn.execute(
        sa.text("""UPDATE odc.dataset d
                SET
                    uri_scheme = subquery.uri_scheme,
                    uri_body = subquery.uri_body
                FROM (
                    SELECT DISTINCT ON (l.dataset_ref)
                        l.dataset_ref, l.uri_scheme, l.uri_body
                    FROM odc.location l
                    WHERE archived IS NULL
                    ORDER BY l.dataset_ref, l.added
                ) subquery
                WHERE d.id = subquery.dataset_ref;""")
    )

    op.drop_table("location", schema="odc", if_exists=True)


def downgrade() -> None:
    loc = op.create_table(
        "location",
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("dataset_ref", postgres.UUID(as_uuid=True), ForeignKey("odc.dataset.id"), nullable=False,
               comment="The product this dataset belongs to"),
        Column("uri_scheme", String, nullable=False, comment="The scheme of the uri."),
        Column("uri_body", String, nullable=False, comment="The body of the uri."),
        Column("added", DateTime(timezone=True), server_default=func.now(), nullable=False, comment="when added"),
        Column("added_by", Text, server_default=func.current_user(), nullable=False, comment="added by whom"),
        Column("archived", DateTime(timezone=True), default=None, nullable=True, index=True,
               comment="when archived, null for the active location"),
        UniqueConstraint('uri_scheme', 'uri_body', 'dataset_ref'),
        Index("ix_loc_ds_added", "dataset_ref", "added"),
        METADATA,
        schema="odc",
        comment="Where data for the dataset can be found (uri).",
        if_not_exists=True,
    )

    conn = op.get_bind()
    res = conn.execute(
        sa.text("SELECT id, uri_scheme, uri_body FROM odc.dataset WHERE uri_body IS NOT NULL")
    ).fetchall()
    values = [{"dataset_ref": r.id, "uri_scheme": r.uri_scheme, "uri_body": r.uri_body} for r in res]
    op.bulk_insert(loc, values)

    op.drop_column("dataset", "uri_scheme", schema="odc")
    op.drop_column("dataset", "uri_body", schema="odc")
