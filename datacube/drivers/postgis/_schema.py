# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Tables for indexing the datasets which were ingested into the AGDC.
"""

import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Text,
    literal,
)
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import NUMRANGE, TSTZRANGE
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    column_property,
    mapped_column,
    registry,
    relationship,
)
from sqlalchemy.sql import func

from . import _core, sql

_LOG: logging.Logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


orm_registry = registry(metadata=Base.metadata)


class MetadataType(Base):
    __tablename__ = "metadata_type"
    __table_args__ = (
        _core.METADATA,
        CheckConstraint(r"name ~* '^\w+$'", name="alphanumeric_name"),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Metadata type, defining search fields requiring dynamic indexes",
        },
    )
    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    name: Mapped[str | None] = mapped_column(
        String,
        unique=True,
        comment="A human-friendly name/label for this metadata type",
    )
    definition = mapped_column(
        postgres.JSONB, nullable=False, comment="metadata schema with search fields"
    )
    # When it was added and by whom.
    added: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="when added"
    )
    added_by: Mapped[str] = mapped_column(
        Text, server_default=func.current_user(), comment="added by whom"
    )
    updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="when last updated"
    )

    products = relationship("Product")
    datasets = relationship("Dataset")


class Product(Base):
    __tablename__ = "product"
    __table_args__ = (
        _core.METADATA,
        CheckConstraint(r"name ~* '^\w+$'", name="alphanumeric_name"),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "A product or dataset type, family of related datasets.",
        },
    )
    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String, unique=True, comment="A human-friendly name/label for this product"
    )
    # DB column named metadata for (temporary) backwards compatibility,
    # but is forbidden by SQLAlchemy declarative style
    metadata_doc = mapped_column(
        name="metadata",
        type_=postgres.JSONB,
        nullable=False,
        comment="""The product metadata document (subset of the full definition)
All datasets of this type should contain these fields.
(newly-ingested datasets may be matched against these fields to determine the dataset type)""",
    )
    metadata_type_ref: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey(MetadataType.id),
        comment="The metadata type - how to interpret the metadata",
    )
    definition = mapped_column(
        "definition",
        postgres.JSONB,
        nullable=False,
        comment="Full product definition document",
    )
    added: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="when added"
    )
    added_by: Mapped[str] = mapped_column(
        Text, server_default=func.current_user(), comment="added by whom"
    )
    updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="when last updated"
    )

    datasets = relationship("Dataset")


class Dataset(Base):
    __tablename__ = "dataset"
    __table_args__ = (
        _core.METADATA,
        {"schema": sql.SCHEMA_NAME, "comment": "A dataset."},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    metadata_type_ref: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey(MetadataType.id),
        comment="The metadata type - how to interpret the metadata",
    )
    product_ref: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey(Product.id),
        comment="The product this dataset belongs to",
    )
    # DB column named metadata for (temporary) backwards compatibility,
    # but is forbidden by SQLAlchemy declarative style
    metadata_doc = mapped_column(
        name="metadata",
        type_=postgres.JSONB,
        nullable=False,
        comment="The dataset metadata document",
    )
    archived: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
        index=True,
        comment="when archived, null if active",
    )
    added: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
        comment="when added",
    )
    added_by: Mapped[str] = mapped_column(
        Text, server_default=func.current_user(), comment="added by whom"
    )

    updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
        comment="when last updated",
    )

    uri_scheme: Mapped[str | None] = mapped_column(
        String, comment="The scheme of the uri."
    )
    uri_body: Mapped[str | None] = mapped_column(
        String,
        comment="""The body of the uri.

The uri scheme and body make up the base URI to find the dataset.

All paths in the dataset metadata can be computed relative to this.
(it is often the path of the source metadata file)

eg 'file:///g/data/datasets/LS8_NBAR/odc-metadata.yaml' or 'ftp://eo.something.com/dataset'
'file' is a scheme, '///g/data/datasets/LS8_NBAR/odc-metadata.yaml' is a body.""",
    )
    uri = column_property(uri_scheme + literal(":") + uri_body)


Index(
    "ix_ds_prod_active",
    Dataset.product_ref,
    postgresql_where=(Dataset.archived == None),
)
Index(
    "ix_ds_mdt_active",
    Dataset.metadata_type_ref,
    postgresql_where=(Dataset.archived == None),
)


class DatasetLineage(Base):
    __tablename__ = "dataset_lineage"
    __table_args__ = (
        _core.METADATA,
        PrimaryKeyConstraint("source_dataset_ref", "derived_dataset_ref"),
        Index("ix_lin_derived_classifier", "derived_dataset_ref", "classifier"),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Represents a source-lineage relationship between two datasets",
        },
    )
    derived_dataset_ref: Mapped[UUID] = mapped_column(
        index=True,
        comment="The downstream derived dataset produced from the upstream source dataset.",
    )
    source_dataset_ref: Mapped[UUID] = mapped_column(
        index=True,
        comment="An upstream source dataset that the downstream derived dataset was produced from.",
    )
    classifier: Mapped[str] = mapped_column(
        String,
        comment="""An identifier for this source dataset.
E.g. the dataset type ('ortho', 'nbar'...) if there's only one source of each type, or a datestamp
for a time-range summary.""",
    )


class DatasetHome(Base):
    __tablename__ = "dataset_home"
    __table_args__ = (
        _core.METADATA,
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Represents an optional 'home index' for an external datasets",
        },
    )
    dataset_ref: Mapped[UUID] = mapped_column(
        primary_key=True,
        comment="The dataset ID - no referential integrity enforced to dataset table.",
    )
    home: Mapped[str] = mapped_column(
        Text,
        comment="""The 'home' index where this dataset can be found.
Not interpreted directly by ODC, provided as a convenience to database administrators.""",
    )


class SpatialIndex:
    """
    Base class for dynamically SpatialIndex ORM models (See _spatial.py)
    """


class SpatialIndexRecord(Base):
    __tablename__ = "spatial_indicies"
    __table_args__ = (
        _core.METADATA,
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Record of the existence of a Spatial Index Table for an SRID/CRS",
        },
    )
    srid: Mapped[int] = mapped_column(
        SmallInteger, primary_key=True, autoincrement=False
    )
    table_name: Mapped[str | None] = mapped_column(
        String,
        unique=True,
        comment="The name of the table implementing the index - DO NOT CHANGE",
    )
    added: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="when added"
    )
    added_by: Mapped[str] = mapped_column(
        Text, server_default=func.current_user(), comment="added by whom"
    )


# In theory could put dataset_ref and search_key in shared parent class, but having a foreign key
# in such a class requires weird and esoteric SQLAlchemy features.  Just leave as separate
# classes with duped columns for now.


class DatasetSearchString(Base):
    __tablename__ = "dataset_search_string"
    __table_args__ = (
        _core.METADATA,
        PrimaryKeyConstraint("dataset_ref", "search_key"),
        Index("ix_string_search", "search_key", "search_val"),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Index for searching datasets by search fields of string type",
        },
    )
    dataset_ref: Mapped[UUID] = mapped_column(
        ForeignKey(Dataset.id),
        index=True,
        comment="The dataset indexed by this search field record.",
    )
    search_key: Mapped[str] = mapped_column(
        String, index=True, comment="The name of the search field"
    )
    search_val: Mapped[str | None] = mapped_column(
        String, comment="The value of the string search field"
    )


class DatasetSearchNumeric(Base):
    __tablename__ = "dataset_search_num"
    __table_args__ = (
        _core.METADATA,
        PrimaryKeyConstraint("dataset_ref", "search_key"),
        Index("ix_num_search", "search_val", postgresql_using="gist"),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Index for searching datasets by search fields of numeric type",
        },
    )
    dataset_ref: Mapped[UUID] = mapped_column(
        ForeignKey(Dataset.id),
        index=True,
        comment="The dataset indexed by this search field record.",
    )
    search_key: Mapped[str] = mapped_column(
        String, index=True, comment="The name of the search field"
    )
    search_val = mapped_column(
        NUMRANGE, nullable=True, comment="The value of the numeric range search field"
    )


class DatasetSearchDateTime(Base):
    __tablename__ = "dataset_search_datetime"
    __table_args__ = (
        _core.METADATA,
        PrimaryKeyConstraint("dataset_ref", "search_key"),
        Index("ix_dt_search", "search_val", postgresql_using="gist"),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Index for searching datasets by search fields of datetime type",
        },
    )
    dataset_ref: Mapped[UUID] = mapped_column(
        ForeignKey(Dataset.id),
        index=True,
        comment="The dataset indexed by this search field record.",
    )
    search_key: Mapped[str] = mapped_column(
        String, index=True, comment="The name of the search field"
    )
    search_val = mapped_column(
        TSTZRANGE, nullable=True, comment="The value of the datetime search field"
    )


search_field_map = {
    "numeric-range": "numeric",
    "double-range": "numeric",
    "integer-range": "numeric",
    "datetime-range": "datetime",
    "string": "string",
    "numeric": "numeric",
    "double": "numeric",
    "integer": "numeric",
    "datetime": "datetime",
    "boolean": "numeric",
    # For backwards compatibility (alias for numeric-range)
    "float-range": "numeric",
}

search_field_indexes: dict[
    str, type[DatasetSearchString | DatasetSearchNumeric | DatasetSearchDateTime]
] = {
    "string": DatasetSearchString,
    "numeric": DatasetSearchNumeric,
    "datetime": DatasetSearchDateTime,
}

search_field_index_map: dict[
    str, type[DatasetSearchDateTime | DatasetSearchNumeric | DatasetSearchString]
] = {k: search_field_indexes[v] for k, v in search_field_map.items()}
