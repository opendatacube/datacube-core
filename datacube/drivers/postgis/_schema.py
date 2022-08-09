# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Tables for indexing the datasets which were ingested into the AGDC.
"""

import logging

from sqlalchemy.orm import aliased, registry, relationship, column_property
from sqlalchemy import ForeignKey, UniqueConstraint, PrimaryKeyConstraint, CheckConstraint, SmallInteger, Text
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.sql import func

from . import sql
from . import _core

_LOG = logging.getLogger(__name__)

orm_registry = registry()


@orm_registry.mapped
class MetadataType:
    __tablename__ = "metadata_type"
    __table_args__ = (
        _core.METADATA,
        CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Metadata type, defining search fields requiring dynamic indexes",
        }
    )
    id = Column(SmallInteger, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=True, comment="A human-friendly name/label for this metadata type")
    definition = Column(postgres.JSONB, nullable=False, comment="metadata schema with search fields")
    # When it was added and by whom.
    added = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="when added")
    added_by = Column(Text, server_default=func.current_user(), nullable=False, comment="added by whom")

    products = relationship("Product")
    datasets = relationship("Dataset")


@orm_registry.mapped
class Product:
    __tablename__ = "product"
    __table_args__ = (
        _core.METADATA,
        CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "A product or dataset type, family of related datasets."
        }
    )
    id = Column(SmallInteger, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False, comment="A human-friendly name/label for this product")
    # DB column named metadata for (temporary) backwards compatibility,
    # but is forbidden by SQLAlchemy declarative style
    metadata_doc = Column(name="metadata",
                          type_=postgres.JSONB, nullable=False,
                          comment="""The product metadata document (subset of the full definition)
All datasets of this type should contain these fields.
(newly-ingested datasets may be matched against these fields to determine the dataset type)""")
    metadata_type_ref = Column(SmallInteger, ForeignKey(MetadataType.id), nullable=False,
                               comment="The metadata type - how to interpret the metadata")
    definition = Column('definition', postgres.JSONB, nullable=False, comment="Full product definition document")
    added = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="when added")
    added_by = Column(Text, server_default=func.current_user(), nullable=False, comment="added by whom")

    datasets = relationship("Dataset")


@orm_registry.mapped
class Dataset:
    __tablename__ = "dataset"
    __table_args__ = (
        _core.METADATA,
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "A dataset."
        }
    )
    id = Column(postgres.UUID(as_uuid=True), primary_key=True)
    metadata_type_ref = Column(SmallInteger, ForeignKey(MetadataType.id), nullable=False,
                               comment="The metadata type - how to interpret the metadata")
    product_ref = Column(SmallInteger, ForeignKey(Product.id), nullable=False,
                         comment="The product this dataset belongs to")
    # DB column named metadata for (temporary) backwards compatibility,
    # but is forbidden by SQLAlchemy declarative style
    metadata_doc = Column(name="metadata", type_=postgres.JSONB, index=False, nullable=False,
                          comment="The dataset metadata document")
    archived = Column(DateTime(timezone=True), default=None, nullable=True,
                      comment="when archived, null if active")
    added = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="when added")
    added_by = Column(Text, server_default=func.current_user(), nullable=False, comment="added by whom")

    locations = relationship("DatasetLocation", viewonly=True)
    active_locations = relationship("DatasetLocation",
                                    primaryjoin="and_(Dataset.id==DatasetLocation.dataset_ref, "
                                                "DatasetLocation.archived==None)",
                                    viewonly=True,
                                    order_by="desc(DatasetLocation.added)")
    archived_locations = relationship("DatasetLocation",
                                      viewonly=True,
                                      primaryjoin="and_(Dataset.id==DatasetLocation.dataset_ref, "
                                                  "DatasetLocation.archived!=None)"
                                     )


@orm_registry.mapped
class DatasetLocation:
    __tablename__ = "location"
    __table_args__ = (
        _core.METADATA,
        UniqueConstraint('uri_scheme', 'uri_body', 'dataset_ref'),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Where data for the dataset can be found (uri)."
        }
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_ref = Column(postgres.UUID(as_uuid=True), ForeignKey(Dataset.id), nullable=False,
                         comment="The product this dataset belongs to")
    uri_scheme = Column(String, nullable=False, comment="The scheme of the uri.")
    uri_body = Column(String, nullable=False, comment="""The body of the uri.

The uri scheme and body make up the base URI to find the dataset.

All paths in the dataset metadata can be computed relative to this.
(it is often the path of the source metadata file)

eg 'file:///g/data/datasets/LS8_NBAR/odc-metadata.yaml' or 'ftp://eo.something.com/dataset'
'file' is a scheme, '///g/data/datasets/LS8_NBAR/odc-metadata.yaml' is a body.""")
    added = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="when added")
    added_by = Column(Text, server_default=func.current_user(), nullable=False, comment="added by whom")
    archived = Column(DateTime(timezone=True), default=None, nullable=True,
                      comment="when archived, null for the active location")
    uri = column_property(uri_scheme + ':' + uri_body)
    dataset = relationship("Dataset")


SelectedDatasetLocation = aliased(DatasetLocation, name="sel_loc")


@orm_registry.mapped
class DatasetSource:
    __tablename__ = "dataset_lineage"
    __table_args__ = (
        _core.METADATA,
        PrimaryKeyConstraint('dataset_ref', 'classifier'),
        UniqueConstraint('source_dataset_ref', 'dataset_ref'),
        {
            "schema": sql.SCHEMA_NAME,
            "comment": "Represents a source-lineage relationship between two datasets"
        }
    )
    dataset_ref = Column(postgres.UUID(as_uuid=True), nullable=False, index=True,
                         comment="The downstream derived dataset produced from the upstream source dataset.")
    source_dataset_ref = Column(
        postgres.UUID(as_uuid=True), nullable=False, index=True,
        comment="An upstream source dataset that the downstream derived dataset was produced from."
    )
    classifier = Column(String, nullable=False, comment="""An identifier for this source dataset.
E.g. the dataset type ('ortho', 'nbar'...) if there's only one source of each type, or a datestamp
for a time-range summary.""")
