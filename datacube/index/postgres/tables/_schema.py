# coding=utf-8
"""
Tables for indexing the datasets which were ingested into the AGDC.
"""
from __future__ import absolute_import

import logging

from sqlalchemy import ForeignKey, UniqueConstraint, PrimaryKeyConstraint, CheckConstraint, SmallInteger
from sqlalchemy import Table, Column, Integer, String, DateTime, Boolean, Float
from sqlalchemy import text
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.sql import func

from . import _core, _sql

_LOG = logging.getLogger(__name__)

METADATA_TYPE = Table(
    'metadata_type', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    Column('name', String, unique=True, nullable=False),

    Column('definition', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', _sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

DATASET_TYPE = Table(
    'dataset_type', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    # A name/label for this type (eg. 'ls7_nbar'). Specified by users.
    Column('name', String, unique=True, nullable=False),

    # All datasets of this type should contain these fields.
    # (newly-ingested datasets may be matched against these fields to determine the dataset type)
    Column('metadata', postgres.JSONB, nullable=False),

    # The metadata format expected (eg. what fields to search by)
    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),

    Column('definition', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', _sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

DATASET = Table(
    'dataset', _core.METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),

    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),
    Column('dataset_type_ref', None, ForeignKey(DATASET_TYPE.c.id), index=True, nullable=False),

    Column('metadata', postgres.JSONB, index=False, nullable=False),

    # Date it was archived. Null for active datasets.
    Column('archived', DateTime(timezone=True), default=None, nullable=True),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', _sql.PGNAME, server_default=func.current_user(), nullable=False),
)

DATASET_LOCATION = Table(
    'dataset_location', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), index=True, nullable=False),

    # The base URI to find the dataset.
    #
    # All paths in the dataset metadata can be computed relative to this.
    # (it is often the path of the source metadata file)
    #
    # eg 'file:///g/data/datasets/LS8_NBAR/agdc-metadata.yaml' or 'ftp://eo.something.com/dataset'
    # 'file' is a scheme, '///g/data/datasets/LS8_NBAR/agdc-metadata.yaml' is a body.
    Column('uri_scheme', String, nullable=False),
    Column('uri_body', String, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', _sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Date it was archived. Null for active locations.
    Column('archived', DateTime(timezone=True), default=None, nullable=True),

    UniqueConstraint('uri_scheme', 'uri_body', 'dataset_ref'),
)

# Link datasets to their source datasets.
DATASET_SOURCE = Table(
    'dataset_source', _core.METADATA,
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),

    # An identifier for this source dataset.
    #    -> Usually it's the dataset type ('ortho', 'nbar'...), as there's typically only one source
    #       of each type.
    Column('classifier', String, nullable=False),
    Column('source_dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),

    PrimaryKeyConstraint('dataset_ref', 'classifier'),
    UniqueConstraint('source_dataset_ref', 'dataset_ref'),
)


### S3-driver specific Tables



# The mapping between a AGDC dataset (single time slice, multiple bands) and
# band to an S3 dataset (multiple times, single bands)
# (dataset_ref,band) <-> s3_dataset_ref
# S3_dataset_mapping
#     datset_ref :: UUID
#     band :: String
#     S3_dataset_ref :: UUID
S3_DATASET_MAPPING = Table(
    's3_dataset_mapping', _core.METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),
    Column('band', String, nullable=False),
    Column('s3_dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),
    UniqueConstraint('dataset_ref', 'band')
)


# -- An S3 dataset: an N-dimensional single band
# -- An example record may look like:
# --  <uuid> | <ref string, defined elsewhere> | 'nir1' | [3,20,1024,1024] | [1,5,256,256] | 'i4' (i.e. int32_t)
# --         | ['height', 'time', 'lat', 'lon'] | [false, false, true, true] | [[140,0.05,191.2],[-27,-0.05,-78.2]]
# --         | [[50,100,150],[636419487, 637024619, 638406989, 641949039, 642554192, 643936587, 644713840,
#               ..., 645318986, 648083782, 648861033, 650243428]]
# S3_DATASET:
#   id :: UUID
#   dataset_key :: String   -- S3 object name without the chunk id
#   band :: String          -- The band contained in this dataset
#   macroshape :: [Integer] -- The integer dimensions of this datset
#   chunk_size :: [Integer] -- The default size of each sub-dataset chunk - allows
#                           -- the chunk_id to be a single number which maps into this N-dimensional structure
#   numpy_type :: String    -- The numpy type data is stored in, i4, f8 etc
#   dimensions :: [String]  -- dimension names which are present in this dataset
#   regular_dims :: [Bool]  -- for each dimension, contains a bool specifying whether that dimension is regular or not
#   regular_index   :: [[Double]] -- [min,step,max] for each index which has a True value in regular_dims
#   irregular_index :: [[Double]] -- the valid values for each irregular index, [[time1..timeN],[height1..heightM]],
#                                 -- this may need to change in the future to better support things
#                                 -- like times as timestamps
S3_DATASET = Table(
    's3_dataset', _core.METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),
    Column('dataset_key', String, nullable=False),
    Column('band', String, nullable=False),
    Column('macro_shape', postgres.ARRAY(Integer, dimensions=1), nullable=False),
    Column('chunk_size', postgres.ARRAY(Integer, dimensions=1), nullable=False),
    Column('numpy_type', String, nullable=False),
    Column('dimensions', postgres.ARRAY(String, dimensions=1), nullable=False),
    Column('regular_dims', postgres.ARRAY(Boolean, dimensions=1), nullable=False),
    Column('regular_index', postgres.ARRAY(Float(), dimensions=2), nullable=False),
    Column('irregular_index', postgres.ARRAY(Float(), dimensions=2), nullable=False)
)

# -- Data specific to each sub-datset chunk
# -- Example record:
# --    <uuid> | <uuid> | 'datacube-chunk-bucket-001' | <kS3 key, defined elsewhere> | 0 | 'nir1'
# --           | 'none' | [1,5,256,256] | [50, 636419487, 140, -27] | [50, 642554192, 152.8, -39.8]
# S3_sub_dataset:
#     S3_dataset_ref :: UUID    -- The parent dataset for this chunk
#     bucket :: String          -- The S3 bucket where the object is stored (so we can potentially
#                               -- shard based on bucket)
#     s3key :: String           -- The S3 object key (this with the bucket makes up the S3 URI)
#     chunk_id :: Integer       -- The (linear) index into the parent dataset for this chunk, see chunk_size
#     band :: String            -- The band for this chunk -- Is this needed?? The parent already defines this
#     compression_scheme :: String -- A string representing the compression scheme used on this chunk.
#                                  -- Allows for us to change scheme in the future for new data without
#                                  -- needing to re-compressing everything in a dataset
#     micro_shape :: [Integer]  -- The actual size of this chunk in all dimensions described in S3_DATASET,
#                               -- may be smaller than chunk_size above
#     index_min :: [Double]     -- For each dimension (regular or not) the min/max for that dimension within this chunk
#     index_max :: [Double]

S3_DATASET_CHUNK = Table(
    's3_dataset_chunk', _core.METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),
    Column('s3_dataset_ref', None, ForeignKey(S3_DATASET.c.id), nullable=False),
    Column('bucket', String, nullable=False),
    Column('chunk_id', Integer, nullable=False),
    Column('compression_scheme', String, nullable=False),
    Column('micro_shape', postgres.ARRAY(Integer, dimensions=1), nullable=False),
    Column('index_min', postgres.ARRAY(Float(), dimensions=1)),
    Column('index_max', postgres.ARRAY(Float(), dimensions=1))
)
