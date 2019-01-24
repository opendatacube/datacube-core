
from sqlalchemy import Table, Column, String, Integer, Float, ForeignKey, UniqueConstraint, Boolean, MetaData
from sqlalchemy.dialects import postgresql as postgres

from datacube.drivers.postgres._core import SQL_NAMING_CONVENTIONS, SCHEMA_NAME
from datacube.drivers.postgres._schema import DATASET

S3_METADATA = MetaData(naming_convention=SQL_NAMING_CONVENTIONS, schema=SCHEMA_NAME)

#: An S3 dataset: an N-dimensional single band
#: An example record may look like:
#:   <uuid> | <ref string, defined elsewhere> | 'nir1' | [3,20,1024,1024] | [1,5,256,256] | 'i4' (i.e. int32_t)
#:          | ['height', 'time', 'lat', 'lon'] | [false, false, true, true] | [[140,0.05,191.2],[-27,-0.05,-78.2]]
#:          | [[50,100,150],[636419487, 637024619, 638406989, 641949039, 642554192, 643936587, 644713840,
#:               ..., 645318986, 648083782, 648861033, 650243428]]
#: s3_dataset:
#:   id :: UUID
#:   dataset_key :: String         -- S3 object name without the chunk id
#:   base_name :: String           -- The macro array name
#:   band :: String                -- The band contained in this dataset
#:   bucket :: String              -- The S3 bucket where the object is stored (so we can potentially
#:                                 -- shard based on bucket)
#:   macroshape :: [Integer]       -- The integer dimensions of this datset
#:   chunk_size :: [Integer]       -- The default size of each sub-dataset chunk - allows
#:                                 -- the chunk_id to be a single number which maps into this N-dimensional structure
#:   numpy_type :: String          -- The numpy type data is stored in, i4, f8 etc
#:   dimensions :: [String]        -- dimension names which are present in this dataset
#:   regular_dims :: [Bool]        -- for each dimension, contains a bool specifying whether that dimension is regular
#:   regular_index   :: [[Double]] -- [min,step,max] for each index which has a True value in regular_dims
#:   irregular_index :: [[Double]] -- the valid values for each irregular index, [[time1..timeN],[height1..heightM]],
#:                                 -- this may need to change in the future to support things like times as timestamps
S3_DATASET = Table(
    's3_dataset', S3_METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),
    Column('base_name', String, nullable=False),
    Column('band', String, nullable=False),
    Column('bucket', String, nullable=False),
    Column('macro_shape', postgres.ARRAY(Integer, dimensions=1), nullable=False),
    Column('chunk_size', postgres.ARRAY(Integer, dimensions=1), nullable=False),
    Column('numpy_type', String, nullable=False),
    Column('dimensions', postgres.ARRAY(String, dimensions=1), nullable=False),
    Column('regular_dims', postgres.ARRAY(Boolean, dimensions=1), nullable=False),
    Column('regular_index', postgres.ARRAY(Float(), dimensions=2), nullable=False),
    Column('irregular_index', postgres.ARRAY(Float(), dimensions=2), nullable=False)
)

#: Data specific to each sub-datset chunk.
#: An example record may look like:
#:   <uuid> | <uuid> | 'datacube-chunk-bucket-001' | <kS3 key, defined elsewhere> | 0 | 'nir1'
#:          | 'none' | [1,5,256,256] | [50, 636419487, 140, -27] | [50, 642554192, 152.8, -39.8]
#: s3_dataset_chunk:
#:   s3_dataset_id :: UUID        -- The parent dataset for this chunk
#:   s3key :: String              -- The S3 object key (this with the bucket makes up the S3 URI)
#:   chunk_id :: Integer          -- The (linear) index into the parent dataset for this chunk, see chunk_size
#:   band :: String               -- The band for this chunk -- Is this needed?? The parent already defines this
#:   compression_scheme :: String -- A string representing the compression scheme used on this chunk.
#:                                -- Allows for us to change scheme in the future for new data without
#:                                -- needing to re-compressing everything in a dataset
#:   micro_shape :: [Integer]     -- The actual size of this chunk in all dimensions described in S3_DATASET,
#:                                -- may be smaller than chunk_size above
#:   index_min :: [Double]        -- For each dimension (regular or not) the min for that dimension within this chunk
#:   index_max :: [Double]        -- For each dimension (regular or not) the max for that dimension within this chunk
S3_DATASET_CHUNK = Table(
    's3_dataset_chunk', S3_METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),
    Column('s3_dataset_id', None, ForeignKey(S3_DATASET.c.id), nullable=False),
    Column('s3_key', String, nullable=False),
    Column('chunk_id', Integer, nullable=False),
    Column('compression_scheme', String, nullable=True),
    Column('micro_shape', postgres.ARRAY(Integer, dimensions=1), nullable=False),
    Column('index_min', postgres.ARRAY(Float(), dimensions=1)),
    Column('index_max', postgres.ARRAY(Float(), dimensions=1))
)

#: The mapping between an AGDC dataset (single time slice, multiple bands) and
#: band to an S3 dataset (multiple times, single band)
#:   (dataset_ref,band) <-> s3_dataset_id
#: s3_dataset_mapping
#:   datset_ref :: UUID    -- UUID of the AGDC dataset, matching a `dataset.id` record
#:   band :: String        -- The band contained in the S3 dataset
#:   s3_dataset_id :: UUID -- UUID of the S3 dataset, mapping a `s3_dataset.id` record
S3_DATASET_MAPPING = Table(
    's3_dataset_mapping', S3_METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),
    Column('band', String, nullable=False),
    Column('s3_dataset_id', None, ForeignKey(S3_DATASET.c.id), nullable=False),
    UniqueConstraint('dataset_ref', 'band', 's3_dataset_id')
)
