from sqlalchemy import create_engine, SmallInteger, String, DateTime
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.schema import Column, Table
from sqlalchemy.dialects import postgresql as postgres
from ._schema import DATASET_TYPE
from ._core import METADATA


EXTENT_META = Table('extent_meta', METADATA,
                    Column('id', SmallInteger, primary_key=True, autoincrement=True),
                    Column('dataset_type_ref', None, ForeignKey(DATASET_TYPE.c.id), nullable=False),
                    Column('start', DateTime(timezone=True), nullable=False),
                    Column('end', DateTime(timezone=True), nullable=False),
                    Column('offset_alias', String, nullable=False),
                    Column('crs', String, nullable=True),

                    UniqueConstraint('dataset_type_ref', 'offset_alias')
                   )

EXTENT = Table('extent', METADATA,
               Column('id', postgres.UUID(as_uuid=True), primary_key=True),
               Column('extent_meta_ref', None, ForeignKey(EXTENT_META.c.id), nullable=False),
               Column('start', DateTime(timezone=True), nullable=False),
               Column('geometry', postgres.JSONB, nullable=True),

               UniqueConstraint('extent_meta_ref', 'start')
              )

RANGES = Table('ranges', METADATA,
               Column('id', SmallInteger, primary_key=True, autoincrement=True),
               Column('dataset_type_ref', None, ForeignKey(DATASET_TYPE.c.id), nullable=False),
               Column('start', DateTime(timezone=True), nullable=False),
               Column('end', DateTime(timezone=True), nullable=False),
               Column('bounds', postgres.JSONB, nullable=True),
               Column('crs', String, nullable=True),
              )

if __name__ == '__main__':
    ENGINE = create_engine('postgresql://aj9439@agdcdev-db.nci.org.au:6432/datacube')
    # Create the extent_meta table
    EXTENT_META.create(bind=ENGINE)
    # Create the extent table
    EXTENT.create(bind=ENGINE)
    # Create ranges table
    RANGES.create(bind=ENGINE)
