from sqlalchemy import create_engine
from datacube.drivers.postgres._schema import EXTENT, EXTENT_META, RANGES


if __name__ == '__main__':
    ENGINE = create_engine('postgresql://aj9439@agdcdev-db.nci.org.au:6432/datacube')
    with ENGINE.connect() as conn:
        TRANS = conn.begin()
        try:
            # Create the extent_meta table
            EXTENT_META.create(bind=conn)
            # Create the extent table
            EXTENT.create(bind=conn)
            # Create ranges table
            RANGES.create(bind=conn)
            TRANS.commit()
        except:  # pylint: disable=bare-except
            TRANS.rollback()
