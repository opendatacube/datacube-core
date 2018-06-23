from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select
from datacube.drivers.postgres._core import SCHEMA_NAME
from sqlalchemy.pool import NullPool
import uuid
from pandas import Timestamp
from datetime import datetime, date, timezone
import itertools


def peek_generator(iterable):
    """ See https://:stackoverflow.com/questions/661603/how-do-i-know-if-a-generator-is-empty-from-the-start """
    try:
        first = next(iterable)
    except StopIteration:
        return None
    return itertools.chain([first], iterable)


def parse_date(d):
    """
    Parses a time representation into a datetime object
    :param d: A time value
    :return datetime: datetime representation of given time value
    """
    if not isinstance(d, date):
        t = Timestamp(d)
        d = date(year=t.year, month=t.month, day=t.day)
    return d


def compute_uuid(dataset_type_ref, start, offset_alias):
    """
    compute the id (i.e. uuid) from dataset_type_ref, start, and offset
    :param dataset_type_ref: An id field value of dataset_type table
    :param datetime start: A time stamp
    :param str offset_alias: The string representation of pandas offset alias
    :return UUID: a uuid reflecting a hash value from dataset_type id, start timestamp, and offset_alias
    """

    name_space = uuid.UUID('{'+format(2**127+dataset_type_ref, 'x')+'}')
    start_time = str(start.year) + str(start.month) + str(start.day)
    return uuid.uuid3(name_space, start_time + offset_alias)


class ExtentMetadata(object):
    """ Pre-load extent meta data"""
    def __init__(self, index):
        # Create extent_meta table object
        self._engine = create_engine(index.url, poolclass=NullPool, client_encoding='utf8')
        meta = MetaData(self._engine, schema=SCHEMA_NAME)
        meta.reflect(bind=self._engine, only=['extent', 'extent_meta', 'ranges'], schema=SCHEMA_NAME)
        self._extent_meta_table = meta.tables[SCHEMA_NAME + '.extent_meta']

        # Load metadata
        # self items would be tuple (dataset_type_ref, offset_alias) indexed dictionary
        self.items = self._load_metadata()

    def _load_metadata(self):
        extent_meta_query = select([self._extent_meta_table.c.id,
                                    self._extent_meta_table.c.dataset_type_ref,
                                    self._extent_meta_table.c.start,
                                    self._extent_meta_table.c.end,
                                    self._extent_meta_table.c.offset_alias,
                                    self._extent_meta_table.c.crs])
        with self._engine.begin() as conn:
            result = {}
            for item in conn.execute(extent_meta_query).fetchall():
                result[(item['dataset_type_ref'], item['offset_alias'])] = {'id': item['id'],
                                                                            'start': item['start'],
                                                                            'end': item['end'],
                                                                            'crs': item['crs']}
        return result
