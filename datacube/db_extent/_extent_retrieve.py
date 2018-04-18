import logging
import warnings
from shapely.geometry import asShape, mapping
from shapely.ops import cascaded_union
import shapely.wkt as wkt
from datacube.utils.geometry import CRS, Geometry
from pandas import Period, Timestamp, DatetimeIndex
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select
from datacube.drivers.postgres._core import SCHEMA_NAME
from datacube.index import Index
from datacube.drivers.postgres import PostgresDb
import uuid
from datetime import datetime, timezone

_LOG = logging.getLogger(__name__)
# pandas style time period frequencies
DEFAULT_FREQ = 'M'
DEFAULT_PER_PROCESS_FREQ = 'D'

# default projection string
DEFAULT_PROJECTION_BOUNDS = 'EPSG:4326'

# database env for datasets
DATASET_ENV = 'default'
# database env for extent data
EXTENT_ENV = 'dev'

# multiprocessing or threading pool size
POOL_SIZE = 31

# SQLAlchemy constants
POOL_RECYCLE_TIME_SEC = 240


class ExtentMetadata(object):
    """ Pre-load extent meta data"""
    def __init__(self, index):
        # Create extent_meta table object
        self._engine = create_engine(index.url, pool_recycle=POOL_RECYCLE_TIME_SEC, client_encoding='utf8')
        meta = MetaData(self._engine, schema=SCHEMA_NAME)
        meta.reflect(bind=self._engine, only=['extent', 'extent_meta', 'product_bounds'], schema=SCHEMA_NAME)
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
                                    self._extent_meta_table.c.projection])
        with self._engine.begin() as conn:
            result = {}
            for item in conn.execute(extent_meta_query).fetchall():
                result[(item['dataset_type_ref'], item['offset_alias'])] = {'id': item['id'],
                                                                            'start': item['start'],
                                                                            'end': item['end'],
                                                                            'projection': item['projection']}
        return result


class ExtentIndex(object):
    """
    Parses and executes queries to get various extent and bounds information from the database
    """
    def __init__(self, datacube_index):
        """
        :param Index datacube_index: datacube Index object linking to extent tables
        """

        # Access to tables
        self._extent_index = datacube_index
        from sqlalchemy.pool import NullPool
        self._engine = create_engine(datacube_index.url, poolclass=NullPool, client_encoding='utf8')
        # self._engine = create_engine(extent_index.url, pool_recycle=POOL_RECYCLE_TIME_SEC,
        #                              pool_pre_ping=True, client_encoding='utf8')
        self._conn = self._engine.connect()
        meta = MetaData(self._engine, schema=SCHEMA_NAME)
        meta.reflect(bind=self._engine, only=['extent', 'extent_meta', 'product_bounds'], schema=SCHEMA_NAME)
        self._extent_table = meta.tables[SCHEMA_NAME+'.extent']
        self._extent_meta_table = meta.tables[SCHEMA_NAME+'.extent_meta']
        self._bounds_table = meta.tables[SCHEMA_NAME + '.product_bounds']
        self._dataset_type_table = meta.tables[SCHEMA_NAME+'.dataset_type']

        # Metadata pre-loads
        self.metadata = ExtentMetadata(datacube_index).items

    @staticmethod
    def _compute_uuid(dataset_type_ref, start, offset_alias):
        """
        compute the id (i.e. uuid) from dataset_type_ref, start, and offset
        :param dataset_type_ref: An id field value of dataset_type table
        :param datetime start: A time stamp
        :param str offset_alias: The string representation of pandas offset alias
        :return UUID: a uuid reflecting a hash value from dataset_type id, start timestamp, and offset_alias
        """

        name_space = uuid.UUID('{'+format(2**127+dataset_type_ref, 'x')+'}')
        start_time = str(start.year) + str(start.month)
        return uuid.uuid3(name_space, start_time + offset_alias)

    @staticmethod
    def _parse_time(time_stamp):
        """
        Parses a time representation into a datetime object
        :param time_stamp: A time value
        :return datetime: datetime representation of given time value
        """
        if not isinstance(time_stamp, datetime):
            t = Timestamp(time_stamp)
            time_stamp = datetime(year=t.year, month=t.month, day=t.day, tzinfo=t.tzinfo)
        if not time_stamp.tzinfo:
            system_tz = datetime.now(timezone.utc).astimezone().tzinfo
            return time_stamp.replace(tzinfo=system_tz)
        return time_stamp

    def _get_extent_meta_row(self, dataset_type_ref, offset_alias):
        """
        Extract a row corresponding to dataset_type id and offset_alias from extent_meta table
        :param dataset_type_ref: dataset type id
        :param str offset_alias: Pandas style offset period string
        :return:
        """
        extent_meta_query = select([self._extent_meta_table]).\
            where((dataset_type_ref == self._extent_meta_table.c.dataset_type_ref) &
                  (offset_alias == self._extent_meta_table.c.offset_alias))
        with self._engine.begin() as conn:
            return conn.execute(extent_meta_query).fetchone()

    def _get_extent_row(self, dataset_type_ref, start, offset_alias):
        """
        Extract and return extent information corresponding to dataset type, start, and offset_alias
        :param dataset_type_ref: dataset type id
        :param datetime start: datetime representation of start timestamp
        :param offset_alias: pandas style period string
        :return: extent field
        """

        # compute the id (i.e. uuid) from dataset_type_ref, start, and offset
        extent_uuid = ExtentIndex._compute_uuid(dataset_type_ref, start, offset_alias)

        extent_query = select([self._extent_table.c.geometry]).\
            where(self._extent_table.c.id == extent_uuid.hex)
        with self._engine.begin() as conn:
            extent_row = conn.execute(extent_query).fetchone()
            if extent_row:
                geo = extent_row['geometry']
            else:
                raise KeyError("Corresponding extent record does not exist in the extent table")
            return geo

    def reproject(self, extent, dataset_type_ref, offset_alias, projection):
        """
        Re-project the extent from the stored projection given in extent metadata to requested projection
        :param extent: a geojson like multi-polygon
        :param dataset_type_ref: dataset_type
        :param offset_alias: pandas style offset alias string
        :param projection: requested projection string
        :return:
        """

        metadata = self.metadata[(dataset_type_ref, offset_alias)]
        # Project to the requested projection
        if projection:
            return Geometry(extent, CRS(metadata['projection'])).to_crs(CRS(projection)).__geo_interface__
        else:
            return extent

    def get_extent_yearly(self, dataset_type_ref, year_start, year_end, projection=None):
        """
        Return yearly extents that correspond to a dataset_type id
        :param dataset_type_ref: dataset_type id
        :param integer year_start: integer indicating from which year to start queries
        :param integer year_end: integer indicating from which year to end queries
        :param string projection: The requested projection
        :return: generator of extents
        """

        for year in range(year_start, year_end + 1):
            start = datetime(year=year, month=1, day=1)
            yield self.get_extent_direct(start=start, offset_alias='1Y',
                                         dataset_type_ref=dataset_type_ref, projection=projection)

    def get_extent_monthly(self, dataset_type_ref, start, end, projection=None):
        """
        Return monthly extents that correspond to a dataset_type id
        :param dataset_type_ref: dataset_type id
        :param start: a time indicating the start of a sequence of months ( the first whole month on
        or after this date and within the given time range will be the first month )
        :param end: a time indicating the end of the sequence ( the last whole month on or before this
        date and within the given time range will be the last month of the sequence
        :param projection: The requested projection
        :return: generator of extents
        """
        # All the months where the first day of the month is within start and end
        # Parse the time arguments
        start = ExtentIndex._parse_time(start)
        end = ExtentIndex._parse_time(end)

        # There seems to be a nanosecond level precision that is discarded by DatetimeIndex.
        # This seems to be acceptable and therefore, lets ignore this warning
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            dti = DatetimeIndex(start=start, end=end, freq='1MS')

        for month in dti:
            yield self.get_extent_direct(start=month, offset_alias='1M',
                                         dataset_type_ref=dataset_type_ref, projection=projection)

    @staticmethod
    def _split_time_range(start, end):
        """
        Splits the time range into maximum number of whole years within the range and remaining months at
        the front and back
        :param datetime start: start of the time range
        :param datetime end: end of the time range
        :return Period, Period, period: Three pandas Period objects holding front, mid, and back sections
        of the time range
        """
        # Assume up to monthly resolution
        if start.month == 1:
            year_start = start.year
            period_month_front = None
        else:
            year_start = start.year + 1
            period_month_front = Period(year=start.year, month=start.month, freq=str(12 - start.month + 1)+'M')
        if end.month == 12:
            year_end = end.year
            period_month_back = None
        else:
            year_end = end.year - 1
            period_month_back = Period(year=end.year, freq=str(end.month)+'M')
        if year_start <= year_end:
            period_year = Period(year=year_start, freq=str(year_end-year_start+1)+'Y')
        else:
            period_year = None
        return period_month_front, period_year, period_month_back

    @staticmethod
    def _compute_extent_union(extent1, extent2):
        """
        Compute shapely union of two extent objects and return it. If both arguments are empty or none,
        then return None
        :param extent1: A shapely multi polygon
        :param extent2: A shapely multi polygon
        :return: The cascaded union of the parameters
        """
        if bool(extent1) & bool(extent2):
            return cascaded_union([extent1, extent2])
        elif extent1:
            return extent1
        elif extent2:
            return extent2
        else:
            return None

    def get_extent(self, product_name, start, end, projection=None):
        """
        The general query function to compute extent within start and end
        :param str product_name: product name
        :param start: An object indicating the start time stamp preferably of type datetime
        :param end: An object indicating the end time stamp preferably of type datetime
        :param str projection: The requested projection string
        :return: total extent
        """
        # Parse the time arguments
        start = ExtentIndex._parse_time(start)
        end = ExtentIndex._parse_time(end)
        extent = wkt.loads('MULTIPOLYGON EMPTY')
        dataset_type_ref = self._extent_index.products.get_by_name(product_name).id
        if dataset_type_ref:
            front, mid, back = ExtentIndex._split_time_range(start, end)
            if front:
                extents = [asShape(et) for et in self.get_extent_monthly(dataset_type_ref,
                                                                         front.start_time, front.end_time)]
                extent = cascaded_union(extents)
            if back:
                extents = [asShape(et) for et in self.get_extent_monthly(dataset_type_ref,
                                                                         back.start_time, back.end_time)]
                extent = cascaded_union(extents)
            extent_monthly = self.reproject(extent, dataset_type_ref, '1M', projection)
            if mid:
                extents = [asShape(et) for et in self.get_extent_yearly(dataset_type_ref,
                                                                        mid.start_time, mid.end_time)]
                extent_yearly = cascaded_union(extents)
                return cascaded_union([extent_monthly, self.reproject(extent_yearly,
                                                                      dataset_type_ref, '1Y', projection)])
            return extent_monthly
        else:
            raise KeyError("Corresponding dataset_type_ref does not exist")

    def get_extent_direct(self, start, offset_alias, product_name=None,
                          dataset_type_ref=None, projection=None):
        """
        A fast direct query of extent specified by either product name or dataset_type, start, and offset
        :param start: An object indicating the start time stamp preferably of type datetime
        :param offset_alias: A pandas style offset alias string
        :param product_name: name of the product
        :param dataset_type_ref: dataset_type id of the product
        :param projection: projection string of the request
        :return: total extent if not found a KeyError exception will be raised
        """

        start = ExtentIndex._parse_time(start)
        if not dataset_type_ref:
            dataset_type_ref = self._extent_index.products.get_by_name(product_name).id
        if dataset_type_ref:
            result = self._get_extent_row(dataset_type_ref, start, offset_alias)
            # Project to the requested projection
            return self.reproject(result, dataset_type_ref, offset_alias, projection)
        else:
            raise KeyError("Corresponding dataset_type_ref does not exist")

    def get_extent_with_geobox(self, start, offset_alias, product_name=None,
                               dataset_type_ref=None, projection=None, geobox=None):
        extent = self.get_extent_direct(start, offset_alias, product_name,
                                        dataset_type_ref, projection)
        return mapping(asShape(extent).intersection(geobox.extent)) if geobox else extent

    def get_bounds(self, product_name):
        """
        Returns a bounds record corresponding to a given product name
        :param str product_name: Name of a product
        :return sqlalchemy.engine.result.RowProxy: a row corresponding to product name, if exists otherwise
        raise KeyError exception
        """
        dataset_type_ref = self._extent_index.products.get_by_name(product_name).id
        if dataset_type_ref:
            bounds_query = select([self._bounds_table.c.dataset_type_ref,
                                   self._bounds_table.c.start,
                                   self._bounds_table.c.end,
                                   self._bounds_table.c.bounds,
                                   self._bounds_table.c.projection]). \
                where(self._bounds_table.c.dataset_type_ref == dataset_type_ref)
            bounds_row = self._conn.execute(bounds_query).fetchone()
            if bounds_row:
                return bounds_row
            else:
                raise KeyError("A bounds record corresponding to {} does not exist".format(product_name))
        else:
            raise KeyError("A dataset_type corresponding to {} does not exist".format(product_name))


if __name__ == '__main__':
    # ToDo These stuff are to be removed
    # Get the Connections to the databases
    EXTENT_DB = PostgresDb.create(hostname='agdcdev-db.nci.org.au', database='datacube', port=6432, username='aj9439')
    EXTENT_IDX = ExtentIndex(datacube_index=Index(EXTENT_DB))
