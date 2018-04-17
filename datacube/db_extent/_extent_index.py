import logging
import warnings
from shapely.geometry import shape, asShape, mapping
from shapely.ops import cascaded_union
import shapely.wkt as wkt
from datacube import Datacube
from datacube.utils.geometry import CRS, BoundingBox
from pandas import period_range, PeriodIndex, Period, Timestamp, DatetimeIndex
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select
from datacube.drivers.postgres._core import SCHEMA_NAME
from datacube.index import Index
from datacube.drivers.postgres import PostgresDb
from multiprocessing import Pool
import uuid
from datetime import datetime, timezone
import json
from functools import reduce

_LOG = logging.getLogger(__name__)
# pandas style time period frequencies
DEFAULT_FREQ = 'M'
DEFAULT_PER_PROCESS_FREQ = 'D'
DEFAULT_PROJECTION_BOUNDS = 'EPSG:4326'

# database env for datasets
DATASET_ENV = 'default'
# database env for extent data
EXTENT_ENV = 'dev'

# multiprocessing or threading pool size
POOL_SIZE = 31


def extent_per_period(dc, product, period, projection=None):
    """
    Computes the extent for a given period. If projection parameter is absent
    no projection is applied
    :param Datacube dc: A Datacube object
    :param str product: product name
    :param Period period: A pandas period object
    :param str projection: A projection string
    :return: A shapely geometry. If the extent is empty a shapely empty geometry object is returned
    """
    datasets = dc.find_datasets_lazy(product=product, time=(period.start_time, period.end_time))
    if not datasets:
        return wkt.loads('MULTIPOLYGON EMPTY')
    if projection:
        extents = [asShape(dataset.extent.to_crs(CRS(projection))) for dataset in datasets if dataset.extent]
    else:
        extents = [asShape(dataset.extent) for dataset in datasets if dataset.extent]
    return cascaded_union(extents) if extents else wkt.loads('MULTIPOLYGON EMPTY')


class ComputeChunk(object):
    def __init__(self, product, hostname, port, database, username, compute, projection=None):
        """
        Perform initialization of product variable and variables required
        for the recreation of datacube objects. Intended for multi-processes.
        :param str product: name of the product
        :param hostname: Host name of the datacube postgres db to extract extents
        :param port: port of the datacube postgres db to extract extents
        :param database: name of the database, i.e. 'datacube'
        :param username: username to access the database
        """
        # globals for multi-processes
        self._hostname = hostname
        self._port = port
        self._database = database
        self._username = username
        self._product = product
        self._compute = compute
        self._projection = projection

    def _set_datacube(self):
        """
        Creates a Datacube object from host, database info, and user info. Datacube objects needed
        to be created per process (They cannot be pickled!)
        :return Datacube: A Datacube object
        """
        db = PostgresDb.create(hostname=self._hostname, port=self._port,
                               database=self._database, username=self._username)
        index = Index(db)
        return Datacube(index=index)

    def __call__(self, period):
        """
        Implements an extent query to be executed by a multiprocess
        :param Period period: a pandas period object specifying time range
        :return: union of extents as computed by shapely cascaded union
        """

        return self._compute(dc=self._set_datacube(), product=self._product,
                             period=period, projection=self._projection)


class ExtentIndex(object):
    """
    Parses and executes queries to store and get various extent information from the database
    """
    def __init__(self, hostname, port, database, username, extent_index):
        """
        There are two sets of arguments: to re-create datacube object
        and an Index object linking to extent tables
        :param hostname: Host name of the datacube postgres db to extract extents
        :param port: port name of the datacube postgres db to extract extents
        :param database: database name of the datacube postgres db to extract extents
        :param username: user name of the datacube postgres db to extract extents
        :param Index extent_index: datacube Index object linking to extent tables
        """
        # We need the following info for multi-processes to create Datacube objects
        self._hostname = hostname
        self._port = port
        self._database = database
        self._username = username

        # Access to tables
        self._extent_index = extent_index
        self._engine = create_engine(extent_index.url, pool_recycle=240, client_encoding='utf8')
        self._conn = self._engine.connect()
        meta = MetaData(self._engine, schema=SCHEMA_NAME)
        meta.reflect(bind=self._engine, only=['extent', 'extent_meta', 'product_bounds'], schema=SCHEMA_NAME)
        self._extent_table = meta.tables[SCHEMA_NAME+'.extent']
        self._extent_meta_table = meta.tables[SCHEMA_NAME+'.extent_meta']
        self._bounds_table = meta.tables[SCHEMA_NAME + '.product_bounds']
        self._dataset_type_table = meta.tables[SCHEMA_NAME+'.dataset_type']

    @property
    def _loading_datacube(self):
        """
        Creates a Datacube object from host, database info, and user info. Datacube objects needed
        to be created per process
        :return: Datacube object
        """
        db = PostgresDb.create(hostname=self._hostname, port=self._port,
                               database=self._database, username=self._username)
        index = Index(db)
        return Datacube(index=index)

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

    def get_dataset_type_ref(self, product_name):
        """
        Find the dataset_type id corresponding to product_name
        :param str product_name:
        :return: dataset_type id
        """
        dataset_type_query = select([self._dataset_type_table.c.id.label('id')]).\
            where(self._dataset_type_table.c.name == product_name)
        with self._engine.begin(close_with_result=True) as conn:
            dataset_type_row = conn.execute(dataset_type_query).fetchone()
            type_id = dataset_type_row['id'] if dataset_type_row else None
            return type_id

    def _get_extent_meta_row(self, dataset_type_ref, offset_alias):
        """
        Extract a row corresponding to dataset_type id and offset_alias from extent_meta table
        :param dataset_type_ref: dataset type id
        :param str offset_alias: Pandas style offset period string
        :return:
        """
        extent_meta_query = select([self._extent_meta_table.c.id.label('id'),
                                    self._extent_meta_table.c.start.label('start'),
                                    self._extent_meta_table.c.end.label('end')]).\
            where((dataset_type_ref == self._extent_meta_table.c.dataset_type_ref) &
                  (offset_alias == self._extent_meta_table.c.offset_alias))
        with self._engine.begin(close_with_result=True) as conn:
            result = conn.execute(extent_meta_query).fetchone()
            row = {'id': result['id'], 'start': result['start'], 'end': result['end']}
            return row

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

        extent_query = select([self._extent_table.c.geometry.label('geometry')]).\
            where(self._extent_table.c.id == extent_uuid.hex)
        with self._engine.begin(close_with_result=True) as conn:
            extent_row = conn.execute(extent_query).fetchone()
            if extent_row:
                geo = extent_row['geometry']
            else:
                raise KeyError("Corresponding extent record does not exist in the extent table")
            return geo

    def _do_insert_query(self, dataset_type_ref, start, offset_alias, extent):
        """
        Insert or Update an extent record corresponding to dataset_type id, start time, and offset
        :param dataset_type_ref: dataset_type id
        :param datetime start: start time
        :param str offset_alias: pandas style offset alias string
        :param extent: new extent
        """

        # compute the id (i.e. uuid) from dataset_type_ref, start, and offset
        extent_uuid = ExtentIndex._compute_uuid(dataset_type_ref, start, offset_alias)

        # See whether an entry already exists in extent
        extent_query = select([self._extent_table.c.id.label('id')]).\
            where(self._extent_table.c.id == extent_uuid.hex)
        conn = self._engine.connect()
        extent_row = conn.execute(extent_query).fetchone()
        if extent_row:
            # Update the existing entry
            update = self._extent_table.update().\
                where(self._extent_table.c.id == extent_row['id']).\
                values(geometry=mapping(extent))
            conn.execute(update)
        else:
            # Insert a new entry
            ins = self._extent_table.insert().values(id=extent_uuid.hex, dataset_type_ref=dataset_type_ref,
                                                     start=start, offset_alias=offset_alias, geometry=mapping(extent))
            conn.execute(ins)
        conn.close()

    def _store_one(self, product_name, dataset_type_ref, period, offset_alias,
                   offset_pool=DEFAULT_PER_PROCESS_FREQ, projection=None):
        """
        Store a record in extent table corresponding to a given (period, offset_alias)
        using multiprocessing pools. Each process execute extent compute corresponding to a offset
        specified by offset_pool
        :param str product_name: Name of a product
        :param dataset_type_ref: dataset_type id of the product
        :param Period period: A pandas style Period object holding a time range
        :param str offset_alias: A pandas style offset alias string
        :param offset_pool: A pandas style offset alias indicating the time range of a query per process
        :param projection: Geo spacial projection to be applied
        """
        # Compute full extent using multiprocess pools
        pool = Pool(processes=POOL_SIZE)
        period_list = PeriodIndex(start=period.start_time, end=period.end_time, freq=offset_pool)
        extent_list = pool.map(ComputeChunk(product=product_name, hostname=self._hostname,
                                            port=self._port, database=self._database,
                                            username=self._username, compute=extent_per_period,
                                            projection=projection), period_list)
        # Filter out extents of NoneType
        extent_list = [extent for extent in extent_list if extent]

        full_extent = cascaded_union(extent_list) if extent_list else None

        # Format the start time to year-month format
        start_time = datetime(year=period.start_time.year, month=period.start_time.month, day=period.start_time.day)
        # Insert the full extent into extent table
        self._do_insert_query(dataset_type_ref, start_time, offset_alias, full_extent)

    def _store_many(self, product_name, dataset_type_ref, start, end, offset_alias, projection=None):
        """
        Store extent records correspond to each period of length offset within start and end
        :param str product_name: product name
        :param dataset_type_ref: product_type id
        :param datetime start: Start time of sequence of offsets
        :param datetime end: End time of sequence of offsets
        :param offset_alias: Length of period as a pandas style offset alias
        :param projection: Geo spacial projection to be applied
        """

        idx = period_range(start=start, end=end, freq=offset_alias)
        for period in idx:
            self._store_one(product_name=product_name, dataset_type_ref=dataset_type_ref,
                            period=period, offset_alias=offset_alias, projection=projection)

    def store_extent(self, product_name, start, end, offset_alias=None, projection=None):
        """
        store product extents to the database for each time period indicated by
        offset alias within the specified time range
        :param product_name: name of the product
        :param start: start time preferably in datetime type of extent computation and storage
        :param end: end time preferably in datetime type of extent computation and storage
        :param offset_alias: pandas style offset alias string indicating the length of each extent record in time
        :param projection: Geo spacial projection to be applied
        """
        if not offset_alias:
            offset_alias = DEFAULT_FREQ

        # Parse the time arguments
        start = ExtentIndex._parse_time(start)
        end = ExtentIndex._parse_time(end)

        # Lets get the dataset_type id
        dataset_type_ref = self.get_dataset_type_ref(product_name)

        if dataset_type_ref:
            conn = self._engine.connect()
            # Now we are ready to get the extent_metadata id
            extent_meta_row = self._get_extent_meta_row(dataset_type_ref, offset_alias)
            if extent_meta_row:
                if start < extent_meta_row['start'] or end > extent_meta_row['end']:
                    # Got to update meta data
                    update = self._extent_meta_table.update(). \
                        where(self._extent_meta_table.c.id == extent_meta_row['id']). \
                        values(start=start, end=end, projection=projection)
                    conn.execute(update)
            else:
                # insert a new meta entry
                ins = self._extent_meta_table.insert().values(dataset_type_ref=dataset_type_ref,
                                                              start=start, end=end,
                                                              offset_alias=offset_alias,
                                                              projection=projection)
                conn.execute(ins)
            conn.close()
            # Time to insert/update new extent data
            self._store_many(product_name=product_name, dataset_type_ref=dataset_type_ref,
                             start=start, end=end, offset_alias=offset_alias, projection=projection)
        else:
            raise KeyError("dataset_type_ref does not exist")

    def _store_bounds_record(self, dataset_type_ref, lower, upper, bounds, projection):
        """
        Store a record in the products_bounds table. It raises KeyError exception if product name is not
        found in the dataset_type table. The stored values are upper and lower bounds of time, axis aligned
        spacial bounds, and spacial projection used.
        :param str product_name: The name of the product
        :param datetime lower: The lower time bound
        :param datetime upper: The upper time bound
        :param BoundingBox bounds: The spacial bounds
        :param str projection: The projection used
        :return:
        """

        bounds_json = {'left': bounds.left, 'bottom': bounds.bottom, 'right': bounds.right, 'top': bounds.top}

        conn = self._engine.connect()

        # See whether an entry exists in product_bounds
        bounds_query = select([self._bounds_table.c.id.label('id')]).\
            where(self._bounds_table.c.dataset_type_ref == dataset_type_ref)
        bounds_row = conn.execute(bounds_query).fetchone()
        if bounds_row:
            # Update the existing entry
            update = self._bounds_table.update().\
                where(self._bounds_table.c.id == bounds_row['id']).\
                values(start=lower, end=upper, bounds=json.dumps(bounds_json), projection=projection)
            conn.execute(update)
        else:
            # Insert a new entry
            ins = self._bounds_table.insert().values(dataset_type_ref=dataset_type_ref,
                                                     start=lower, end=upper,
                                                     bounds=json.dumps(bounds_json), projection=projection)
            conn.execute(ins)
        conn.close()

    @staticmethod
    def _empty_box(bound):
        """
        Check whether the BoundingBox is empty
        :param BoundingBox bound: a BoundingBox
        :return bool:
        """
        return bound.left is None or bound.bottom is None or bound.right is None or bound.top is None

    @staticmethod
    def _bounds_union(bound1, bound2):
        """
        Computes the union of two given spacial bounds
        :param BoundingBox bound1: A axis aligned bound
        :param BoundingBox bound2: A axis aligned bound
        :return BoundingBox: The union of the given bounds
        """
        if ExtentIndex._empty_box(bound1):
            return bound2
        elif ExtentIndex._empty_box(bound2):
            return bound1
        else:
            return BoundingBox(left=min(bound1.left, bound2.left),
                               bottom=min(bound1.bottom, bound2.bottom),
                               right=max(bound1.right, bound2.right),
                               top=max(bound1.top, bound2.top))

    def _compute_bounds_with_hints(self, product, time_hints, projection=None):
        """
        Use the given time period as a hint to find the bounds. If actual bounds falls outside the given
        time limits the values computed will not make sense
        :param str product: Product name
        :param tuple time_hints: A tuple containing the begin and end of a period
        :param str projection: A projection string
        :return tuple : min_time, max_time, bounding box
        """
        def _cool_min(a, b):
            if not a:
                return b
            elif not b:
                return a
            else:
                return min(a, b)

        def _cool_max(a, b):
            if not a:
                return b
            elif not b:
                return a
            else:
                return max(a, b)

        pool = Pool(processes=POOL_SIZE)
        period_list = PeriodIndex(start=time_hints[0], end=time_hints[1], freq='1Y')
        bounds_list = pool.map(ComputeChunk(product=product, hostname=self._hostname,
                                            port=self._port, database=self._database,
                                            username=self._username, compute=ExtentIndex._compute_bounds,
                                            projection=projection), period_list)

        # Aggregate time min, time max, and bounds
        return reduce(lambda x, y: (_cool_min(x[0], y[0]), _cool_max(x[1], y[1]),
                                    ExtentIndex._bounds_union(x[2], y[2])), bounds_list)

    @staticmethod
    def _compute_bounds(dc, product, projection=None, period=None):
        """
        Computes the min, max time bounds and spacial bounds
        :param Datacube dc: A datacube object
        :param str product: product name
        :param str projection: a projection string
        :param Period period: A pandas Period object
        :return tuple: min time, max, time, and bounding box
        """
        if period:
            datasets = dc.find_datasets_lazy(product=product, time=(period.start_time, period.end_time))
        else:
            datasets = dc.find_datasets_lazy(product=product)
        try:
            first = next(datasets)
            lower, upper = first.time.begin, first.time.end
            bounds = first.extent.boundingbox if not projection else first.extent.to_crs(CRS(projection)).boundingbox
        except StopIteration:
            return None, None, BoundingBox(left=None, bottom=None, right=None, top=None)
        for dataset in datasets:
            if dataset.time.begin < lower:
                lower = dataset.time.begin
            if dataset.time.end > upper:
                upper = dataset.time.end
            if not projection:
                bounds = ExtentIndex._bounds_union(bounds, dataset.extent.boundingbox)
            else:
                bounds = ExtentIndex._bounds_union(bounds, dataset.extent.to_crs(CRS(projection)).boundingbox)
        return lower, upper, bounds

    def _store_bounds(self, product_name, projection=None, time_hints=None):
        """
        Store a bounds record in the product_bounds table. It computes max, min bounds in the projected space
        using dataset extents. If projection is not specified, it is assumed that CRS is constant product wide else
        spacial bounds computed may not make sense. It stores time bounds as well. It does not validate projection
        parameter
        :param product_name:
        :param projection:
        :return:
        """

        # Get the dataset_type_ref
        dataset_type_ref = self.get_dataset_type_ref(product_name)
        if not dataset_type_ref:
            raise KeyError("dataset_type_ref does not exist")

        if time_hints:
            lower, upper, bounds = self._compute_bounds_with_hints(product=product_name,
                                                                   time_hints=time_hints,
                                                                   projection=projection)
        else:
            lower, upper, bounds = ExtentIndex._compute_bounds(dc=self._loading_datacube, product=product_name,
                                                               projection=projection)

        self._store_bounds_record(dataset_type_ref=dataset_type_ref, lower=lower,
                                  upper=upper, bounds=bounds, projection=projection)

    def store_bounds(self, product_name, projection=None):
        """
        Store a bounds record in the product_bounds table. It computes max, min bounds in the projected space
        using dataset extents. If projection is not specified, it is assumed that CRS is constant product wide else
        spacial bounds computed may not make sense. It stores time bounds as well
        :param str product_name: The name of the product
        :param projection: The projection to be used when computing bounds of extent
        """
        if not projection:
            warnings.warn("The 'projection' parameter is not specified. It is assumed that"
                          "'CRS' is constant product wide, else bounds computed may not make sense",
                          RuntimeWarning)

        self._store_bounds(product_name, projection)

    def get_extent_yearly(self, dataset_type_ref, year_start, year_end):
        """
        Return yearly extents that correspond to a dataset_type id
        :param dataset_type_ref: dataset_type id
        :param integer year_start: integer indicating from which year to start queries
        :param integer year_end: integer indicating from which year to end queries
        :return: generator of extents
        """

        for year in range(year_start, year_end + 1):
            start = datetime(year=year, month=1, day=1)
            yield self.get_extent_direct(start=start, offset_alias='1Y', dataset_type_ref=dataset_type_ref)

    def get_extent_monthly(self, dataset_type_ref, start, end):
        """
        Return monthly extents that correspond to a dataset_type id
        :param dataset_type_ref: dataset_type id
        :param start: a time indicating the start of a sequence of months ( the first whole month on
        or after this date and within the given time range will be the first month )
        :param end: a time indicating the end of the sequence ( the last whole month on or before this
        date and within the given time range will be the last month of the sequence
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
            yield self.get_extent_direct(start=month, offset_alias='1M', dataset_type_ref=dataset_type_ref)

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

    def get_extent(self, product_name, start, end):
        """
        The general query function to compute extent within start and end
        :param str product_name: product name
        :param start: An object indicating the start time stamp preferably of type datetime
        :param end: An object indicating the end time stamp preferably of type datetime
        :return: total extent
        """
        # Parse the time arguments
        start = ExtentIndex._parse_time(start)
        end = ExtentIndex._parse_time(end)

        dataset_type_ref = self.get_dataset_type_ref(product_name)
        extent = None
        if dataset_type_ref:
            front, mid, back = ExtentIndex._split_time_range(start, end)
            if front:
                for et in self.get_extent_monthly(dataset_type_ref, front.start_time, front.end_time):
                    extent = ExtentIndex._compute_extent_union(extent, shape(et))
            if mid:
                for et in self.get_extent_yearly(dataset_type_ref, mid.start_time, mid.end_time):
                    extent = ExtentIndex._compute_extent_union(extent, shape(et))
            if back:
                for et in self.get_extent_monthly(dataset_type_ref, back.start_time, back.end_time):
                    extent = ExtentIndex._compute_extent_union(extent, shape(et))
        return extent

    def get_extent_direct(self, start, offset_alias, product_name=None, dataset_type_ref=None):
        """
        A fast direct query of extent specified by either product name or dataset_type, start, and offset
        :param start: An object indicating the start time stamp preferably of type datetime
        :param offset_alias: A pandas style offset alias string
        :param product_name: name of the product
        :param dataset_type_ref: dataset_type id of the product
        :return: total extent if not found a KeyError exception will be raised
        """

        start = ExtentIndex._parse_time(start)
        if not dataset_type_ref:
            dataset_type_ref = self.get_dataset_type_ref(product_name)
        if dataset_type_ref:
            return self._get_extent_row(dataset_type_ref, start, offset_alias)
        else:
            raise KeyError("Corresponding dataset_type_ref does not exist")

    def get_bounds(self, product_name):
        """
        Returns a bounds record corresponding to a given product name
        :param str product_name: Name of a product
        :return sqlalchemy.engine.result.RowProxy: a row corresponding to product name, if exists otherwise
        raise KeyError exception
        """
        dataset_type_ref = self.get_dataset_type_ref(product_name)
        if dataset_type_ref:
            bounds_query = select([self._bounds_table.c.dataset_type_ref.label('dataset_type_ref'),
                                   self._bounds_table.c.start.label('start'),
                                   self._bounds_table.c.end.label('end'),
                                   self._bounds_table.c.bounds.label('bounds'),
                                   self._bounds_table.c.projection.label('projection')]). \
                where(self._bounds_table.c.dataset_type_ref == dataset_type_ref)
            bounds_row = self._conn.execute(bounds_query).fetchone()
            if bounds_row:
                return bounds_row
            else:
                raise KeyError("A bounds record corresponding to {} does not exist".format(product_name))
        else:
            raise KeyError("A dataset_type corresponding to {} does not exist".format(product_name))


if __name__ == '__main__':
    # Get the Connections to the databases
    EXTENT_DB = PostgresDb.create(hostname='agdcdev-db.nci.org.au', database='datacube', port=6432, username='aj9439')
    EXTENT_IDX = ExtentIndex(hostname='agdc-db.nci.org.au', database='datacube', port=6432,
                             username='aj9439', extent_index=Index(EXTENT_DB))

    # load into extents table
    EXTENT_IDX.store_extent(product_name='ls8_nbar_albers', start='2017-01',
                            end='2017-02', offset_alias='1M', projection='EPSG:4326')
    EXTENT_IDX.store_bounds(product_name='ls8_nbar_albers', projection='EPSG:4326')
