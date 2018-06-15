import logging
import warnings
from shapely.geometry import asShape, mapping
from shapely.ops import cascaded_union
import shapely.wkt as wkt
from datacube import Datacube
from datacube.utils.geometry import CRS, BoundingBox
from pandas import period_range, PeriodIndex, Period
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select
from datacube.drivers.postgres._core import SCHEMA_NAME
from datacube.index import Index
from datacube.drivers.postgres import PostgresDb
from multiprocessing import Pool
from datetime import datetime
import json
from functools import reduce
from datacube.db_extent import ExtentMetadata, compute_uuid, parse_time, peek_generator, ExtentIndex

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
    """
    This is a callable class designed for multiprocessors to initialize and subsequently execute
    periodic computation on a datacube of given database parameters.
    """
    def __init__(self, product, hostname, port, database, username, compute, projection=None):
        """
        Perform initialization of product variable and variables required
        for the recreation of datacube objects. Intended for multi-processes.

        :param str product: name of the product
        :param hostname: Host name of the datacube postgres db to extract extents
        :param port: port of the datacube postgres db to extract extents
        :param database: name of the database, i.e. 'datacube'
        :param username: username to access the database
        :param compute: A function to act on a product for a given period using a given projection
        :param projection: The projection to be used by the compute function
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

        # We need to release datacube resources after compute, so use a context manager
        with self._set_datacube() as dc:
            return self._compute(dc=dc, product=self._product, period=period, projection=self._projection)


class ExtentUpload(object):
    """
    Parses and executes queries to store and get various extent information from the database
    """
    def __init__(self, hostname, port, database, username, destination_index):
        """
        Arguments specifying the source and destination databases to use.

        There are two sets of arguments: to re-create datacube object
        and an Index object linking to extent tables

        :param hostname: Host name of the datacube postgres db to extract extents
        :param port: port name of the datacube postgres db to extract extents
        :param database: database name of the datacube postgres db to extract extents
        :param username: user name of the datacube postgres db to extract extents

        :param Index destination_index: datacube Index where extents will be stored
        """
        # We need the following info for multi-processes to create Datacube objects
        self._hostname = hostname
        self._port = port
        self._database = database
        self._username = username

        # Access to tables
        self._extent_index = destination_index
        from sqlalchemy.pool import NullPool
        self._engine = create_engine(destination_index.url, poolclass=NullPool, client_encoding='utf8')
        meta = MetaData(self._engine, schema=SCHEMA_NAME)
        meta.reflect(bind=self._engine, only=['extent', 'extent_meta', 'ranges'], schema=SCHEMA_NAME)
        self._extent_table = meta.tables[SCHEMA_NAME+'.extent']
        self._extent_meta_table = meta.tables[SCHEMA_NAME+'.extent_meta']
        self._ranges_table = meta.tables[SCHEMA_NAME + '.ranges']
        self._dataset_type_table = meta.tables[SCHEMA_NAME+'.dataset_type']

        # Metadata pre-loads
        self.metadata = ExtentMetadata(destination_index).items

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
        # Alternatively you can get extent_meta id and search for (extent_meta_id, start)
        extent_uuid = compute_uuid(dataset_type_ref, start, offset_alias)

        extent_query = select([self._extent_table.c.geometry]).\
            where(self._extent_table.c.id == extent_uuid.hex)
        with self._engine.begin() as conn:
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
        extent_uuid = compute_uuid(dataset_type_ref, start, offset_alias)

        # See whether an entry already exists in extent
        extent_query = select([self._extent_table.c.id]).\
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
            # Get extent_meta_ref
            extent_meta_ref = self.metadata[(dataset_type_ref, offset_alias)]['id']
            # Insert a new entry
            ins = self._extent_table.insert().values(id=extent_uuid.hex, extent_meta_ref=extent_meta_ref,
                                                     start=start, geometry=mapping(extent))
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
        period_list = PeriodIndex(start=period.start_time, end=period.end_time, freq=offset_pool)
        with Pool(processes=POOL_SIZE) as pool:
            extent_list = pool.map(ComputeChunk(product=product_name, hostname=self._hostname,
                                                port=self._port, database=self._database,
                                                username=self._username, compute=extent_per_period,
                                                projection=projection), period_list)

        # Filter out extents of NoneType (this part probably unnecessary now)
        extent_list = [extent for extent in extent_list if extent]
        full_extent = cascaded_union(extent_list) if extent_list else wkt.loads('MULTIPOLYGON EMPTY')

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
        offset alias within the specified time range. It updates the extent_meta and extent tables rather than
        replacing the corresponding records so that there is no gaps along time that wasn't looked at.

        :param product_name: name of the product
        :param start: start time preferably in datetime type of extent computation and storage
        :param end: end time preferably in datetime type of extent computation and storage
        :param offset_alias: pandas style offset alias string indicating the length of each extent record in time
        :param projection: Geo spacial projection to be applied
        """
        if not offset_alias:
            offset_alias = DEFAULT_FREQ

        # Parse the time arguments
        start = parse_time(start)
        end = parse_time(end)

        # Lets get the dataset_type id
        dataset_type_ref = self._extent_index.products.get_by_name(product_name).id

        if dataset_type_ref:
            conn = self._engine.connect()
            # Now we are ready to get the extent_metadata id
            extent_meta_row = self._get_extent_meta_row(dataset_type_ref, offset_alias)
            if extent_meta_row:
                # make sure there are no gaps from start to end in the database
                # new_start_c and new_end_c are for computes
                new_start_c = extent_meta_row['end'] if start > extent_meta_row['end'] else start
                new_end_c = extent_meta_row['start'] if end < extent_meta_row['start'] else end

                # new_start_d and new_end_d are for extent_meta table to update
                new_start_d = min(new_start_c, extent_meta_row['start'])
                new_end_d = max(new_end_c, extent_meta_row['end'])

                # Got to update meta data
                update = self._extent_meta_table.update(). \
                    where(self._extent_meta_table.c.id == extent_meta_row['id']). \
                    values(start=new_start_d, end=new_end_d, crs=projection)
                conn.execute(update)

                # We are pre-loading metadata so got to update those
                self.metadata = ExtentMetadata(self._extent_index).items

                conn.close()
                # Time to insert/update new extent data
                self._store_many(product_name=product_name, dataset_type_ref=dataset_type_ref,
                                 start=new_start_c, end=new_end_c, offset_alias=offset_alias, projection=projection)

            else:
                # insert a new meta entry
                ins = self._extent_meta_table.insert().values(dataset_type_ref=dataset_type_ref,
                                                              start=start, end=end,
                                                              offset_alias=offset_alias,
                                                              crs=projection)
                conn.execute(ins)

                # We are pre-loading metadata so got to update those
                self.metadata = ExtentMetadata(self._extent_index).items

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

        :param datetime lower: The lower time bound
        :param datetime upper: The upper time bound
        :param BoundingBox bounds: The spacial bounds
        :param str projection: The projection used
        :return:
        """

        bounds_json = {'left': bounds.left, 'bottom': bounds.bottom, 'right': bounds.right, 'top': bounds.top}

        conn = self._engine.connect()

        # See whether an entry exists in product_bounds
        bounds_query = select([self._ranges_table.c.id]).\
            where(self._ranges_table.c.dataset_type_ref == dataset_type_ref)
        bounds_row = conn.execute(bounds_query).fetchone()
        if bounds_row:
            # Update the existing entry
            update = self._ranges_table.update().\
                where(self._ranges_table.c.id == bounds_row['id']).\
                values(time_min=lower, time_max=upper, bounds=json.dumps(bounds_json), crs=projection)
            conn.execute(update)
        else:
            # Insert a new entry
            ins = self._ranges_table.insert().values(dataset_type_ref=dataset_type_ref,
                                                     time_min=lower, time_max=upper,
                                                     bounds=json.dumps(bounds_json), crs=projection)
            conn.execute(ins)
        conn.close()

    @staticmethod
    def _empty_box(bound):
        """
        Check whether the BoundingBox is empty

        :param BoundingBox bound: a BoundingBox
        :return bool:
        """
        if not bool(bound):
            return None
        return bound.left is None or bound.bottom is None or bound.right is None or bound.top is None

    @staticmethod
    def _bounds_union(bound1, bound2):
        """
        Computes the union of two given spacial bounds

        :param BoundingBox bound1: A axis aligned bound
        :param BoundingBox bound2: A axis aligned bound
        :return BoundingBox: The union of the given bounds
        """
        if ExtentUpload._empty_box(bound1):
            return bound2
        elif ExtentUpload._empty_box(bound2):
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

        period_list = PeriodIndex(start=time_hints[0], end=time_hints[1], freq='1Y')
        with Pool(processes=POOL_SIZE) as pool:
            bounds_list = pool.map(ComputeChunk(product=product, hostname=self._hostname,
                                                port=self._port, database=self._database,
                                                username=self._username, compute=ExtentUpload._compute_bounds,
                                                projection=projection), period_list)

        # Aggregate time min, time max, and bounds
        return reduce(lambda x, y: (_cool_min(x[0], y[0]), _cool_max(x[1], y[1]),
                                    ExtentUpload._bounds_union(x[2], y[2])), bounds_list)

    @staticmethod
    def _compute_bounds(datasets, projection=None):
        """
        Computes the min, max time bounds and spacial bounds

        :param datasets: Non-empty list of datasets
        :param str projection: a projection string
        :return tuple: min time, max, time, and bounding box
        """

        if not bool(datasets):
            raise ValueError('There is no datasets')

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
                bounds = ExtentUpload._bounds_union(bounds, dataset.extent.boundingbox)
            else:
                bounds = ExtentUpload._bounds_union(bounds, dataset.extent.to_crs(CRS(projection)).boundingbox)
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
        dataset_type_ref = self._extent_index.products.get_by_name(product_name).id
        if not dataset_type_ref:
            raise KeyError("dataset_type_ref does not exist")

        if time_hints:
            lower, upper, bounds = self._compute_bounds_with_hints(product=product_name,
                                                                   time_hints=time_hints,
                                                                   projection=projection)
            self._store_bounds_record(dataset_type_ref=dataset_type_ref, lower=lower,
                                      upper=upper, bounds=bounds, projection=projection)
        else:
            dc = self._loading_datacube
            datasets = peek_generator(dc.find_datasets_lazy(product=product_name))
            if datasets:
                lower, upper, bounds = ExtentUpload._compute_bounds(datasets, projection=projection)
                self._store_bounds_record(dataset_type_ref=dataset_type_ref, lower=lower,
                                          upper=upper, bounds=bounds, projection=projection)

    def update_bounds(self, product_name, to_time):
        to_time = parse_time(to_time)
        # Retrieve the current bounds record
        bounds = ExtentIndex(datacube_index=self._extent_index).get_bounds(product_name)
        if bounds:
            from_time = bounds['time_max']
            if from_time > to_time:
                return
            dc = self._loading_datacube
            datasets = peek_generator(dc.find_datasets_lazy(product=product_name, time=(from_time, to_time)))
            if datasets:
                old_lower = bounds['time_min']
                bds = json.loads(bounds['bounds'])
                old_bounds = BoundingBox(left=bds['left'], bottom=bds['bottom'], right=bds['right'], top=bds['top'])
                _, new_upper, new_bounds = ExtentUpload._compute_bounds(datasets,
                                                                        projection=bounds['crs'])
                dataset_type_ref = self._extent_index.products.get_by_name(product_name).id
                self._store_bounds_record(dataset_type_ref=dataset_type_ref, lower=old_lower,
                                          upper=new_upper,
                                          bounds=ExtentUpload._bounds_union(old_bounds, new_bounds),
                                          projection=bounds['crs'])
            else:
                return
        else:
            raise KeyError("{} does not exist".format(product_name))

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


if __name__ == '__main__':
    # ToDo These stuff are to be removed
    # Get the Connections to the databases
    EXTENT_DB = PostgresDb.create(hostname='agdcdev-db.nci.org.au', database='datacube', port=6432, username='aj9439')
    EXTENT_IDX = ExtentUpload(hostname='agdc-db.nci.org.au', database='datacube', port=6432,
                              username='aj9439', destination_index=Index(EXTENT_DB))

    # load into extents table
    # EXTENT_IDX.store_extent(product_name='ls8_nbar_scene', start='2013-01',
    #                         end='2013-05', offset_alias='1D', projection='EPSG:4326')
    # EXTENT_IDX.store_extent(product_name='ls8_nbar_albers', start='2017-01',
    #                         end='2017-05', offset_alias='1M', projection='EPSG:4326')
    # EXTENT_IDX.store_bounds(product_name='ls8_nbar_albers', projection='EPSG:4326')
    # EXTENT_IDX.store_bounds(product_name='ls8_nbar_scene', projection='EPSG:4326')
