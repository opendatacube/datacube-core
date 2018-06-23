import logging
import warnings
from shapely.geometry import asShape
from shapely.ops import cascaded_union
import shapely.wkt as wkt
from datacube import Datacube
from datacube.utils.geometry import CRS, BoundingBox
from pandas import period_range, PeriodIndex, Period
from datacube.index import Index
from datacube.drivers.postgres import PostgresDb
from multiprocessing import Pool
from datetime import datetime
import json
from functools import reduce
from datacube.db_extent import parse_date, peek_generator

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
        self.index = destination_index

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

    def compute_extent_one(self, product_name, dataset_type_ref, period, offset_alias,
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

        # Filter out extents of NoneType
        extent_list = [extent for extent in extent_list if extent]
        return cascaded_union(extent_list).__geo_interface__ if extent_list else None

    def compute_extent_many(self, product_name, dataset_type_ref, start, end, offset_alias, projection=None):
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
            yield (period, self.compute_extent_one(product_name=product_name, dataset_type_ref=dataset_type_ref,
                                                   period=period, offset_alias=offset_alias,
                                                   projection=projection))

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
        start = parse_date(start)
        end = parse_date(end)

        # Lets get the dataset_type id
        dataset_type_ref = self.index.products.get_by_name(product_name).id

        if dataset_type_ref:
            # get the extent_metadata
            with self.index._db.connect() as db_api:
                metadata = db_api.get_extent_meta(dataset_type_ref, offset_alias)
            if metadata:
                # make sure there are no gaps from start to end in the database
                # new_start_c and new_end_c are for computes
                new_start_c = metadata['end'] if start > metadata['end'] else start
                new_end_c = metadata['start'] if end < metadata['start'] else end

                # new_start_d and new_end_d are for extent_meta table to update
                new_start_d = min(new_start_c, metadata['start'])
                new_end_d = max(new_end_c, metadata['end'])

                # Got to update meta data
                with self.index._db.connect() as db_api:
                    db_api.merge_extent_meta(dataset_type_ref,new_start_d, new_end_d, offset_alias, projection)

                # compute new extent data
                extents = self.compute_extent_many(product_name=product_name, dataset_type_ref=dataset_type_ref,
                                                   start=new_start_c, end=new_end_c, offset_alias=offset_alias,
                                                   projection=projection)
                # Save extents
                with self.index._db.connect() as db_api:
                    db_api.update_extent_slice_many(metadata['id'], extents)

            else:
                # insert a new meta entry
                with self.index._db.connect() as db_api:
                    db_api.merge_extent_meta(dataset_type_ref, start, end, offset_alias, projection)
                    # get it back to obtain the id
                    metadata = db_api.get_extent_meta(dataset_type_ref, offset_alias)
                    if not metadata:
                        raise KeyError("Extent meta record insert has failed")
                # compute new extent data
                extents = self.compute_extent_many(product_name=product_name, dataset_type_ref=dataset_type_ref,
                                                   start=start, end=end, offset_alias=offset_alias,
                                                   projection=projection)
                # Save extents
                with self.index._db.connect() as db_api:
                    db_api.update_extent_slice_many(metadata['id'], extents)

        else:
            raise KeyError("dataset_type_ref does not exist")

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

    def _store_bounds(self, product_name, crs=None, time_hints=None):
        """
        Store a bounds record in the product_bounds table. It computes max, min bounds in the projected space
        using dataset extents. If projection is not specified, it is assumed that CRS is constant product wide else
        spacial bounds computed may not make sense. It stores time bounds as well. It does not validate projection
        parameter

        :param product_name:
        :param crs:
        :return:
        """

        # Get the dataset_type_ref
        dataset_type_ref = self.index.products.get_by_name(product_name).id
        if not dataset_type_ref:
            raise KeyError("dataset_type_ref does not exist")

        if time_hints:
            lower, upper, bounds = self._compute_bounds_with_hints(product=product_name,
                                                                   time_hints=time_hints,
                                                                   projection=crs)
            with self.index._db.connect() as db_api:
                db_api.update_ranges(dataset_type_ref=dataset_type_ref, time_min=lower,
                                     time_max=upper, bounds=bounds, crs=crs)
        else:
            dc = self._loading_datacube
            datasets = peek_generator(dc.find_datasets_lazy(product=product_name))
            if datasets:
                lower, upper, bounds = ExtentUpload._compute_bounds(datasets, projection=crs)
                with self.index._db.connect() as db_api:
                    db_api.update_ranges(dataset_type_ref=dataset_type_ref, time_min=lower,
                                         time_max=upper, bounds=bounds, crs=crs)

    def update_bounds(self, product_name, to_time):
        to_time = parse_date(to_time)
        # Retrieve the current ranges record
        ranges = self.index.products.ranges(product_name)
        if ranges:
            from_time = ranges['time_max']
            if from_time > to_time:
                return
            dc = self._loading_datacube
            datasets = peek_generator(dc.find_datasets_lazy(product=product_name, time=(from_time, to_time)))
            if datasets:
                old_lower = ranges['time_min']
                bds = json.loads(ranges['bounds'])
                old_bounds = BoundingBox(left=bds['left'], bottom=bds['bottom'], right=bds['right'], top=bds['top'])
                _, new_upper, new_bounds = self._compute_bounds(datasets, projection=ranges['crs'])
                dataset_type_ref = self.index.products.get_by_name(product_name).id
                with self.index._db.connect() as db_api:
                    db_api.update_ranges(dataset_type_ref=dataset_type_ref, time_min=old_lower,
                                         time_max=new_upper,
                                         bounds=self._bounds_union(old_bounds, new_bounds), crs=ranges['crs'])
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
    # EXTENT_IDX.store_bounds(product_name='ls8_nbar_albers', projection='EPSG:4326')
    # EXTENT_IDX.store_bounds(product_name='ls8_nbar_scene', projection='EPSG:4326')
    EXTENT_IDX.store_extent(product_name='ls8_nbar_scene', start='2013-01',
                            end='2013-05', offset_alias='1M', projection='EPSG:4326')
    # EXTENT_IDX.store_extent(product_name='ls8_nbar_albers', start='2017-01',
    #                         end='2017-05', offset_alias='1M', projection='EPSG:4326')
