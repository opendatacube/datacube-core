import logging
from shapely.geometry import asShape
from shapely.ops import cascaded_union
import shapely.wkt as wkt
from datacube.utils.geometry import CRS, BoundingBox
from pandas import period_range, PeriodIndex, Period
from datacube.drivers.postgres import PostgresDb
from datacube.model import Range
from multiprocessing import Pool
from datetime import datetime
from urllib.parse import urlparse

_LOG = logging.getLogger(__name__)

# pandas style time period frequencies
DEFAULT_FREQ = 'M'
DEFAULT_PER_PROCESS_FREQ = 'D'

# default projection string
DEFAULT_PROJECTION_BOUNDS = 'EPSG:4326'

# multiprocessing or threading pool size
POOL_SIZE = 31


class ComputeChunk(object):
    """
    This is a callable class designed for multiprocessors to initialize and subsequently execute
    periodic computation on a datacube of given database parameters.
    """

    def __init__(self, product, compute, hostname, port, database, username, crs=None):
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
        self._crs = crs

    def _set_index(self):
        """
        Creates a Datacube object from host, database info, and user info. Datacube objects needed
        to be created per process (They cannot be pickled!)

        :return Datacube: A Datacube object
        """
        db = PostgresDb.create(hostname=self._hostname, port=self._port,
                               database=self._database, username=self._username)
        # We need to import Index here to avoid circular imports
        from datacube.index import Index
        return Index(db)

    def __call__(self, period):
        """
        Implements an extent query to be executed by a multiprocess

        :param Period period: a pandas period object specifying time range
        :return: union of extents as computed by shapely cascaded union
        """

        # We need to release datacube resources after compute, so use a context manager
        with self._set_index() as index:
            return self._compute(index=index, product=self._product, period=period, projection=self._crs)


class ComputeResource(object):
    def __init__(self, db, datasets):
        """
        :type db: datacube.drivers.postgres._connections.PostgresDb
        :type datasets: datacube.index._datasets.DatasetResource
        """
        self._db = db
        self.datasets = datasets

    @staticmethod
    def _extent_per_period(index, product, period, projection=None):
        """
        Computes the extent for a given period. If projection parameter is absent
        no projection is applied

        :param Datacube dc: A Datacube object
        :param str product: product name
        :param Period period: A pandas period object
        :param str projection: A projection string
        :return: A shapely geometry. If the extent is empty a shapely empty geometry object is returned
        """
        datasets = index.datasets.search(product=product, time=Range(period.start_time, period.end_time))
        if not datasets:
            return wkt.loads('MULTIPOLYGON EMPTY')
        if projection:
            extents = [asShape(dataset.extent.to_crs(CRS(projection))) for dataset in datasets if dataset.extent]
        else:
            extents = [asShape(dataset.extent) for dataset in datasets if dataset.extent]
        return cascaded_union(extents) if extents else wkt.loads('MULTIPOLYGON EMPTY')

    def _get_db_url_parts(self):
        # get the db url
        url = self._db.url
        # parse the url
        u = urlparse(url.__str__())
        return u.hostname, u.port, u.username, u.path[1:]

    def compute_extent_one(self, product_name, period,
                           offset_pool=DEFAULT_PER_PROCESS_FREQ, crs=None):
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

        # Get url parts
        hostname, port, username, database = self._get_db_url_parts()

        # Compute full extent using multiprocess pools
        period_list = PeriodIndex(start=period.start_time, end=period.end_time, freq=offset_pool)
        with Pool(processes=POOL_SIZE) as pool:
            extent_list = pool.map(ComputeChunk(product=product_name, hostname=hostname,
                                                port=port, database=database,
                                                username=username, compute=self._extent_per_period,
                                                crs=crs), period_list)

        # Filter out extents of NoneType
        extent_list = [extent for extent in extent_list if extent]
        return cascaded_union(extent_list).__geo_interface__ if extent_list else None

    def compute_extent_many(self, product_name, start, end, offset_alias, crs=None):
        """
        Store extent records correspond to each period of length offset within start and end

        :param str product_name: product name
        :param dataset_type_ref: product_type id
        :param datetime start: Start time of sequence of offsets
        :param datetime end: End time of sequence of offsets
        :param offset_alias: Length of period as a pandas style offset alias
        :param crs: Geo spacial projection to be applied
        """

        idx = period_range(start=start, end=end, freq=offset_alias)
        for period in idx:
            yield (period, self.compute_extent_one(product_name=product_name, period=period,
                                                   crs=crs))

    @staticmethod
    def bounds_union(bound1, bound2):
        """
        Computes the union of two given spacial bounds

        :param BoundingBox bound1: A axis aligned bound
        :param BoundingBox bound2: A axis aligned bound
        :return BoundingBox: The union of the given bounds
        """

        def _empty_box(bound):
            """
            Check whether the BoundingBox is empty

            :param BoundingBox bound: a BoundingBox
            :return bool:
            """
            if not bool(bound):
                return None
            return bound.left is None or bound.bottom is None or bound.right is None or bound.top is None

        if _empty_box(bound1):
            return bound2
        elif _empty_box(bound2):
            return bound1
        else:
            return BoundingBox(left=min(bound1.left, bound2.left),
                               bottom=min(bound1.bottom, bound2.bottom),
                               right=max(bound1.right, bound2.right),
                               top=max(bound1.top, bound2.top))

    def compute_ranges(self, crs=None, **kwargs):
        """
        Computes the min, max time bounds and spatial bounds
        """
        datasets = self.datasets.search(**kwargs)
        try:
            first = next(datasets)
            lower, upper = first.time.begin, first.time.end
            bounds = first.extent.boundingbox if not crs else first.extent.to_crs(CRS(crs)).boundingbox
        except StopIteration:
            return None
        for dataset in datasets:
            if dataset.time.begin < lower:
                lower = dataset.time.begin
            if dataset.time.end > upper:
                upper = dataset.time.end
            if not crs:
                bounds = self.bounds_union(bounds, dataset.extent.boundingbox)
            else:
                bounds = self.bounds_union(bounds, dataset.extent.to_crs(CRS(crs)).boundingbox)
        return lower, upper, bounds
