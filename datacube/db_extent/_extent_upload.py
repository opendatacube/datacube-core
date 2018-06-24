import logging
import warnings
from datacube.utils.geometry import BoundingBox
from datacube.index import Index
from datacube.drivers.postgres import PostgresDb
from datacube.model import Range
import json
from datacube.db_extent import parse_date, peek_generator

_LOG = logging.getLogger(__name__)

# pandas style time period frequencies
DEFAULT_FREQ = 'M'


class ExtentUpload(object):
    """
    Parses and executes queries to store and get various extent information from the database
    """
    def __init__(self, hostname, port, database, username, destination_db):
        """
        Arguments specifying the source and destination databases to use.

        There are two sets of arguments: to re-create datacube object
        and an Index object linking to extent tables

        :param hostname: Host name of the datacube postgres db to extract extents
        :param port: port name of the datacube postgres db to extract extents
        :param database: database name of the datacube postgres db to extract extents
        :param username: user name of the datacube postgres db to extract extents

        :param PostgresDb destination_db: datacube db where extents will be stored
        """
        # We need the following info for multi-processes to create Datacube objects
        self._hostname = hostname
        self._port = port
        self._database = database
        self._username = username
        self._db_source = PostgresDb.create(hostname=hostname, database=database, port=port, username=username)
        self._source_index = Index(self._db_source)

        # Access to tables
        self._destination_db = destination_db
        self.index = Index(destination_db)

    def store_extent(self, product_name, start, end, offset_alias=None, crs=None):
        """
        store product extents to the database for each time period indicated by
        offset alias within the specified time range. It updates the extent_meta and extent tables rather than
        replacing the corresponding records so that there is no gaps along time that wasn't looked at.

        :param product_name: name of the product
        :param start: start time preferably in datetime type of extent computation and storage
        :param end: end time preferably in datetime type of extent computation and storage
        :param offset_alias: pandas style offset alias string indicating the length of each extent record in time
        :param crs: Geo spacial projection to be applied
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
            with self._destination_db.connect() as db_api:
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
                with self._destination_db.connect() as db_api:
                    db_api.merge_extent_meta(dataset_type_ref, new_start_d, new_end_d, offset_alias, crs)

                # compute new extent data
                extents = self._source_index.computes.compute_extent_many(product_name=product_name,
                                                                          start=new_start_c, end=new_end_c,
                                                                          offset_alias=offset_alias,
                                                                          crs=crs)
                # Save extents
                with self._destination_db.connect() as db_api:
                    db_api.update_extent_slice_many(metadata['id'], extents)

            else:
                # insert a new meta entry
                with self._destination_db.connect() as db_api:
                    db_api.merge_extent_meta(dataset_type_ref, start, end, offset_alias, crs)
                    # get it back to obtain the id
                    metadata = db_api.get_extent_meta(dataset_type_ref, offset_alias)
                    if not metadata:
                        raise KeyError("Extent meta record insert has failed")
                # compute new extent data
                extents = self._source_index.computes.compute_extent_many(product_name=product_name,
                                                                          start=start, end=end,
                                                                          offset_alias=offset_alias,
                                                                          crs=crs)
                # Save extents
                with self._destination_db.connect() as db_api:
                    db_api.update_extent_slice_many(metadata['id'], extents)

        else:
            raise KeyError("dataset_type_ref does not exist")

    # def _compute_bounds_with_hints(self, product, time_hints, projection=None):
    #     """
    #     ToDo:
    #     Use the given time period as a hint to find the bounds. If actual bounds falls outside the given
    #     time limits the values computed will not make sense
    #
    #     :param str product: Product name
    #     :param tuple time_hints: A tuple containing the begin and end of a period
    #     :param str projection: A projection string
    #     :return tuple : min_time, max_time, bounding box
    #     """
    #     def _cool_min(a, b):
    #         if not a:
    #             return b
    #         elif not b:
    #             return a
    #         else:
    #             return min(a, b)
    #
    #     def _cool_max(a, b):
    #         if not a:
    #             return b
    #         elif not b:
    #             return a
    #         else:
    #             return max(a, b)
    #
    #     period_list = PeriodIndex(start=time_hints[0], end=time_hints[1], freq='1Y')
    #     with Pool(processes=POOL_SIZE) as pool:
    #         bounds_list = pool.map(ComputeChunk(product=product, hostname=self._hostname,
    #                                             port=self._port, database=self._database,
    #                                             username=self._username, compute=ExtentUpload._compute_bounds,
    #                                             projection=projection), period_list)
    #
    #     # Aggregate time min, time max, and bounds
    #     return reduce(lambda x, y: (_cool_min(x[0], y[0]), _cool_max(x[1], y[1]),
    #                                 ExtentUpload._bounds_union(x[2], y[2])), bounds_list)

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
            lower, upper, bounds = self._source_index.computes.compute_ranges(crs=crs, product=product_name,
                                                                              time=time_hints)
        else:
            lower, upper, bounds = self._source_index.computes.compute_ranges(crs=crs, product=product_name)
        with self._destination_db.connect() as db_api:
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
            old_lower = ranges['time_min']
            bds = json.loads(ranges['bounds'])
            old_bounds = BoundingBox(left=bds['left'], bottom=bds['bottom'], right=bds['right'], top=bds['top'])
            new_ranges = self._source_index.computes.compute_ranges(crs=ranges['crs'],
                                                                    product=product_name,
                                                                    time=Range(from_time, to_time))
            if new_ranges:
                _, new_upper, new_bounds = new_ranges
                dataset_type_ref = self.index.products.get_by_name(product_name).id
                with self._destination_db.connect() as db_api:
                    db_api.update_ranges(dataset_type_ref=dataset_type_ref, time_min=old_lower,
                                         time_max=new_upper,
                                         bounds=self._source_index.computes.bounds_union(old_bounds, new_bounds),
                                         crs=ranges['crs'])
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
                              username='aj9439', destination_db=EXTENT_DB)

    # load into extents table
    # EXTENT_IDX.store_bounds(product_name='ls8_nbar_albers', projection='EPSG:4326')
    EXTENT_IDX.store_bounds(product_name='ls8_nbar_scene', projection='EPSG:4326')
    EXTENT_IDX.store_extent(product_name='ls8_nbar_scene', start='2013-01',
                            end='2013-10', offset_alias='1M', crs='EPSG:4326')
    # EXTENT_IDX.store_extent(product_name='ls8_nbar_albers', start='2017-01',
    #                         end='2017-05', offset_alias='1M', projection='EPSG:4326')
