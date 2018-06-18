from datetime import datetime, date, timezone
from pandas import Timestamp
from datacube.drivers.postgres import PostgresDb
from datacube.index._products import ProductResource
from datacube.index._metadata_types import MetadataTypeResource
import unittest


class MockPostgresDb(PostgresDb):
    def __init__(self):  # pylint: disable=super-init-not-called
        pass

    def connect(self):
        return MockPostgresDbAPI()


class MockPostgresDbAPI(object):
    def __init__(self):
        self.extent_meta = [
            {'id': 1, 'dataset_type_ref': 11,
             'start': datetime(year=2015, month=3, day=19), 'end': datetime(year=2018, month=6, day=14),
             'offset_alias': '1Y', 'crs': 'EPSG:4326',
             'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'},
            {'id': 2, 'dataset_type_ref': 32,
             'start': datetime(year=2018, month=3, day=19), 'end': datetime(year=2018, month=6, day=14),
             'offset_alias': '1M', 'crs': 'EPSG:4326',
             'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'}
        ]
        self.extent = [
            {'id': 'uuid11xxxxxxxxxxxxxxxxxxxxxxxxx1', 'extent_meta_ref': 1,
             'start': datetime(year=2015, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}
            },
            {'id': 'uuid11xxxxxxxxxxxxxxxxxxxxxxxxx2', 'extent_meta_ref': 1,
             'start': datetime(year=2016, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}
            },
            {'id': 'uuid11xxxxxxxxxxxxxxxxxxxxxxxxx3', 'extent_meta_ref': 1,
             'start': datetime(year=2017, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}
            },
            {'id': 'uuid11xxxxxxxxxxxxxxxxxxxxxxxxx4', 'extent_meta_ref': 1,
             'start': datetime(year=2018, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}
            },
            {'id': 'uuid32xxxxxxxxxxxxxxxxxxxxxxxxx1', 'extent_meta_ref': 2,
             'start': datetime(year=2018, month=3, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}
            },
            {'id': 'uuid32xxxxxxxxxxxxxxxxxxxxxxxxx2', 'extent_meta_ref': 2,
             'start': datetime(year=2018, month=4, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}
            },
            {'id': 'uuid32xxxxxxxxxxxxxxxxxxxxxxxxx3', 'extent_meta_ref': 2,
             'start': datetime(year=2018, month=5, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}
            },
            {'id': 'uuid32xxxxxxxxxxxxxxxxxxxxxxxxx4', 'extent_meta_ref': 2,
             'start': datetime(year=2018, month=6, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}
            }
        ]
        self.ranges = [
            {'id': 1, 'dataset_type_ref': 11,
             'time_min': datetime(year=2015, month=3, day=19), 'time_max': datetime(year=2018, month=6, day=14),
             'bounds': {}, 'crs': 'EPSG:4326',
             'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'
            },
            {'id': 2, 'dataset_type_ref': 32,
             'time_min': datetime(year=2018, month=3, day=19), 'time_max': datetime(year=2018, month=6, day=14),
             'bounds': {}, 'crs': 'EPSG:4326',
             'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'
            }
        ]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get_dataset_type_by_name(self, name):
        if name == 'ls8_nbar_scene':
            return {'definition': {}, 'metadata_type_ref': 1, 'id': 11}
        elif name == 'ls8_nbar_albers':
            return {'definition': {}, 'metadata_type_ref': 1, 'id': 32}
        else:
            return None

    def get_db_extent_meta(self, dataset_type_ref, offset_alias):
        """
        Extract a row corresponding to dataset_type id and offset_alias from extent_meta table
        :param dataset_type_ref: dataset type id
        :param str offset_alias: Pandas style offset period string. for example '1M' indicate a month,
                                 '1Y' indicates a year, '1D' indicates a day.
        :return: single extent_meta row matching the parameters
        """
        for rec in self.extent_meta:
            if rec['dataset_type_ref'] == dataset_type_ref and rec['offset_alias'] == offset_alias:
                return rec
        return None

    def get_db_extent(self, dataset_type_ref, start, offset_alias):
        """
        Extract and return extent information corresponding to dataset type, start, and offset_alias.
        The start time and db_extent.start are casted to date types during retrieval.
        :param dataset_type_ref: dataset type id
        :param datetime.datetime start: datetime representation of start timestamp
        :param offset_alias: pandas style period string, for example '1M' indicate a month,
                            '1Y' indicates a year, '1D' indicates a day.
        :return: 'geometry' field if a database record exits otherwise None
        """

        def _parse_date(time_stamp):
            """
               Parses a time representation into a datetime object with year, month, day values and timezone
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

        # Get extent metadata
        metadata = self.get_db_extent_meta(dataset_type_ref, offset_alias)
        if not bool(metadata):
            return None

        start = _parse_date(start)
        for rec in self.extent:
            if rec['extent_meta_ref'] == metadata['id'] and rec['start'].date() == start.date():
                return rec['geometry']
        return None

    def get_ranges(self, dataset_type_ref):
        """
        Returns a ranges record corresponding to a given product id
        :param dataset_type_ref: dataset type id
        :return sqlalchemy.engine.result.RowProxy: a row corresponding to product name, if exists otherwise
        return None
        """
        for rec in self.ranges:
            if rec['dataset_type_ref'] == dataset_type_ref:
                return rec
        return None


class TestExtent(unittest.TestCase):
    def setUp(self):
        self.mock_db = MockPostgresDb()
        self.mock_db_api = MockPostgresDbAPI()

        # We will access a real db for setting up the metadata types
        from datacube import Datacube
        db = Datacube(app='test').index._db  # pylint: disable=protected-access
        metadata_type_resource = MetadataTypeResource(db)

        self.products = ProductResource(db=self.mock_db, metadata_type_resource=metadata_type_resource)

    # test db api access
    def test_db_api_access(self):
        extent = self.products.extent(dataset_type_id=11, start='1-1-2015',
                                      offset_alias='1Y').__geo_interface__['coordinates']
        self.assert_multipolygon(extent, self.mock_db_api.extent[0]['geometry']['coordinates'])

    # test different time representations
    def test_time_values(self):
        extent = self.products.extent(dataset_type_id=11, start='01-01-2015',
                                      offset_alias='1Y').__geo_interface__['coordinates']
        self.assert_multipolygon(extent, self.mock_db_api.extent[0]['geometry']['coordinates'])

        extent = self.products.extent(dataset_type_id=11, start=datetime(year=2015, month=1, day=1),
                                      offset_alias='1Y').__geo_interface__['coordinates']
        self.assert_multipolygon(extent, self.mock_db_api.extent[0]['geometry']['coordinates'])

        extent = self.products.extent(dataset_type_id=11, start=date(year=2015, month=1, day=1),
                                      offset_alias='1Y').__geo_interface__['coordinates']
        self.assert_multipolygon(extent, self.mock_db_api.extent[0]['geometry']['coordinates'])

        extent = self.products.extent(dataset_type_id=11, start='2015',
                                      offset_alias='1Y').__geo_interface__['coordinates']
        self.assert_multipolygon(extent, self.mock_db_api.extent[0]['geometry']['coordinates'])

        extent = self.products.extent(dataset_type_id=11, start='01-2015',
                                      offset_alias='1Y').__geo_interface__['coordinates']
        self.assert_multipolygon(extent, self.mock_db_api.extent[0]['geometry']['coordinates'])

        extent = self.products.extent(dataset_type_id=11, start='1-1-2015',
                                      offset_alias='1Y').__geo_interface__['coordinates']
        self.assert_multipolygon(extent, self.mock_db_api.extent[0]['geometry']['coordinates'])

    def test_yearly_extents(self):
        extents = self.products.extent_periodic(dataset_type_id=11, start='01-01-2015', end='01-01-2017',
                                                offset_alias='1Y')
        for item in extents:
            self.assert_multipolygon(item['extent'].__geo_interface__['coordinates'],
                                     self.mock_db_api.extent[0]['geometry']['coordinates'])

    def test_monthly_extents(self):
        extents = self.products.extent_periodic(dataset_type_id=32, start='01-01-2018', end='01-05-2018',
                                                offset_alias='1M')
        for item in extents:
            self.assert_multipolygon(item['extent'].__geo_interface__['coordinates'],
                                     self.mock_db_api.extent[0]['geometry']['coordinates'])

    def test_ranges(self):
        ranges = self.products.ranges('ls8_nbar_scene')
        self.assertEqual(ranges['dataset_type_ref'], 11)

    def assert_multipolygon(self, p1, p2):
        if not len(p1) == len(p2):
            raise AssertionError('Multipolygons has different sizes')
        else:
            if not len(p1) == 0:
                p1_outer = p1[0]
                p2_outer = p2[0]
                self.assertCountEqual(p1_outer, p2_outer)
                if len(p1) == 2:
                    p1_inner = p1[1]
                    p2_inner = p2[1]
                    self.assertCountEqual(p1_inner, p2_inner)


if __name__ == '__main__':
    unittest.main()
