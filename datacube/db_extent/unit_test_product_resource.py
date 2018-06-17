from datetime import datetime


class MockPostgresDb(objects):
    def __init__(self):
        pass

    def connect(self):
        yield MockPostgresDbAPI()


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
                          'coordinates': [[[0, 0], [2, 0], [2, 2], [0, 2]], [[1, 1], [1.5, 1], [1.5, 1.5]]]
                          }
             },
            {'id': 'uuid11xxxxxxxxxxxxxxxxxxxxxxxxx2', 'extent_meta_ref': 1,
             'start': datetime(year=2016, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[[0, 0], [2, 0], [2, 2], [0, 2]], [[1, 1], [1.5, 1], [1.5, 1.5]]]
                          }
             },
            {'id': 'uuid11xxxxxxxxxxxxxxxxxxxxxxxxx3', 'extent_meta_ref': 1,
             'start': datetime(year=2017, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[[0, 0], [2, 0], [2, 2], [0, 2]], [[1, 1], [1.5, 1], [1.5, 1.5]]]
                          }
             },
            {'id': 'uuid11xxxxxxxxxxxxxxxxxxxxxxxxx4', 'extent_meta_ref': 1,
             'start': datetime(year=2018, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[[0, 0], [2, 0], [2, 2], [0, 2]], [[1, 1], [1.5, 1], [1.5, 1.5]]]
                          }
             },
            {'id': 'uuid32xxxxxxxxxxxxxxxxxxxxxxxxx1', 'extent_meta_ref': 1,
             'start': datetime(year=2018, month=3, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[[0, 0], [2, 0], [2, 2], [0, 2]], [[1, 1], [1.5, 1], [1.5, 1.5]]]
                          }
             },
            {'id': 'uuid32xxxxxxxxxxxxxxxxxxxxxxxxx2', 'extent_meta_ref': 1,
             'start': datetime(year=2018, month=4, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[[0, 0], [2, 0], [2, 2], [0, 2]], [[1, 1], [1.5, 1], [1.5, 1.5]]]
                          }
             },
            {'id': 'uuid32xxxxxxxxxxxxxxxxxxxxxxxxx3', 'extent_meta_ref': 1,
             'start': datetime(year=2018, month=5, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[[0, 0], [2, 0], [2, 2], [0, 2]], [[1, 1], [1.5, 1], [1.5, 1.5]]]
                          }
             },
            {'id': 'uuid32xxxxxxxxxxxxxxxxxxxxxxxxx4', 'extent_meta_ref': 1,
             'start': datetime(year=2018, month=6, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[[0, 0], [2, 0], [2, 2], [0, 2]], [[1, 1], [1.5, 1], [1.5, 1.5]]]
                          }
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
        from datetime import datetime, timezone
        from pandas import Timestamp

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


# set up objects for unit tests
from datacube.index._products import ProductResource
product_resource = ProductResource(db=MockPostgresDb(), metadata_type_resource=None)