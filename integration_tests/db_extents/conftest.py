import pytest
from datetime import datetime, date, timezone
from urllib.parse import urlparse
import json
from shapely.geometry import asShape

SCHEMA_NAME = 'agdc'


@pytest.fixture
def extent_data():
    extent_meta = [
        {'id': 1, 'dataset_type_ref': 11,
         'start': date(year=2015, month=3, day=19), 'end': date(year=2018, month=6, day=14),
         'offset_alias': '1Y', 'crs': 'EPSG:4326',
         'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'},
        {'id': 2, 'dataset_type_ref': 32,
         'start': date(year=2018, month=3, day=19), 'end': date(year=2018, month=6, day=14),
         'offset_alias': '1M', 'crs': 'EPSG:4326',
         'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'}
    ]
    extent_slice = [
        {'extent_meta_ref': 1, 'start': date(year=2015, month=1, day=1),
         'geometry': {'type': 'Polygon',
                      'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
        {'extent_meta_ref': 1, 'start': date(year=2016, month=1, day=1),
         'geometry': {'type': 'Polygon',
                      'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
        {'extent_meta_ref': 1, 'start': date(year=2017, month=1, day=1),
         'geometry': {'type': 'Polygon',
                      'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
        {'extent_meta_ref': 1, 'start': date(year=2018, month=1, day=1),
         'geometry': {'type': 'Polygon',
                      'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
        {'extent_meta_ref': 2, 'start': date(year=2018, month=3, day=1),
         'geometry': {'type': 'Polygon',
                      'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
        {'extent_meta_ref': 2, 'start': date(year=2018, month=4, day=1),
         'geometry': {'type': 'Polygon',
                      'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
        {'extent_meta_ref': 2, 'start': date(year=2018, month=5, day=1),
         'geometry': {'type': 'Polygon',
                      'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
        {'extent_meta_ref': 2, 'start': date(year=2018, month=6, day=1),
         'geometry': {'type': 'Polygon',
                      'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}}
    ]
    return extent_meta, extent_slice


@pytest.fixture
def load_extents(initialised_postgres_db, extent_data):  # pylint: disable=redefined-outer-name
    """
    Loads a bunch of records to extent_meta and extent tables.
    """

    extent_meta, extent_slice = extent_data
    engine = initialised_postgres_db._engine
    u = urlparse(engine.url.__str__())
    username = u.username
    time_now = datetime.now(timezone.utc).isoformat()

    # we need dataset_type data to satisfy foreign key relationships
    with engine.connect() as conn:
        # load dataset_type table
        conn.execute(
            "INSERT INTO {}.dataset_type ".format(SCHEMA_NAME) +
            "VALUES (11, 'ls8_nbar_test', '{{}}', 1, '{{}}', TIMESTAMP '{}', '{}')".format(time_now, username)
        )
        conn.execute(
            "INSERT INTO {}.dataset_type ".format(SCHEMA_NAME) +
            "VALUES (32, 'ls8_nbar_albers_test', '{{}}', 1, '{{}}', TIMESTAMP '{}', '{}')".format(
                time_now, username
            ))

    with initialised_postgres_db.connect() as db_api:
        for item in extent_meta:
            db_api.merge_extent_meta(dataset_type_ref=item['dataset_type_ref'],
                                     start=item['start'].isoformat(), end=item['end'].isoformat(),
                                     offset_alias=item['offset_alias'], crs=item['crs'])
        for item in extent_slice:
            db_api.update_extent_slice(extent_meta_ref=item['extent_meta_ref'],
                                       start=item['start'].isoformat(),
                                       extent=item['geometry'])


@pytest.fixture
def load_ranges(initialised_postgres_db):
    """
    Loads a record to ranges table. It also adds a relevant record to dataset_type
    table to ensure foreign key relationship
    """
    engine = initialised_postgres_db._engine
    u = urlparse(engine.url.__str__())
    username = u.username
    time_now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        # load dataset_type table
        conn.execute(
            "INSERT INTO {}.dataset_type ".format(SCHEMA_NAME) +
            "VALUES (11, 'ls8_nbar_test', '{{}}', 1, '{{}}', TIMESTAMP '{}', '{}')".format(time_now, username)
        )
        # load ranges_type table
        time_min = datetime(year=2015, month=1, day=1)
        time_max = datetime(year=2018, month=1, day=1)
        bounds = {'left': 0, 'bottom': 0, 'right': 1.5, 'top': 1.5}
        conn.execute(
            "INSERT INTO {}.dataset_type_range ".format(SCHEMA_NAME) +
            "VALUES (1, 11, TIMESTAMP '{}', TIMESTAMP '{}', '{}' , 'EPSG:4326', TIMESTAMP '{}', '{}')".format(
                time_min, time_max, json.dumps(bounds), time_now, username
            )
        )
