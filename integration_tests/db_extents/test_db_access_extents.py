from datacube import Datacube
from urllib.parse import urlparse
from sqlalchemy import create_engine
from datacube.drivers.postgres import PostgresDb
from datacube.drivers.postgres._core import ensure_db, has_schema
from datacube.index import Index
from datetime import datetime, timezone
import json

SCHEMA_NAME = 'agdc'


class TestDbExtent(object):
    @staticmethod
    def load_tables(engine):
        extent_meta = [
            {'id': 1, 'dataset_type_ref': 11,
             'start': datetime(year=2015, month=3, day=19), 'end': datetime(year=2018, month=6, day=14),
             'offset_alias': '1Y', 'crs': 'EPSG:4326',
             'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'},
            {'id': 2, 'dataset_type_ref': 32,
             'start': datetime(year=2018, month=3, day=19), 'end': datetime(year=2018, month=6, day=14),
             'offset_alias': '1M', 'crs': 'EPSG:4326',
             'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'}
        ]
        extent = [
            {'id': 'dddd11ee-eeee-eeee-eeee-eeeeeeeeeee1', 'extent_meta_ref': 1,
             'start': datetime(year=2015, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
            {'id': 'dddd11ee-eeee-eeee-eeee-eeeeeeeeeee2', 'extent_meta_ref': 1,
             'start': datetime(year=2016, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
            {'id': 'dddd11ee-eeee-eeee-eeee-eeeeeeeeeee3', 'extent_meta_ref': 1,
             'start': datetime(year=2017, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
            {'id': 'dddd11ee-eeee-eeee-eeee-eeeeeeeeeee4', 'extent_meta_ref': 1,
             'start': datetime(year=2018, month=1, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
            {'id': 'dddd32ee-eeee-eeee-eeee-eeeeeeeeeee1', 'extent_meta_ref': 2,
             'start': datetime(year=2018, month=3, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
            {'id': 'dddd32ee-eeee-eeee-eeee-eeeeeeeeeee2', 'extent_meta_ref': 2,
             'start': datetime(year=2018, month=4, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
            {'id': 'dddd32ee-eeee-eeee-eeee-eeeeeeeeeee3', 'extent_meta_ref': 2,
             'start': datetime(year=2018, month=5, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}},
            {'id': 'dddd32ee-eeee-eeee-eeee-eeeeeeeeeee4', 'extent_meta_ref': 2,
             'start': datetime(year=2018, month=6, day=1),
             'geometry': {'type': 'Polygon',
                          'coordinates': [[(0, 0), (2, 0), (2, 2), (0, 2)], [(1, 1), (1.5, 1), (1.5, 1.5)]]}}
        ]
        ranges = [
            {'id': 1, 'dataset_type_ref': 11,
             'time_min': datetime(year=2015, month=3, day=19), 'time_max': datetime(year=2018, month=6, day=14),
             'bounds': {}, 'crs': 'EPSG:4326',
             'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'},
            {'id': 2, 'dataset_type_ref': 32,
             'time_min': datetime(year=2018, month=3, day=19), 'time_max': datetime(year=2018, month=6, day=14),
             'bounds': {}, 'crs': 'EPSG:4326',
             'time_added': datetime(year=2018, month=6, day=14), 'added_by': 'aj9439'}
        ]
        u = urlparse(engine.url.__str__())
        username = u.username
        time_now = datetime.now(timezone.utc).isoformat()
        with engine.connect() as conn:
            # load metadata_type table
            conn.execute("INSERT INTO {}.metadata_type VALUES (1, 'eo', '{{}}', TIMESTAMP '{}', '{}')".format(
                SCHEMA_NAME, time_now, username
            ))
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
            # load extent_meta table
            for item in extent_meta:
                conn.execute(
                    "INSERT INTO {}.extent_meta ".format(SCHEMA_NAME) +
                    "VALUES ({}, {}, TIMESTAMP '{}', TIMESTAMP '{}', '{}', '{}', TIMESTAMP '{}', '{}')".format(
                        item['id'], item['dataset_type_ref'],
                        item['start'].isoformat(), item['end'].isoformat(),
                        item['offset_alias'], item['crs'],
                        item['time_added'].isoformat(), item['added_by']
                    ))
            # load extent table
            for item in extent:
                conn.execute("INSERT INTO {}.extent VALUES ('{}'::uuid, {}, TIMESTAMP '{}', '{}')".format(
                    SCHEMA_NAME,
                    item['id'], item['extent_meta_ref'],
                    item['start'],
                    json.dumps(item['geometry'])
                ))

    @staticmethod
    def drop_database(engine, db):
        with engine.connect() as conn:
            # update system catalog
            conn.execute("UPDATE pg_database SET datallowconn = 'false' WHERE datname = '{}'".format(db))
            # force disconnect all clients
            conn.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'".format(db))
            # end the open transaction
            conn.execute("commit")
            # drop database db_extent_test if it exists
            conn.execute("drop database {}".format(db))

    @staticmethod
    def database_exists(engine, db):
        with engine.connect() as conn:
            return bool(conn.execute(
                "SELECT datname from pg_catalog.pg_database where datname = '{}'".format(db)
            ).fetchone())

    @classmethod
    def setup(cls):
        cls._dc = Datacube(app='test', env='dev')

        # get the engine of default postgres database
        url = urlparse(cls._dc.index._db._engine.url.__str__())
        new_url = url.scheme + '://' + url.netloc + '/postgres'
        engine = create_engine(new_url)

        # drop db_extent_test
        if cls.database_exists(engine, 'db_extent_test'):
            cls.drop_database(engine, 'db_extent_test')

        # create a new database
        with cls._dc.index._db._engine.connect() as conn:
            # end the open transaction
            conn.execute("commit")
            # create a new database db_extent_test
            conn.execute("create database db_extent_test")

        # get the engine of the newly created database
        url = urlparse(cls._dc.index._db._engine.url.__str__())
        new_url = url.scheme + '://' + url.netloc + '/db_extent_test'
        engine = create_engine(new_url)

        # create schema if not present
        if not has_schema(engine, engine):
            ensure_db(engine)

        # load tables
        cls.load_tables(engine)

        # update Datacube object to refer to the newly created database
        o = urlparse(new_url)
        db = PostgresDb.create(hostname=o.hostname, database='db_extent_test', port=o.port, username=o.username)
        cls._dc = Datacube(index=Index(db))

        # open a transaction object
        cls._trans = cls._dc.index._db._engine.connect().begin()

        # load extents and ranges
        print('in setup')

    @classmethod
    def teardown(cls):
        # rollback the transaction
        cls._trans.rollback()

        # connect to default postgres database
        url = urlparse(cls._dc.index._db._engine.url.__str__())
        new_url = url.scheme + '://' + url.netloc + '/postgres'
        engine = create_engine(new_url)

        # drop the database
        cls.drop_database(engine, 'db_extent_test')
        print('in teardown')

    # Test extent_meta
    def test_extent_meta(self):
        # get a year long extent meta
        with self._dc.index._db.connect() as conn:
            res = conn.get_db_extent_meta(11, '1Y')
            print(res)


if __name__ == '__main__':
    TestDbExtent.setup()
    TestDbExtent().test_extent_meta()
    TestDbExtent.teardown()
