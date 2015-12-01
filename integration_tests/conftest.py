# coding=utf-8
"""
Common methods for index integration tests.
"""
from __future__ import absolute_import

import os
import shutil
from pathlib import Path

import pytest
import rasterio
import yaml

from datacube.config import LocalConfig
from datacube.index._api import Index
from datacube.index.postgres import PostgresDb
from datacube.index.postgres.tables._core import ensure_db, SCHEMA_NAME


@pytest.fixture
def local_config(tmpdir):
    default = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'agdcintegration.conf')
    user = os.path.expanduser('~/.datacube_integration.conf')
    config = LocalConfig.find([default, user])
    config._config.set('locations', u'testdata', 'file://' + str(tmpdir.mkdir('testdata')))
    return config


@pytest.fixture
def db(local_config):
    db = PostgresDb.from_config(local_config)
    # Drop and recreate tables so our tests have a clean db.
    db._connection.execute('drop schema if exists %s cascade;' % SCHEMA_NAME)
    ensure_db(db._connection, db._engine)
    return db


@pytest.fixture
def index(db, local_config):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    """
    return Index(db, local_config)


@pytest.fixture
def default_collection(index):
    """
    :type index: datacube.index._api.Index
    """
    collections = index._add_default_collection()
    return collections[0]


def create_empty_geotiff(path):
    metadata = {'count': 1,
                'crs': 'EPSG:28355',
                'driver': 'GTiff',
                'dtype': 'int16',
                'height': 8521,
                'nodata': -999.0,
                'transform': [25.0, 0.0, 638000.0, 0.0, -25.0, 6276000.0],
                'width': 9721}
    with rasterio.open(path, 'w', **metadata) as dst:
        pass


@pytest.fixture
def example_ls5_dataset(tmpdir):
    # Based on LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302
    dataset_dir = tmpdir.mkdir('ls5_dataset')
    shutil.copy(os.path.join(os.path.dirname(__file__), 'ga-metadata.yaml'), str(dataset_dir))

    # Write geotiffs
    geotiff_name = "LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302_B{}0.tif"
    scene_dir = dataset_dir.mkdir('product').mkdir('scene01')
    scene_dir.join('report.txt').write('Example')
    for num in (1, 2, 3):
        path = scene_dir.join(geotiff_name.format(num))
        create_empty_geotiff(str(path))

    return Path(str(dataset_dir))


@pytest.fixture
def telemetry_collection(index):
    """
    :type index: datacube.index._api.Index
    """
    parsed = yaml.load(Path(__file__).parent.joinpath('telemetry-collection.yaml').open('r'))
    return index.collections.add(parsed)[0]


def test_ls5_dataset(example_ls5_dataset):
    assert False
