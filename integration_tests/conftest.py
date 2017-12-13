# coding=utf-8
"""
Common methods for index integration tests.
"""
from __future__ import absolute_import

import itertools
from collections import namedtuple
import logging
import os
import shutil
from datetime import datetime, timedelta
from contextlib import contextmanager
from pathlib import Path
from copy import copy, deepcopy
from uuid import UUID, uuid4

import pytest
import numpy as np
import rasterio
import yaml

import datacube.utils
from datacube.index.postgres import _dynamic

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

from datacube.api import API
from datacube.config import LocalConfig
from datacube.index._api import Index, _DEFAULT_METADATA_TYPES_PATH
from datacube.index.postgres import PostgresDb
from datacube.index.postgres.tables import _core

Driver = namedtuple('Driver', ['name', 'uri_scheme', 'as_uri'])

# On Windows, symlinks are not supported in Python 2 and require
# specific privileges otherwise, so we copy instead of linking
if os.name == 'nt' or not hasattr(os, 'symlink'):
    symlink = shutil.copy
else:
    symlink = os.symlink

_SINGLE_RUN_CONFIG_TEMPLATE = """
[locations]
testdata: {test_tile_folder}
eotiles: {eotiles_tile_folder}
"""

GEOTIFF = {
    'date': datetime(1990, 3, 2),
    'shape': {
        'x': 432,
        'y': 321
    },
    'pixel_size': {
        'x': 25.0,
        'y': -25.0
    },
    'crs': 'EPSG:28355',  # 'EPSG:28355'
    'ul': {
        'x': 638000.0,  # Coords must match crs
        'y': 6276000.0  # Coords must match crs
    }
}

INTEGRATION_DEFAULT_CONFIG_PATH = Path(__file__).parent.joinpath('agdcintegration.conf')

_EXAMPLE_LS5_NBAR_DATASET_FILE = Path(__file__).parent.joinpath('example-ls5-nbar.yaml')
_TIME_SLICES = 3
'''Number of time slices to create in the sample data.'''

_BANDS = 3
'''Number of bands to generate geotiffs for.'''

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs' / 'config_samples'
DATASET_TYPES = CONFIG_SAMPLES / 'dataset_types'
LS5_SAMPLES = CONFIG_SAMPLES / 'storage_types' / 'ga_landsat_5'
LS5_NBAR_INGEST_CONFIG = CONFIG_SAMPLES / 'ingester' / 'ls5_nbar_albers.yaml'
LS5_NBAR_STORAGE_TYPE = LS5_SAMPLES / 'ls5_geographic.yaml'
LS5_NBAR_NAME = 'ls5_nbar'
LS5_NBAR_ALBERS_STORAGE_TYPE = LS5_SAMPLES / 'ls5_albers.yaml'
LS5_NBAR_ALBERS_NAME = 'ls5_nbar_albers'

# Resolution and chunking shrink factors
TEST_STORAGE_SHRINK_FACTORS = (100, 100)
TEST_STORAGE_NUM_MEASUREMENTS = 2
GEOGRAPHIC_VARS = ('latitude', 'longitude')
PROJECTED_VARS = ('x', 'y')

EXAMPLE_LS5_DATASET_ID = UUID('bbf3e21c-82b0-11e5-9ba1-a0000100fe80')


class MockIndex(object):
    def __init__(self, db):
        self._db = db

    @property
    def url(self):
        return self._db.url


@pytest.fixture
def integration_config_paths(tmpdir):
    test_tile_folder = str(tmpdir.mkdir('testdata'))
    test_tile_folder = Path(test_tile_folder).as_uri()
    eotiles_tile_folder = str(tmpdir.mkdir('eotiles'))
    eotiles_tile_folder = Path(eotiles_tile_folder).as_uri()
    run_config_file = tmpdir.mkdir('config').join('test-run.conf')
    run_config_file.write(
        _SINGLE_RUN_CONFIG_TEMPLATE.format(test_tile_folder=test_tile_folder, eotiles_tile_folder=eotiles_tile_folder)
    )
    return (
        str(INTEGRATION_DEFAULT_CONFIG_PATH),
        str(run_config_file),
        os.path.expanduser('~/.datacube_integration.conf')
    )


@pytest.fixture
def global_integration_cli_args(integration_config_paths):
    """
    The first arguments to pass to a cli command for integration test configuration.
    """
    # List of a config files in order.
    return list(itertools.chain(*(('--config_file', f) for f in integration_config_paths)))


@pytest.fixture
def local_config(integration_config_paths):
    return LocalConfig.find(integration_config_paths)


@pytest.fixture(params=["US/Pacific", "UTC", ])
def db(local_config, request):
    timezone = request.param

    db = PostgresDb.from_config(local_config, application_name='test-run', validate_connection=False)

    # Drop and recreate tables so our tests have a clean db.
    with db.connect() as connection:
        _core.drop_db(connection._connection)
    remove_dynamic_indexes()

    # Disable informational messages since we're doing this on every test run.
    with _increase_logging(_core._LOG) as _:
        _core.ensure_db(db._engine)

    c = db._engine.connect()
    c.execute('alter database %s set timezone = %r' % (local_config.db_database, str(timezone)))
    c.close()

    # We don't need informational create/drop messages for every config change.
    _dynamic._LOG.setLevel(logging.WARN)

    yield db
    db.close()


@contextmanager
def _increase_logging(log, level=logging.WARN):
    previous_level = log.getEffectiveLevel()
    log.setLevel(level)
    yield
    log.setLevel(previous_level)


def remove_dynamic_indexes():
    """
    Clear any dynamically created indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in _core.METADATA.tables.values():
        table.indexes.intersection_update([i for i in table.indexes if not i.name.startswith('dix_')])


@pytest.fixture(params=['NetCDF CF', 's3-test'])
def driver(request):
    # TODO: s3-driver support
    driver_name = request.param
    if driver_name not in ['NetCDF CF']:
        pytest.skip(driver_name + " driver not able to be loaded in this environment")

    driver = Driver(name=driver_name, uri_scheme='file', as_uri=(lambda p: 'file://'+str(p)))
    return driver


@pytest.fixture
def index(db):
    return Index(db)


@pytest.fixture
def dict_api(index):
    """
    :type index: datacube.index._api.Index
    """
    return API(index=index)


@pytest.fixture
def ls5_telem_doc(ga_metadata_type):
    return {
        "name": "ls5_telem_test",
        "description": 'LS5 Test',
        "metadata": {
            "platform": {
                "code": "LANDSAT_5"
            },
            "product_type": "satellite_telemetry_data",
            "ga_level": "P00",
            "format": {
                "name": "RCC"
            }
        },
        "metadata_type": ga_metadata_type.name
    }


@pytest.fixture
def ls5_telem_type(index, ls5_telem_doc):
    return index.products.add_document(ls5_telem_doc)


@pytest.fixture(scope='session')
def geotiffs(tmpdir_factory):
    """Create test geotiffs and corresponding yamls.

    We create one yaml per time slice, itself comprising one geotiff
    per band, each with specific custom data that can be later
    tested. These are meant to be used by all tests in the current
    session, by way of symlinking the yamls and tiffs returned by this
    fixture, in order to save disk space (and potentially generation
    time).

    The yamls are customised versions of
    :ref:`_EXAMPLE_LS5_NBAR_DATASET_FILE` shifted by 24h and with
    spatial coords reflecting the size of the test geotiff, defined in
    :ref:`GEOTIFF`.

    :param tmpdir_fatory: pytest tmp dir factory.
    :return: List of dictionaries `{'day':..., 'uuid':..., 'path':...,
      'tiffs':...}` with `day`: compact day string, e.g. `19900302`,
      `uuid` a unique UUID for this dataset (i.e. specific day),
      `path`: path to the yaml ingestion file, `tiffs`: list of paths
      to the actual geotiffs in that dataset, one per band.

    """
    tiffs_dir = tmpdir_factory.mktemp('tiffs')

    config = load_yaml_file(_EXAMPLE_LS5_NBAR_DATASET_FILE)[0]
    # Customise the spatial coordinates
    ul = GEOTIFF['ul']
    lr = {dim: ul[dim] + GEOTIFF['shape'][dim] * GEOTIFF['pixel_size'][dim] for dim in ('x', 'y')}
    config['grid_spatial']['projection']['geo_ref_points'] = {
        'ul': ul,
        'ur': {
            'x': lr['x'],
            'y': ul['y']
        },
        'll': {
            'x': ul['x'],
            'y': lr['y']
        },
        'lr': lr
    }
    # Generate the custom geotiff yamls
    return [_make_tiffs_and_yamls(tiffs_dir, config, day_offset)
            for day_offset in range(_TIME_SLICES)]


def _make_tiffs_and_yamls(tiffs_dir, config, day_offset):
    """Make a custom yaml and tiff for a day offset.

    :param path-like tiffs_dir: The base path to receive the tiffs.
    :param dict config: The yaml config to be cloned and altered.
    :param int day_offset: how many days to offset the original yaml
      by.
    """
    config = deepcopy(config)
    # Shift dates by the specific offset
    delta = timedelta(days=day_offset)
    day_orig = config['acquisition']['aos'].strftime('%Y%m%d')
    config['acquisition']['aos'] += delta
    config['acquisition']['los'] += delta
    config['extent']['from_dt'] += delta
    config['extent']['center_dt'] += delta
    config['extent']['to_dt'] += delta
    day = config['acquisition']['aos'].strftime('%Y%m%d')

    # Set the main UUID and assign random UUIDs where needed
    uuid = uuid4()
    config['id'] = str(uuid)
    level1 = config['lineage']['source_datasets']['level1']
    level1['id'] = str(uuid4())
    level1['lineage']['source_datasets']['satellite_telemetry_data']['id'] = str(uuid4())

    # Alter band data
    bands = config['image']['bands']
    for band in bands.keys():
        # Copy dict to avoid aliases in yaml output (for better legibility)
        bands[band]['shape'] = copy(GEOTIFF['shape'])
        bands[band]['cell_size'] = {
            dim: abs(GEOTIFF['pixel_size'][dim]) for dim in ('x', 'y')}
        bands[band]['path'] = bands[band]['path'].replace('product/', '').replace(day_orig, day)

    dest_path = str(tiffs_dir.join('agdc-metadata_%s.yaml' % day))
    with open(dest_path, 'w') as dest_yaml:
        yaml.dump(config, dest_yaml)
    return {
        'day': day,
        'uuid': uuid,
        'path': dest_path,
        'tiffs': _make_geotiffs(tiffs_dir, day_offset)  # make 1 geotiff per band
    }


def _make_geotiffs(tiffs_dir, day_offset):
    """Generate custom geotiff files, one per band."""
    tiffs = {}
    metadata = {'count': 1,
                'crs': GEOTIFF['crs'],
                'driver': 'GTiff',
                'dtype': 'int16',
                'width': GEOTIFF['shape']['x'],
                'height': GEOTIFF['shape']['y'],
                'nodata': -999.0,
                'transform': [GEOTIFF['pixel_size']['x'],
                              0.0,
                              GEOTIFF['ul']['x'],
                              0.0,
                              GEOTIFF['pixel_size']['y'],
                              GEOTIFF['ul']['y']]}

    for band in range(_BANDS):
        path = str(tiffs_dir.join('band%02d_time%02d.tif' % ((band + 1), day_offset)))
        with rasterio.open(path, 'w', **metadata) as dst:
            # Write data in "corners" (rounded down by 100, for a size of 100x100)
            data = np.zeros((GEOTIFF['shape']['y'], GEOTIFF['shape']['x']), dtype=np.int16)
            data[:] = np.arange(GEOTIFF['shape']['y'] * GEOTIFF['shape']['x']) \
                        .reshape((GEOTIFF['shape']['y'], GEOTIFF['shape']['x'])) + 10 * band + day_offset
            '''
            lr = (100 * int(GEOTIFF['shape']['y'] / 100.0),
                  100 * int(GEOTIFF['shape']['x'] / 100.0))
            data[0:100, 0:100] = 100 + day_offset
            data[lr[0] - 100:lr[0], 0:100] = 200 + day_offset
            data[0:100, lr[1] - 100:lr[1]] = 300 + day_offset
            data[lr[0] - 100:lr[0], lr[1] - 100:lr[1]] = 400 + day_offset
            '''
            dst.write(data, 1)
        tiffs[band] = path
    return tiffs


@pytest.fixture
def example_ls5_dataset_path(example_ls5_dataset_paths):
    """Create a single sample raw observation (dataset + geotiff)."""
    return list(example_ls5_dataset_paths.values())[0]


@pytest.fixture
def example_ls5_dataset_paths(tmpdir, geotiffs):
    """Create sample raw observations (dataset + geotiff).

    This fixture should be used by eah test requiring a set of
    observations over multiple time slices. The actual geotiffs and
    corresponding yamls are symlinks to a set created for the whole
    test session, in order to save disk and time.

    :param tmpdir: The temp directory in which to create the datasets.
    :param list geotiffs: List of session geotiffs and yamls, to be
      linked from this unique observation set sample.
    :return: dict: Dict of directories containing each observation,
      indexed by dataset UUID.
    """
    dataset_dirs = {}
    dataset_dir = tmpdir.mkdir('ls5_dataset')
    for geotiff in geotiffs:
        obs_name = 'LS5_TM_NBAR_P54_GANBAR01-002_090_084_%s' % geotiff['day']
        obs_dir = dataset_dir.mkdir(obs_name)
        symlink(str(geotiff['path']), str(obs_dir.join('agdc-metadata.yaml')))

        scene_dir = obs_dir.mkdir('scene01')
        scene_dir.join('report.txt').write('Example')
        geotiff_name = '%s_B{}0.tif' % obs_name
        for band in range(_BANDS):
            path = scene_dir.join(geotiff_name.format(band + 1))
            symlink(str(geotiff['tiffs'][band]), str(path))
        dataset_dirs[geotiff['uuid']] = Path(str(obs_dir))
    return dataset_dirs


@pytest.fixture
def ls5_nbar_ingest_config(tmpdir, driver):
    dataset_dir = tmpdir.mkdir('ls5_nbar_ingest_test')
    config = load_yaml_file(LS5_NBAR_INGEST_CONFIG)[0]
    config = alter_dataset_type_for_testing(config)
    config['storage']['crs'] = 'EPSG:28355'
    config['storage']['chunking']['time'] = 1
    # config['storage']['tile_size']['time'] = 2
    config['location'] = str(dataset_dir)
    if driver.name in ('s3', 's3-test'):
        config['container'] = str(dataset_dir)

    config_path = dataset_dir.join('ls5_nbar_ingest_config.yaml')
    with open(str(config_path), 'w') as stream:
        yaml.dump(config, stream)
    return config_path, config


def create_empty_geotiff(path):
    metadata = {'count': 1,
                'crs': 'EPSG:28355',
                'driver': 'GTiff',
                'dtype': 'int16',
                'height': 8521,
                'nodata': -999.0,
                'transform': [25.0, 0.0, 638000.0, 0.0, -25.0, 6276000.0],
                'compress': 'lzw',
                'width': 9721}
    with rasterio.open(path, 'w', **metadata) as dst:
        pass


@pytest.fixture
def default_metadata_type_docs():
    return [doc for (path, doc) in datacube.utils.read_documents(_DEFAULT_METADATA_TYPES_PATH)]


@pytest.fixture
def default_metadata_type_doc(default_metadata_type_docs):
    return [doc for doc in default_metadata_type_docs if doc['name'] == 'eo'][0]


@pytest.fixture
def telemetry_metadata_type_doc(default_metadata_type_docs):
    return [doc for doc in default_metadata_type_docs if doc['name'] == 'telemetry'][0]


@pytest.fixture
def ga_metadata_type_doc():
    _FULL_EO_METADATA = Path(__file__).parent.joinpath('extensive-eo-metadata.yaml')
    [(path, eo_md_type)] = datacube.utils.read_documents(_FULL_EO_METADATA)
    return eo_md_type


@pytest.fixture
def default_metadata_types(index, default_metadata_type_docs):
    # type: (Index, list) -> list
    for d in default_metadata_type_docs:
        index.metadata_types.add(index.metadata_types.from_doc(d))
    return index.metadata_types.get_all()


@pytest.fixture
def ga_metadata_type(index, ga_metadata_type_doc):
    return index.metadata_types.add(index.metadata_types.from_doc(ga_metadata_type_doc))


@pytest.fixture
def default_metadata_type(index, default_metadata_types):
    return index.metadata_types.get_by_name('eo')


@pytest.fixture
def telemetry_metadata_type(index, default_metadata_types):
    return index.metadata_types.get_by_name('telemetry')


@pytest.fixture
def indexed_ls5_scene_dataset_types(index, ga_metadata_type):
    """
    :type index: datacube.index._api.Index
    :rtype: datacube.model.StorageType
    """

    dataset_types = load_test_dataset_types(
        DATASET_TYPES / 'ls5_scenes.yaml',
        # Use our larger metadata type with a more diverse set of field types.
        metadata_type=ga_metadata_type
    )

    types = []
    for dataset_type in dataset_types:
        types.append(index.products.add_document(dataset_type))

    return types


@pytest.fixture
def example_ls5_nbar_metadata_doc():
    return load_yaml_file(_EXAMPLE_LS5_NBAR_DATASET_FILE)[0]


def load_test_dataset_types(filename, metadata_type=None):
    types = load_yaml_file(filename)
    return [alter_dataset_type_for_testing(type_, metadata_type=metadata_type) for type_ in types]


def load_yaml_file(filename):
    with open(str(filename)) as f:
        return list(yaml.load_all(f, Loader=SafeLoader))


def alter_dataset_type_for_testing(type_, metadata_type=None):
    if 'measurements' in type_:
        type_ = limit_num_measurements(type_)
    if 'storage' in type_:
        type_ = shrink_storage_type(type_,
                                    GEOGRAPHIC_VARS if is_geogaphic(type_) else PROJECTED_VARS,
                                    TEST_STORAGE_SHRINK_FACTORS)
    if metadata_type:
        type_['metadata_type'] = metadata_type.name
    return type_


def limit_num_measurements(storage_type):
    measurements = storage_type['measurements']
    if len(measurements) > TEST_STORAGE_NUM_MEASUREMENTS:
        storage_type['measurements'] = measurements[:TEST_STORAGE_NUM_MEASUREMENTS]
    return storage_type


def use_test_storage(storage_type):
    storage_type['location_name'] = 'testdata'
    return storage_type


def is_geogaphic(storage_type):
    return 'latitude' in storage_type['storage']['resolution']


def shrink_storage_type(storage_type, variables, shrink_factors):
    storage = storage_type['storage']
    for var in variables:
        storage['resolution'][var] = storage['resolution'][var] * shrink_factors[0]
        storage['chunking'][var] = storage['chunking'][var] / shrink_factors[1]
    return storage_type
