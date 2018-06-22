# coding=utf-8
"""
Common methods for index integration tests.
"""
from __future__ import absolute_import

import itertools
import os
import shutil
from copy import copy, deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4
from types import SimpleNamespace

import numpy as np
import pytest
import rasterio
import yaml
from click.testing import CliRunner

import datacube.scripts.cli_app
import datacube.utils
from datacube.drivers.postgres import _core
from datacube.index import index_connect
from datacube.index._metadata_types import default_metadata_type_docs

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

from datacube.config import LocalConfig
from datacube.index.index import Index
from datacube.drivers.postgres import PostgresDb

# On Windows, symlinks are not supported in Python 2 and require
# specific privileges otherwise, so we copy instead of linking
if os.name == 'nt' or not hasattr(os, 'symlink'):
    symlink = shutil.copy
else:
    symlink = os.symlink

_SINGLE_RUN_CONFIG_TEMPLATE = """

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
INTEGRATION_TESTS_DIR = Path(__file__).parent

_EXAMPLE_LS5_NBAR_DATASET_FILE = INTEGRATION_TESTS_DIR / 'example-ls5-nbar.yaml'

#: Number of time slices to create in sample data
NUM_TIME_SLICES = 3

#: Number of bands to place in generated GeoTIFFs
NUM_BANDS = 3

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs' / 'config_samples'

LS5_SAMPLES = CONFIG_SAMPLES / 'storage_types' / 'ga_landsat_5'
LS5_NBAR_STORAGE_TYPE = LS5_SAMPLES / 'ls5_geographic.yaml'
LS5_NBAR_ALBERS_STORAGE_TYPE = LS5_SAMPLES / 'ls5_albers.yaml'

# Resolution and chunking shrink factors
TEST_STORAGE_SHRINK_FACTORS = (100, 100)
TEST_STORAGE_NUM_MEASUREMENTS = 2
GEOGRAPHIC_VARS = ('latitude', 'longitude')
PROJECTED_VARS = ('x', 'y')

EXAMPLE_LS5_DATASET_ID = UUID('bbf3e21c-82b0-11e5-9ba1-a0000100fe80')

# def pytest_generate_tests(metafunc):
#     if "fixture_param" in metafunc.fixturenames:
#         metafunc.parametrize("fixture_param", ["foo"], scope="module")
#     idlist = []
#     argvalues = []
#     for scenario in metafunc.cls.scenarios:
#         idlist.append(scenario[0])
#         items = scenario[1].items()
#         argnames = [x[0] for x in items]
#         argvalues.append(([x[1] for x in items]))
#     metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")

CONFIG_FILE_PATHS = [str(INTEGRATION_TESTS_DIR / 'agdcintegration.conf'),
                     os.path.expanduser('~/.datacube_integration.conf')]


@pytest.fixture
def global_integration_cli_args():
    """
    The first arguments to pass to a cli command for integration test configuration.
    """
    # List of a config files in order.
    return list(itertools.chain(*(('--config', f) for f in CONFIG_FILE_PATHS)))


@pytest.fixture
def datacube_env_name(request):
    if hasattr(request, 'param'):
        return request.param
    else:
        return 'datacube'


@pytest.fixture
def local_config(datacube_env_name):
    """Provides a :class:`LocalConfig` configured with suitable config file paths.

    .. seealso::

        The :func:`integration_config_paths` fixture sets up the config files.
    """
    return LocalConfig.find(CONFIG_FILE_PATHS, env=datacube_env_name)


@pytest.fixture
def ingest_configs(datacube_env_name):
    """ Provides dictionary product_name => config file name
    """
    if datacube_env_name == "s3aio_env":
        return {
            'ls5_nbar_albers': 'ls5_nbar_albers_s3test.yaml',
            'ls5_pq_albers': 'ls5_pq_albers_s3test.yaml',
        }

    return {
        'ls5_nbar_albers': 'ls5_nbar_albers.yaml',
        'ls5_pq_albers': 'ls5_pq_albers.yaml',
    }


@pytest.fixture(params=["US/Pacific", "UTC", ])
def uninitialised_postgres_db(local_config, request):
    """
    Return a connection to an empty PostgreSQL database
    """
    timezone = request.param

    db = PostgresDb.from_config(local_config,
                                application_name='test-run',
                                validate_connection=False)

    # Drop tables so our tests have a clean db.
    # with db.begin() as c:  # Creates a new PostgresDbAPI, by passing a new connection to it
    _core.drop_db(db._engine)
    db._engine.execute('alter database %s set timezone = %r' % (local_config['db_database'], timezone))

    # We need to run this as well, I think because SQLAlchemy grabs them into it's MetaData,
    # and attempts to recreate them. WTF TODO FIX
    remove_dynamic_indexes()

    yield db
    # with db.begin() as c:  # Drop SCHEMA
    _core.drop_db(db._engine)
    db.close()


@pytest.fixture
def index(local_config, uninitialised_postgres_db):
    """
    :type initialised_postgres_db: datacube.drivers.postgres._connections.PostgresDb
    """
    index = index_connect(local_config, validate_connection=False)
    index.init_db()
    return index


@pytest.fixture
def index_empty(local_config, uninitialised_postgres_db):
    """
    :type initialised_postgres_db: datacube.drivers.postgres._connections.PostgresDb
    """
    index = index_connect(local_config, validate_connection=False)
    index.init_db(with_default_types=False)
    return index


@pytest.fixture
def initialised_postgres_db(index):
    """
    Return a connection to an PostgreSQL database, initialised with the default schema
    and tables.
    """
    return index._db


def remove_dynamic_indexes():
    """
    Clear any dynamically created indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in _core.METADATA.tables.values():
        table.indexes.intersection_update([i for i in table.indexes if not i.name.startswith('dix_')])


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
            for day_offset in range(NUM_TIME_SLICES)]


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
    width = GEOTIFF['shape']['x']
    height = GEOTIFF['shape']['y']
    metadata = {'count': 1,
                'crs': GEOTIFF['crs'],
                'driver': 'GTiff',
                'dtype': 'int16',
                'width': width,
                'height': height,
                'nodata': -999.0,
                'transform': [GEOTIFF['pixel_size']['x'],
                              0.0,
                              GEOTIFF['ul']['x'],
                              0.0,
                              GEOTIFF['pixel_size']['y'],
                              GEOTIFF['ul']['y']]}

    for band in range(NUM_BANDS):
        path = str(tiffs_dir.join('band%02d_time%02d.tif' % ((band + 1), day_offset)))
        with rasterio.open(path, 'w', **metadata) as dst:
            # Write data in "corners" (rounded down by 100, for a size of 100x100)
            data = np.zeros((height, width), dtype=np.int16)
            data[:] = np.arange(height * width).reshape((height, width)) + 10 * band + day_offset
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
        for band in range(NUM_BANDS):
            path = scene_dir.join(geotiff_name.format(band + 1))
            symlink(str(geotiff['tiffs'][band]), str(path))
        dataset_dirs[geotiff['uuid']] = Path(str(obs_dir))
    return dataset_dirs


def create_empty_geotiff(path):
    # Example method, not used
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
def default_metadata_type_doc():
    return [doc for doc in default_metadata_type_docs() if doc['name'] == 'eo'][0]


@pytest.fixture
def telemetry_metadata_type_doc():
    return [doc for doc in default_metadata_type_docs() if doc['name'] == 'telemetry'][0]


@pytest.fixture
def ga_metadata_type_doc():
    _FULL_EO_METADATA = Path(__file__).parent.joinpath('extensive-eo-metadata.yaml')
    [(path, eo_md_type)] = datacube.utils.read_documents(_FULL_EO_METADATA)
    return eo_md_type


@pytest.fixture
def default_metadata_types(index):
    """Inserts the default metadata types into the Index"""
    # type: (Index, list) -> list
    for d in default_metadata_type_docs():
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
def indexed_ls5_scene_products(index, ga_metadata_type):
    """Add Landsat 5 scene Products into the Index"""
    products = load_test_products(
        CONFIG_SAMPLES / 'dataset_types' / 'ls5_scenes.yaml',
        # Use our larger metadata type with a more diverse set of field types.
        metadata_type=ga_metadata_type
    )

    types = []
    for product in products:
        types.append(index.products.add_document(product))

    return types


@pytest.fixture
def example_ls5_nbar_metadata_doc():
    return load_yaml_file(_EXAMPLE_LS5_NBAR_DATASET_FILE)[0]


def load_test_products(filename, metadata_type=None):
    dataset_types = load_yaml_file(filename)
    return [alter_product_for_testing(dataset_type, metadata_type=metadata_type) for dataset_type in dataset_types]


def load_yaml_file(filename):
    with open(str(filename)) as f:
        return list(yaml.load_all(f, Loader=SafeLoader))


def alter_product_for_testing(product, metadata_type=None):
    limit_num_measurements(product)
    if 'storage' in product:
        product = shrink_storage_type(product,
                                      GEOGRAPHIC_VARS if is_geogaphic(product) else PROJECTED_VARS,
                                      TEST_STORAGE_SHRINK_FACTORS)
    if metadata_type:
        product['metadata_type'] = metadata_type.name
    return product


def limit_num_measurements(dataset_type):
    if 'measurements' not in dataset_type:
        return
    measurements = dataset_type['measurements']
    if len(measurements) > TEST_STORAGE_NUM_MEASUREMENTS:
        dataset_type['measurements'] = measurements[:TEST_STORAGE_NUM_MEASUREMENTS]
    return dataset_type


def is_geogaphic(storage_type):
    return 'latitude' in storage_type['storage']['resolution']


def shrink_storage_type(storage_type, variables, shrink_factors):
    storage = storage_type['storage']
    for var in variables:
        storage['resolution'][var] = storage['resolution'][var] * shrink_factors[0]
        storage['chunking'][var] = storage['chunking'][var] / shrink_factors[1]
    return storage_type


@pytest.fixture
def clirunner(global_integration_cli_args, datacube_env_name):
    def _run_cli(opts, catch_exceptions=False,
                 expect_success=True, cli_method=datacube.scripts.cli_app.cli,
                 verbose_flag='-v'):
        exe_opts = list(itertools.chain(*(('--config', f) for f in CONFIG_FILE_PATHS)))
        exe_opts += ['--env', datacube_env_name]
        if verbose_flag:
            exe_opts.append(verbose_flag)
        exe_opts.extend(opts)

        runner = CliRunner()
        result = runner.invoke(
            cli_method,
            exe_opts,
            catch_exceptions=catch_exceptions
        )
        if expect_success:
            assert 0 == result.exit_code, "Error for %r. output: %r" % (opts, result.output)
        return result

    return _run_cli


@pytest.fixture
def dataset_add_configs():
    B = INTEGRATION_TESTS_DIR/'data'/'dataset_add'
    return SimpleNamespace(metadata=str(B/'metadata.yml'),
                           products=str(B/'products.yml'),
                           datasets=str(B/'datasets.yml'))


def edit_for_fast_ingest(config):
    config = alter_product_for_testing(config)
    config['storage']['crs'] = 'EPSG:28355'
    config['storage']['chunking']['time'] = 1
    return config


def edit_for_end2end(config):
    storage = config.get('storage', {})

    storage['crs'] = 'EPSG:3577'
    storage['tile_size']['x'] = 100000.0
    storage['tile_size']['y'] = 100000.0

    config['storage'] = storage
    return config


def prepare_test_ingestion_configuration(tmpdir,
                                         output_dir,
                                         filename,
                                         mode=None):
    customizers = {
        'fast_ingest': edit_for_fast_ingest,
        'end2end': edit_for_end2end,
    }

    filename = Path(filename)
    if output_dir is None:
        output_dir = tmpdir.mkdir(filename.stem)
    config = load_yaml_file(filename)[0]

    if mode is not None:
        if mode not in customizers:
            raise ValueError('Wrong mode: ' + mode)
        config = customizers[mode](config)

    config['location'] = str(output_dir)

    # If ingesting with the s3test driver
    if 'bucket' in config['storage']:
        config['storage']['bucket'] = str(output_dir)

    config_path = tmpdir.join(filename.name)
    with open(str(config_path), 'w') as stream:
        yaml.dump(config, stream)
    return config_path, config
