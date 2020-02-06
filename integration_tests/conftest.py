# coding=utf-8
"""
Common methods for index integration tests.
"""
import itertools
import os
from copy import copy, deepcopy
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml
from click.testing import CliRunner
from hypothesis import HealthCheck, settings

import datacube.scripts.cli_app
import datacube.utils
from datacube.drivers.postgres import _core
from datacube.index import index_connect
from datacube.index._metadata_types import default_metadata_type_docs
from integration_tests.utils import _make_geotiffs, _make_ls5_scene_datasets, load_yaml_file, \
    GEOTIFF, load_test_products

from datacube.config import LocalConfig
from datacube.drivers.postgres import PostgresDb

_SINGLE_RUN_CONFIG_TEMPLATE = """

"""

INTEGRATION_TESTS_DIR = Path(__file__).parent

_EXAMPLE_LS5_NBAR_DATASET_FILE = INTEGRATION_TESTS_DIR / 'example-ls5-nbar.yaml'

#: Number of time slices to create in sample data
NUM_TIME_SLICES = 3

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs' / 'config_samples'

CONFIG_FILE_PATHS = [str(INTEGRATION_TESTS_DIR / 'agdcintegration.conf'),
                     os.path.expanduser('~/.datacube_integration.conf')]

# Configure Hypothesis to allow slower tests, because we're testing datasets
# and disk IO rather than scalar values in memory.  Ask @Zac-HD for details.
settings.register_profile(
    'opendatacube', deadline=5000, max_examples=10,
    suppress_health_check=[HealthCheck.too_slow]
)
settings.load_profile('opendatacube')


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
def index(local_config,
          uninitialised_postgres_db: PostgresDb):
    index = index_connect(local_config, validate_connection=False)
    index.init_db()
    return index


@pytest.fixture
def index_empty(local_config, uninitialised_postgres_db: PostgresDb):
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
    Clear any dynamically created postgresql indexes from the schema.
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
    :return: List of dictionaries like::

        {
            'day':..., # compact day string, e.g. `19900302`
            'uuid':..., # a unique UUID for this dataset (i.e. specific day)
            'path':..., # path to the yaml ingestion file
            'tiffs':... # list of paths to the actual geotiffs in that dataset, one per band.
        }

    """
    tiffs_dir = tmpdir_factory.mktemp('tiffs')

    config = load_yaml_file(_EXAMPLE_LS5_NBAR_DATASET_FILE)[0]

    # Customise the spatial coordinates
    ul = GEOTIFF['ul']
    lr = {
        dim: ul[dim] + GEOTIFF['shape'][dim] * GEOTIFF['pixel_size'][dim]
        for dim in ('x', 'y')
    }
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
    :param int day_offset: how many days to offset the original yaml by.
    """
    config = deepcopy(config)

    # Increment all dates by the day_offset
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
    dataset_dirs = _make_ls5_scene_datasets(geotiffs, tmpdir)
    return dataset_dirs


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
def clirunner_raw():
    def _run_cli(opts,
                 catch_exceptions=False,
                 expect_success=True,
                 cli_method=datacube.scripts.cli_app.cli,
                 verbose_flag='-v'):
        exe_opts = []
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
    B = INTEGRATION_TESTS_DIR / 'data' / 'dataset_add'
    return SimpleNamespace(metadata=str(B / 'metadata.yml'),
                           products=str(B / 'products.yml'),
                           datasets_bad1=str(B / 'datasets_bad1.yml'),
                           datasets=str(B / 'datasets.yml'))
