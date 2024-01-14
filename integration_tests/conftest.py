# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Common methods for index integration tests.
"""
import itertools
import os
from copy import copy, deepcopy
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Union
from uuid import uuid4

import pytest
import yaml
from click.testing import CliRunner
from hypothesis import HealthCheck, settings
from sqlalchemy import text

import datacube.scripts.cli_app
import datacube.utils
from datacube.drivers.postgres import _core as pgres_core
from datacube.drivers.postgis import _core as pgis_core
from datacube.index import index_connect
from datacube.index.abstract import default_metadata_type_docs
from integration_tests.utils import _make_geotiffs, _make_ls5_scene_datasets, load_yaml_file, \
    GEOTIFF, load_test_products

from datacube.cfg import ODCConfig, ODCEnvironment, psql_url_from_config
from datacube.drivers.postgres import PostgresDb
from datacube.drivers.postgis import PostGisDb
from datacube.model import LineageTree, LineageDirection

_SINGLE_RUN_CONFIG_TEMPLATE = """

"""

INTEGRATION_TESTS_DIR = Path(__file__).parent

_EXAMPLE_LS5_NBAR_DATASET_FILE = INTEGRATION_TESTS_DIR / 'example-ls5-nbar.yaml'

#: Number of time slices to create in sample data
NUM_TIME_SLICES = 3

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs' / 'config_samples'

CONFIG_FILE_PATHS = [
    os.path.expanduser('~/.datacube_integration.conf'),
    str(INTEGRATION_TESTS_DIR / 'integration.conf'),
]

# Configure Hypothesis to allow slower tests, because we're testing datasets
# and disk IO rather than scalar values in memory.  Ask @Zac-HD for details.
settings.register_profile(
    'opendatacube', deadline=5000, max_examples=10,
    suppress_health_check=[HealthCheck.too_slow]
)
settings.load_profile('opendatacube')

EO3_TESTDIR = INTEGRATION_TESTS_DIR / 'data' / 'eo3'


def get_eo3_test_data_doc(path):
    from datacube.utils import read_documents
    for path, doc in read_documents(EO3_TESTDIR / path):
        return doc


@pytest.fixture
def ext_eo3_mdt_path():
    return str(EO3_TESTDIR / "eo3_landsat_ard.odc-type.yaml")


@pytest.fixture
def eo3_product_paths():
    return [
        str(EO3_TESTDIR / "ard_ls8.odc-product.yaml"),
        str(EO3_TESTDIR / "ga_ls_wo_3.odc-product.yaml"),
        str(EO3_TESTDIR / "s2_africa_product.yaml"),
    ]


@pytest.fixture
def eo3_dataset_paths():
    return [
        str(EO3_TESTDIR / "ls8_dataset.yaml"),
        str(EO3_TESTDIR / "ls8_dataset2.yaml"),
        str(EO3_TESTDIR / "ls8_dataset3.yaml"),
        str(EO3_TESTDIR / "ls8_dataset4.yaml"),
        str(EO3_TESTDIR / "wo_dataset.yaml"),
        str(EO3_TESTDIR / "s2_africa_dataset.yaml"),
    ]


@pytest.fixture
def eo3_dataset_update_path():
    return str(EO3_TESTDIR / "ls8_dataset_update.yaml")


@pytest.fixture
def dataset_with_lineage_doc():
    return (
        get_eo3_test_data_doc("wo_ds_with_lineage.odc-metadata.yaml"),
        's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/090/086/2016/05/12/'
        'ga_ls_wo_3_090086_2016-05-12_final.stac-item.json'
    )


@pytest.fixture
def eo3_ls8_dataset_doc():
    return (
        get_eo3_test_data_doc("ls8_dataset.yaml"),
        's3://dea-public-data/baseline/ga_ls8c_ard_3/090/086/2016/05/12/'
        'ga_ls8c_ard_3-0-0_090086_2016-05-12_final.stac-item.json'
    )


@pytest.fixture
def eo3_ls8_dataset2_doc():
    return (
        get_eo3_test_data_doc("ls8_dataset2.yaml"),
        's3://dea-public-data/baseline/ga_ls8c_ard_3/090/086/2016/05/28/'
        'ga_ls8c_ard_3-0-0_090086_2016-05-28_final.stac-item.json'
    )


@pytest.fixture
def eo3_ls8_dataset3_doc():
    return (
        get_eo3_test_data_doc("ls8_dataset3.yaml"),
        's3://dea-public-data/baseline/ga_ls8c_ard_3/101/077/2013/04/04/'
        'ga_ls8c_ard_3-0-0_101077_2013-04-04_final.stac-item.json'
    )


@pytest.fixture
def eo3_ls8_dataset4_doc():
    return (
        get_eo3_test_data_doc("ls8_dataset4.yaml"),
        's3://dea-public-data/baseline/ga_ls8c_ard_3/101/077/2013/07/21/'
        'ga_ls8c_ard_3-0-0_101077_2013-07-21_final.stac-item.json'
    )


@pytest.fixture
def eo3_wo_dataset_doc():
    return (
        get_eo3_test_data_doc("wo_dataset.yaml"),
        's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/090/086/2016/05/12/'
        'ga_ls_wo_3_090086_2016-05-12_final.stac-item.json'
    )


@pytest.fixture
def eo3_africa_dataset_doc():
    return (
        get_eo3_test_data_doc("s2_africa_dataset.yaml"),
        's3://deafrica-sentinel-2/sentinel-s2-l2a-cogs/37/M/CQ/'
        '2022/8/S2A_37MCQ_20220808_0_L2A/S2A_37MCQ_20220808_0_L2A.json'
    )


@pytest.fixture
def datasets_with_unembedded_lineage_doc():
    return [
        (
            get_eo3_test_data_doc("ls8_dataset.yaml"),
            's3://dea-public-data/baseline/ga_ls8c_ard_3/090/086/2016/05/12/'
            'ga_ls8c_ard_3-0-0_090086_2016-05-12_final.stac-item.json'
        ),
        (
            get_eo3_test_data_doc("wo_dataset.yaml"),
            's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/090/086/2016/05/12/'
            'ga_ls_wo_3_090086_2016-05-12_final.stac-item.json'
        ),
    ]


@pytest.fixture
def extended_eo3_metadata_type_doc():
    return get_eo3_test_data_doc("eo3_landsat_ard.odc-type.yaml")


@pytest.fixture
def eo3_sentinel_metadata_type_doc():
    return get_eo3_test_data_doc("eo3_sentinel_ard.odc-type.yaml")


@pytest.fixture
def extended_eo3_product_doc():
    return get_eo3_test_data_doc("ard_ls8.odc-product.yaml")


@pytest.fixture
def base_eo3_product_doc():
    return get_eo3_test_data_doc("ga_ls_wo_3.odc-product.yaml")


@pytest.fixture
def africa_s2_product_doc():
    return get_eo3_test_data_doc("s2_africa_product.yaml")


@pytest.fixture
def s2_ard_product_doc():
    return get_eo3_test_data_doc("ga_s2am_ard_3.odc-product.yaml")


@pytest.fixture
def final_dataset_doc():
    return (
        get_eo3_test_data_doc("final_dataset.yaml"),
        's3://dea-public-data/baseline/ga_ls8c_ard_3/090/086/2023/04/30'
        'ga_ls8c_ard_3-2-1_090086_2023-04-30_final.stac-item.json'
    )


@pytest.fixture
def nrt_dataset_doc():
    return (
        get_eo3_test_data_doc("nrt_dataset.yaml"),
        's3://dea-public-data/baseline/ga_ls8c_ard_3/090/086/2023/04/30_nrt'
        'ga_ls8c_ard_3-2-1_090086_2023-04-30_nrt.stac-item.json'
    )


@pytest.fixture
def ga_s2am_ard_3_interim_doc():
    return (
        get_eo3_test_data_doc("ga_s2am_ard_3_interim.yaml"),
        's3://dea-public-data/baseline/ga_s2am_ard_3/53/JNN/2021/07/24_interim'
        '20230222T235626/ga_s2am_ard_3-2-1_53JNN_2021-07-24_interim.odc-metadata.yaml'
    )


@pytest.fixture
def ga_s2am_ard_3_final_doc():
    return (
        get_eo3_test_data_doc("ga_s2am_ard_3_final.yaml"),
        's3://dea-public-data/baseline/ga_s2am_ard_3/53/JNN/2021/07/24'
        '20210724T023436/ga_s2am_ard_3-2-1_53JNN_2021-07-24_final.odc-metadata.yaml'
    )


def doc_to_ds(index, product_name, ds_doc, ds_path, src_tree=None, derived_tree=None):
    from datacube.index.hl import Doc2Dataset
    resolver = Doc2Dataset(index, products=[product_name], verify_lineage=False)
    ds, err = resolver(ds_doc, ds_path)
    assert err is None and ds is not None
    if index.supports_external_lineage:
        index.datasets.add(ds, with_lineage=False)
        if src_tree:
            index.lineage.add(src_tree)
        if derived_tree:
            index.lineage.add(derived_tree)
        if not (src_tree or derived_tree):
            eo3_tree = LineageTree.from_eo3_doc(ds_doc)
            index.lineage.add(eo3_tree)
    else:
        index.datasets.add(ds, with_lineage=index.supports_lineage)
    return index.datasets.get(ds.id)


def doc_to_ds_no_add(index, product_name, ds_doc, ds_path):
    from datacube.index.hl import Doc2Dataset
    resolver = Doc2Dataset(index, products=[product_name], verify_lineage=False)
    ds, err = resolver(ds_doc, ds_path)
    assert err is None and ds is not None
    return ds


@pytest.fixture
def extended_eo3_metadata_type(index, extended_eo3_metadata_type_doc):
    return index.metadata_types.add(
        index.metadata_types.from_doc(extended_eo3_metadata_type_doc)
    )


@pytest.fixture
def eo3_sentinel_metadata_type(index, eo3_sentinel_metadata_type_doc):
    return index.metadata_types.add(
        index.metadata_types.from_doc(eo3_sentinel_metadata_type_doc)
    )


@pytest.fixture
def ls8_eo3_product(index, extended_eo3_metadata_type, extended_eo3_product_doc):
    return index.products.add_document(extended_eo3_product_doc)


@pytest.fixture
def wo_eo3_product(index, base_eo3_product_doc):
    return index.products.add_document(base_eo3_product_doc)


@pytest.fixture
def africa_s2_eo3_product(index, africa_s2_product_doc):
    return index.products.add_document(africa_s2_product_doc)


@pytest.fixture
def ga_s2am_ard_3_product(index, eo3_sentinel_metadata_type, s2_ard_product_doc):
    return index.products.add_document(s2_ard_product_doc)


@pytest.fixture
def eo3_products(index, extended_eo3_metadata_type,
                 ls8_eo3_product, wo_eo3_product,
                 africa_s2_eo3_product):
    return [
        africa_s2_eo3_product,
        ls8_eo3_product,
        wo_eo3_product,
    ]


@pytest.fixture
def ls8_eo3_dataset(index, extended_eo3_metadata_type, ls8_eo3_product, eo3_ls8_dataset_doc):
    return doc_to_ds(index,
                     ls8_eo3_product.name,
                     *eo3_ls8_dataset_doc)


@pytest.fixture
def ls8_eo3_dataset2(index, extended_eo3_metadata_type, ls8_eo3_product, eo3_ls8_dataset2_doc):
    return doc_to_ds(index,
                     ls8_eo3_product.name,
                     *eo3_ls8_dataset2_doc)


@pytest.fixture
def ls8_eo3_dataset3(index, extended_eo3_metadata_type, ls8_eo3_product, eo3_ls8_dataset3_doc):
    return doc_to_ds(index,
                     ls8_eo3_product.name,
                     *eo3_ls8_dataset3_doc)


@pytest.fixture
def ls8_eo3_dataset4(index, extended_eo3_metadata_type, ls8_eo3_product, eo3_ls8_dataset4_doc):
    return doc_to_ds(index,
                     ls8_eo3_product.name,
                     *eo3_ls8_dataset4_doc)


@pytest.fixture
def wo_eo3_dataset(index, wo_eo3_product, eo3_wo_dataset_doc, ls8_eo3_dataset):
    return doc_to_ds(index,
                     wo_eo3_product.name,
                     *eo3_wo_dataset_doc)


@pytest.fixture
def africa_eo3_dataset(index, africa_s2_eo3_product, eo3_africa_dataset_doc):
    return doc_to_ds(index,
                     africa_s2_eo3_product.name,
                     *eo3_africa_dataset_doc)


@pytest.fixture
def nrt_dataset(index, extended_eo3_metadata_type, ls8_eo3_product, nrt_dataset_doc):
    return doc_to_ds_no_add(
        index,
        ls8_eo3_product.name,
        *nrt_dataset_doc)


@pytest.fixture
def final_dataset(index, extended_eo3_metadata_type, ls8_eo3_product, final_dataset_doc):
    return doc_to_ds_no_add(
        index,
        ls8_eo3_product.name,
        *final_dataset_doc)


@pytest.fixture
def ds_no_region(index, extended_eo3_metadata_type, ls8_eo3_product, final_dataset_doc):
    doc_no_region = deepcopy(final_dataset_doc)
    doc_no_region[0]["properties"]["odc:region_code"] = None
    return doc_to_ds_no_add(
        index,
        ls8_eo3_product.name,
        *doc_no_region)


@pytest.fixture
def ga_s2am_ard3_final(index, eo3_sentinel_metadata_type, ga_s2am_ard_3_product, ga_s2am_ard_3_final_doc):
    return doc_to_ds_no_add(
        index,
        ga_s2am_ard_3_product.name,
        *ga_s2am_ard_3_final_doc)


@pytest.fixture
def ga_s2am_ard3_interim(index, eo3_sentinel_metadata_type, ga_s2am_ard_3_product, ga_s2am_ard_3_interim_doc):
    return doc_to_ds_no_add(
        index,
        ga_s2am_ard_3_product.name,
        *ga_s2am_ard_3_interim_doc)


@pytest.fixture
def mem_index_fresh(in_memory_config):
    from datacube import Datacube
    with Datacube(env=in_memory_config) as dc:
        yield dc


@pytest.fixture
def mem_index_eo3(mem_index_fresh,
                  extended_eo3_metadata_type_doc,
                  extended_eo3_product_doc,
                  base_eo3_product_doc):
    mem_index_fresh.index.metadata_types.add(
        mem_index_fresh.index.metadata_types.from_doc(extended_eo3_metadata_type_doc)
    )
    mem_index_fresh.index.products.add_document(base_eo3_product_doc)
    mem_index_fresh.index.products.add_document(extended_eo3_product_doc)
    return mem_index_fresh


@pytest.fixture
def mem_eo3_data(mem_index_eo3, datasets_with_unembedded_lineage_doc):
    (doc_ls8, loc_ls8), (doc_wo, loc_wo) = datasets_with_unembedded_lineage_doc
    from datacube.index.hl import Doc2Dataset
    resolver = Doc2Dataset(mem_index_eo3.index)
    ds_ls8, err = resolver(doc_ls8, loc_ls8)
    mem_index_eo3.index.datasets.add(ds_ls8)
    ds_wo, err = resolver(doc_wo, loc_wo)
    mem_index_eo3.index.datasets.add(ds_wo)
    return mem_index_eo3, ds_ls8.id, ds_wo.id


@pytest.fixture(scope="module", params=["datacube", "experimental"])
def datacube_env_name(request):
    return request.param


@pytest.fixture(params=[("datacube", "experimental"), ("experimental", "datacube")])
def datacube_env_name_pair(request) -> tuple[str, str]:
    return request.param


@pytest.fixture
def odc_config() -> ODCConfig:
    return ODCConfig(paths=CONFIG_FILE_PATHS)


@pytest.fixture
def cfg_env(odc_config, datacube_env_name: str) -> ODCEnvironment:
    """Provides a :class:`ODCEnvironment` configured with suitable config file paths."""
    return odc_config[datacube_env_name]


@pytest.fixture
def cfg_env_pair(odc_config: ODCConfig, datacube_env_name_pair: tuple[str, str]
                 ) -> tuple[ODCEnvironment, ODCEnvironment]:
    """Provides a pair of :class:`ODCEnvironment` configured with suitable config file paths.
    """
    return tuple(odc_config[env] for env in datacube_env_name_pair)


@pytest.fixture
def null_config(odc_config: ODCConfig) -> ODCEnvironment:
    """Provides a :class:`ODCEnvironment` configured with null index driver
    """
    return odc_config["nulldriver"]


@pytest.fixture
def in_memory_config(odc_config: ODCConfig) -> ODCEnvironment:
    """Provides a :class:`ODCEnvironment` configured with memory index driver
    """
    return odc_config["localmemory"]


def reset_db(cfg_env: ODCEnvironment, tz=None) -> PostgresDb | PostGisDb:
    from urllib.parse import urlparse
    url = psql_url_from_config(cfg_env)
    url_components = urlparse(url)
    db_name = url_components.path[1:]
    if cfg_env._name in ('datacube', 'default', 'postgres'):
        db = PostgresDb.from_config(
            cfg_env,
            application_name='test-run',
            validate_connection=False
        )
        # Drop tables so our tests have a clean db.
        # with db.begin() as c:  # Creates a new PostgresDbAPI, by passing a new connection to it
        with db._engine.connect() as connection:
            pgres_core.drop_db(connection)
            if tz:
                connection.execute(text(f'alter database {db_name} set timezone = {tz!r}'))
        # We need to run this as well, I think because SQLAlchemy grabs them into it's MetaData,
        # and attempts to recreate them. WTF TODO FIX
        remove_postgres_dynamic_indexes()
    else:
        db = PostGisDb.from_config(
            cfg_env,
            application_name='test-run',
            validate_connection=False
        )
        with db._engine.connect() as connection:
            pgis_core.drop_db(connection)
            if tz:
                connection.execute(text(f'alter database {db_name} set timezone = {tz!r}'))
        remove_postgis_dynamic_indexes()
    return db


def cleanup_db(cfg_env: ODCEnvironment, db: PostgresDb | PostGisDb):
    with db._engine.connect() as connection:
        if cfg_env._name in ('datacube', 'default', 'postgres'):
            # with db.begin() as c:  # Drop SCHEMA
            pgres_core.drop_db(connection)
        else:
            pgis_core.drop_db(connection)
    db.close()


@pytest.fixture(params=["US/Pacific", "UTC"])
def uninitialised_postgres_db(cfg_env: ODCEnvironment, request) -> PostgresDb | PostGisDb:
    """
    Return a connection to an empty PostgreSQL or PostGIS database
    """
    # Setup
    timezone = request.param
    db = reset_db(cfg_env, timezone)

    yield db

    # Cleanup
    cleanup_db(cfg_env, db)


@pytest.fixture
def uninitialised_postgres_db_pair(cfg_env_pair):
    """
    Return a pair connections to empty PostgreSQL or PostGIS databases
    """
    dbs = tuple(reset_db(cfg_env) for cfg_env in cfg_env_pair)

    yield dbs

    for local_cfg, db in zip(cfg_env_pair, dbs):
        cleanup_db(local_cfg, db)


@pytest.fixture
def index(cfg_env,
          uninitialised_postgres_db: Union[PostGisDb, PostgresDb]):
    index = index_connect(cfg_env, validate_connection=False)
    index.init_db()
    yield index
    del index


@pytest.fixture
def index_pair_populated_empty(cfg_env_pair, uninitialised_postgres_db_pair,
                               extended_eo3_metadata_type_doc,
                               base_eo3_product_doc, extended_eo3_product_doc, africa_s2_product_doc,
                               eo3_ls8_dataset_doc, eo3_ls8_dataset2_doc,
                               eo3_ls8_dataset3_doc, eo3_ls8_dataset4_doc,
                               eo3_wo_dataset_doc, eo3_africa_dataset_doc):
    populated_cfg, empty_cfg = cfg_env_pair
    populated_idx = index_connect(populated_cfg, validate_connection=False)
    empty_idx = index_connect(empty_cfg, validate_connection=False)
    populated_idx.init_db()
    empty_idx.init_db(with_default_types=False)
    assert list(empty_idx.products.get_all()) == []
    assert list(populated_idx.products.get_all()) == []
    # Populate the populated index
    populated_idx.metadata_types.add(
        populated_idx.metadata_types.from_doc(extended_eo3_metadata_type_doc)
    )
    for prod_doc in (base_eo3_product_doc, extended_eo3_product_doc, africa_s2_product_doc):
        populated_idx.products.add_document(prod_doc)
    for ds_doc, ds_path in (eo3_ls8_dataset_doc, eo3_ls8_dataset2_doc,
                            eo3_ls8_dataset3_doc, eo3_ls8_dataset4_doc,
                            eo3_wo_dataset_doc, eo3_africa_dataset_doc):
        doc_to_ds(populated_idx, ds_doc['product']['name'], ds_doc, ds_path)
    assert list(populated_idx.products.get_all()) != list(empty_idx.products.get_all())
    assert list(empty_idx.products.get_all()) == []
    assert list(populated_idx.products.get_all()) != []

    yield (populated_idx, empty_idx)

    del populated_idx
    del empty_idx


@pytest.fixture
def index_empty(cfg_env, uninitialised_postgres_db: Union[PostGisDb, PostgresDb]):
    index = index_connect(cfg_env, validate_connection=False)
    index.init_db(with_default_types=False)
    yield index
    del index


def remove_postgres_dynamic_indexes():
    """
    Clear any dynamically created postgresql indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in pgres_core.METADATA.tables.values():
        table.indexes.intersection_update([i for i in table.indexes if not i.name.startswith('dix_')])


def remove_postgis_dynamic_indexes():
    """
    Clear any dynamically created postgis indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    # for table in pgis_core.METADATA.tables.values():
    #    table.indexes.intersection_update([i for i in table.indexes if not i.name.startswith('dix_')])
    # Dynamic indexes disabled.


@pytest.fixture
def ls5_telem_doc(ga_metadata_type):
    return {
        "name": "ls5_telem_test",
        "description": 'LS5 Test',
        "license": "CC-BY-4.0",
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
def eo3_metadata_type_docs(eo3_base_metadata_type_doc, extended_eo3_metadata_type_doc):
    return [eo3_base_metadata_type_doc, extended_eo3_metadata_type_doc]


@pytest.fixture
def eo3_base_metadata_type_doc():
    return [doc for doc in default_metadata_type_docs() if doc['name'] == 'eo3'][0]


@pytest.fixture
def telemetry_metadata_type_doc():
    return [doc for doc in default_metadata_type_docs() if doc['name'] == 'telemetry'][0]


@pytest.fixture
def ga_metadata_type_doc():
    _FULL_EO_METADATA = Path(__file__).parent.joinpath('extensive-eo-metadata.yaml')  # noqa: N806
    [(path, eo_md_type)] = datacube.utils.read_documents(_FULL_EO_METADATA)
    return eo_md_type


@pytest.fixture
def default_metadata_types(index, eo3_metadata_type_docs):
    """Inserts the default metadata types into the Index"""
    if index.supports_legacy:
        type_docs = default_metadata_type_docs()
    else:
        type_docs = eo3_metadata_type_docs
    for d in type_docs:
        index.metadata_types.add(index.metadata_types.from_doc(d))
    return index.metadata_types.get_all()


@pytest.fixture
def ga_metadata_type(index, ga_metadata_type_doc):
    return index.metadata_types.add(index.metadata_types.from_doc(ga_metadata_type_doc))


@pytest.fixture
def default_metadata_type(index, default_metadata_types):
    if index.supports_legacy:
        return index.metadata_types.get_by_name('eo')
    else:
        return index.metadata_types.get_by_name('eo3')


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
def clirunner(datacube_env_name):
    def _run_cli(opts, catch_exceptions=False,
                 expect_success=True, cli_method=datacube.scripts.cli_app.cli,
                 skip_env=False, skip_config_paths=False,
                 mix_stderr=True,
                 verbose_flag='-v'):
        # If raw config passed in, skip default test config
        if not skip_config_paths:
            exe_opts = list(itertools.chain(*(('--config', f) for f in CONFIG_FILE_PATHS)))
        if not skip_env:
            exe_opts += ['--env', datacube_env_name]
        if verbose_flag:
            exe_opts.append(verbose_flag)
        exe_opts.extend(opts)

        runner = CliRunner(mix_stderr=mix_stderr)
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
                           datasets_no_id=str(B / 'datasets_no_id.yml'),
                           datasets_eo3=str(B / 'datasets_eo3.yml'),
                           datasets_eo3_updated=str(B / 'datasets_eo3_updated.yml'),
                           datasets=str(B / 'datasets.yml'),
                           empty_file=str(B / 'empty_file.yml'))


@pytest.fixture
def src_tree_ids():
    return {
        "root": uuid4(),
        "ard1": uuid4(),
        "ard2": uuid4(),

        "l1_1": uuid4(),
        "l1_2": uuid4(),
        "l1_3": uuid4(),

        "l1_4": uuid4(),
        "l1_5": uuid4(),
        "l1_6": uuid4(),

        "atmos": uuid4(),
        "atmos_parent": uuid4()
    }


@pytest.fixture
def src_lineage_tree(src_tree_ids):
    ids = src_tree_ids
    direction = LineageDirection.SOURCES
    return LineageTree(
        dataset_id=ids["root"], direction=direction,
        children={
            "ard": [
                LineageTree(
                    dataset_id=ids["ard1"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_1"], direction=direction,
                                home="level1",
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_2"], direction=direction,
                                home="level1",
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_3"], direction=direction,
                                home="level1",
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                home="anciliary",
                                children=None
                            )
                        ],
                    }
                ),
                LineageTree(
                    dataset_id=ids["ard2"], direction=direction,
                    children={
                        "l1": [
                            LineageTree(
                                dataset_id=ids["l1_4"], direction=direction,
                                home="level1",
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_5"], direction=direction,
                                home="level1",
                                children={}
                            ),
                            LineageTree(
                                dataset_id=ids["l1_6"], direction=direction,
                                home="level1",
                                children={}
                            ),
                        ],
                        "atmos_corr": [
                            LineageTree(
                                dataset_id=ids["atmos"], direction=direction,
                                home="anciliary",
                                children={
                                    "preatmos": [
                                        LineageTree(
                                            dataset_id=ids["atmos_parent"], direction=direction,
                                            home="anciliary",
                                            children={}
                                        )
                                    ]
                                }
                            )
                        ],
                    }
                ),
            ]
        }
    ), ids


@pytest.fixture
def compatible_derived_tree(src_tree_ids):
    ids = src_tree_ids.copy()
    ids.update({
        "atmos_grandparent": uuid4(),
        "ard3": uuid4(),
        "ard4": uuid4(),
        "leaf_1": uuid4(),
        "leaf_2": uuid4(),
        "leaf_3": uuid4(),
        "child_of_root": uuid4(),
        "grandchild_of_root": uuid4(),
    })
    tree = LineageTree(
        dataset_id=ids["atmos_grandparent"],
        direction=LineageDirection.DERIVED,
        home="steves_basement",
        children={
            "spam": [
                LineageTree(
                    dataset_id=ids["atmos_parent"],
                    direction=LineageDirection.DERIVED,
                    home="anciliary",
                    children={
                        "preatmos": [
                            LineageTree(
                                dataset_id=ids["atmos"],
                                direction=LineageDirection.DERIVED,
                                home="anciliary",
                                children={
                                    "atmos_corr": [
                                        LineageTree(
                                            dataset_id=ids["ard1"],
                                            direction=LineageDirection.DERIVED,
                                            home="ard",
                                            children={
                                                "ard": [
                                                    LineageTree(
                                                        dataset_id=ids["root"],
                                                        direction=LineageDirection.DERIVED,
                                                        home="extensions", children={
                                                            "dra": [
                                                                LineageTree(
                                                                    dataset_id=ids["child_of_root"],
                                                                    direction=LineageDirection.DERIVED,
                                                                    home="extensions", children={
                                                                        "rad": [
                                                                            LineageTree(
                                                                                dataset_id=ids["grandchild_of_root"],
                                                                                direction=LineageDirection.DERIVED,
                                                                                home="extensions", children={}
                                                                            )

                                                                        ]
                                                                    }
                                                                )
                                                            ]
                                                        }
                                                    ),
                                                    LineageTree(
                                                        dataset_id=ids["leaf_1"],
                                                        direction=LineageDirection.DERIVED,
                                                        home="extensions", children={}
                                                    ),
                                                ]
                                            }
                                        ),
                                        LineageTree(
                                            dataset_id=ids["ard2"],
                                            direction=LineageDirection.DERIVED,
                                            home="ard",
                                            children={}
                                        ),
                                        LineageTree(
                                            dataset_id=ids["ard3"],
                                            direction=LineageDirection.DERIVED,
                                            home="ard",
                                            children={
                                                "ard": [
                                                    LineageTree(
                                                        dataset_id=ids["leaf_2"],
                                                        direction=LineageDirection.DERIVED,
                                                        home="extensions", children={}
                                                    ),
                                                    LineageTree(
                                                        dataset_id=ids["leaf_3"],
                                                        direction=LineageDirection.DERIVED,
                                                        home="extensions", children={}
                                                    ),
                                                ]
                                            }
                                        ),
                                        LineageTree(
                                            dataset_id=ids["ard4"],
                                            direction=LineageDirection.DERIVED,
                                            home="ard",
                                            children={}
                                        ),
                                    ]
                                }
                            )
                        ]
                    }
                )
            ]
        }
    )
    return tree, ids


@pytest.fixture
def dataset_with_external_lineage(index,
                                  src_lineage_tree, compatible_derived_tree,
                                  ls8_eo3_product, eo3_ls8_dataset_doc):
    src_tree, ids = src_lineage_tree
    derived_tree, ids = compatible_derived_tree
    eo3_ls8_dataset_doc[0]["id"] = ids["root"]
    dataset = doc_to_ds(index, ls8_eo3_product.name, *eo3_ls8_dataset_doc,
                        src_tree=src_tree, derived_tree=derived_tree)
    return dataset, src_tree, derived_tree, ids
