from __future__ import absolute_import
from datetime import datetime
from pathlib import Path

import six

from click.testing import CliRunner
import netCDF4
import yaml

import datacube.scripts.run_ingest

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES =  PROJECT_ROOT / 'docs/config_samples/'
LS5_SAMPLES = CONFIG_SAMPLES / 'ga_landsat_5/'
LS5_NBAR_MAPPING = LS5_SAMPLES / 'ls5_nbar_mapping.yaml'
LS5_NBAR_NAME = 'LS5 NBAR'
LS5_NBAR_ALBERS_MAPPING = LS5_SAMPLES / 'ls5_nbar_mapping_albers.yaml'
LS5_NBAR_ALBERS_NAME = 'LS5 NBAR Albers'

TEST_STORAGE_SHRINK_FACTOR = 100
TEST_STORAGE_NUM_MEASUREMENTS = 2
GEOGRAPHIC_VARS = ('latitude', 'longitude')
PROJECTED_VARS = ('x', 'y')

EXPECTED_STORAGE_UNIT_DATA_SHAPE = (1, 40, 40)

JSON_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'


def test_full_ingestion(global_integration_cli_args, index, default_collection, example_ls5_dataset):
    """
    Loads two storage configuration then ingests a sample Landsat 5 scene

    One storage configuration specifies Australian Albers Equal Area Projection,
    the other is simple latitude/longitude.

    The input dataset should be recorded, two sets of netcdf storage units should be created
    and recorded in the database.
    :param db:
    :return:
    """
    assert default_collection  # default_collection has been added to database by fixture

    # Load a mapping config
    index.mappings.add(load_test_mapping(LS5_NBAR_MAPPING))
    index.mappings.add(load_test_mapping(LS5_NBAR_ALBERS_MAPPING))

    # Run Ingest script on a dataset
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            str(example_ls5_dataset),
            '-v', '-v'
        ]
    )
    result = CliRunner().invoke(
        datacube.scripts.run_ingest.cli,
        opts
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0

    # Check dataset is indexed
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1
    assert datasets[0].id == 'bbf3e21c-82b0-11e5-9ba1-a0000100fe80'

    # Check storage units are indexed and written
    sus = index.storage.search_eager()

    latlon = [su for su in sus if su.storage_mapping.name == LS5_NBAR_NAME]
    assert len(latlon) == 12
    albers = [su for su in sus if su.storage_mapping.name == LS5_NBAR_ALBERS_NAME]
    assert len(albers) == 12

    for su in (latlon[0], albers[0]):
        with netCDF4.Dataset(su.filepath) as nco:
            check_data_shape(nco)
            check_cf_compliance(nco)
            check_dataset_metadata_in_su(nco, example_ls5_dataset)
        check_open_with_xray(su.filepath)


def check_data_shape(nco):
    assert nco.variables['band_10'].shape == EXPECTED_STORAGE_UNIT_DATA_SHAPE


def check_cf_compliance(dataset):
    # At the moment the compliance-checker is only compatible with Python 2
    if not six.PY2:
        #TODO Add Warning or fix for Python 3
        return

    from compliance_checker.runner import CheckSuite, ComplianceChecker
    NORMAL_CRITERIA = 2

    cs = CheckSuite()
    cs.load_all_available_checkers()
    score_groups = cs.run(dataset, 'cf')

    groups = ComplianceChecker.stdout_output(cs, score_groups, verbose=1, limit=NORMAL_CRITERIA)
    assert cs.passtree(groups, limit=NORMAL_CRITERIA)


def check_dataset_metadata_in_su(nco, dataset_dir):
    assert len(nco.variables['extra_metadata']) == 1  # 1 time slice
    stored_metadata = str(netCDF4.chartostring(nco.variables['extra_metadata'][0]))
    ds_filename = dataset_dir / 'agdc-metadata.yaml'
    with ds_filename.open() as f:
        orig_metadata = f.read()
    stored = make_pgsqljson_match_yaml_load(yaml.safe_load(stored_metadata))
    original = make_pgsqljson_match_yaml_load(yaml.safe_load(orig_metadata))
    assert stored == original


def check_open_with_xray(filename):
    import xray
    xray.open_dataset(filename)


def test_shrink_mapping():
    mapping = load_mapping_file(LS5_NBAR_MAPPING)
    mapping = alter_mapping_config_for_testing(mapping)
    assert len(mapping['measurements']) <= TEST_STORAGE_NUM_MEASUREMENTS
    for var in GEOGRAPHIC_VARS:
        assert abs(mapping['storage']['resolution'][var]) == 0.025
        assert mapping['storage']['chunking'][var] == 5


def test_load_mapping():
    mapping_config = load_mapping_file(LS5_NBAR_ALBERS_MAPPING)
    assert mapping_config
    assert 'name' in mapping_config
    assert 'storage' in mapping_config
    assert 'match' in mapping_config


def load_test_mapping(filename):
    mapping_config = load_mapping_file(filename)
    return alter_mapping_config_for_testing(mapping_config)


def load_mapping_file(filename):
    with open(str(filename)) as f:
        return yaml.safe_load(f)


def alter_mapping_config_for_testing(mapping):
    mapping = limit_num_measurements(mapping)
    mapping = use_test_storage(mapping)
    if is_geog_mapping(mapping):
        return shrink_mapping(mapping, GEOGRAPHIC_VARS)
    else:
        return shrink_mapping(mapping, PROJECTED_VARS)


def limit_num_measurements(mapping):
    measurements = mapping['measurements']
    if len(measurements) <= TEST_STORAGE_NUM_MEASUREMENTS:
        return mapping
    else:
        measurements_to_delete = sorted(measurements)[TEST_STORAGE_NUM_MEASUREMENTS:]
        for key in measurements_to_delete:
            del measurements[key]
        return mapping


def use_test_storage(mapping):
    mapping['location_name'] = 'testdata'
    return mapping


def is_geog_mapping(mapping):
    return 'latitude' in mapping['storage']['resolution']


def shrink_mapping(mapping, variables):
    storage = mapping['storage']
    for var in variables:
        storage['resolution'][var] = storage['resolution'][var] * TEST_STORAGE_SHRINK_FACTOR
        storage['chunking'][var] = storage['chunking'][var] / TEST_STORAGE_SHRINK_FACTOR
    return mapping


def make_pgsqljson_match_yaml_load(data):
    """Un-munge YAML data passed through PostgreSQL JSON"""
    for key, value in data.items():
        if isinstance(value, dict):
            data[key] = make_pgsqljson_match_yaml_load(value)
        elif isinstance(value, datetime):
            data[key] = value.strftime(JSON_DATE_FORMAT)
        elif value is None:
            data[key] = {}
    return data