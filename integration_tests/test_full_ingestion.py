from __future__ import absolute_import

import warnings
from datetime import datetime
from pathlib import Path

import netCDF4
import numpy as np
import six
import yaml
from click.testing import CliRunner

import datacube.scripts.run_ingest

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs/config_samples/'
LS5_SAMPLES = CONFIG_SAMPLES / 'ga_landsat_5/'
LS5_NBAR_STORAGE_TYPE = LS5_SAMPLES / 'ls5_nbar_mapping.yaml'
LS5_NBAR_NAME = 'ls5_nbar'
LS5_NBAR_ALBERS_STORAGE_TYPE = LS5_SAMPLES / 'ls5_nbar_mapping_albers.yaml'
LS5_NBAR_ALBERS_NAME = 'ls5_nbar_albers'

TEST_STORAGE_SHRINK_FACTOR = 100
TEST_STORAGE_NUM_MEASUREMENTS = 2
GEOGRAPHIC_VARS = ('latitude', 'longitude')
PROJECTED_VARS = ('x', 'y')

EXAMPLE_LS5_DATASET_ID = 'bbf3e21c-82b0-11e5-9ba1-a0000100fe80'

EXPECTED_STORAGE_UNIT_DATA_SHAPE = (1, 40, 40)
EXPECTED_NUMBER_OF_STORAGE_UNITS = 12

JSON_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'

COMPLIANCE_CHECKER_NORMAL_LIMIT = 2


def test_full_ingestion(global_integration_cli_args, index, default_collection, example_ls5_dataset):
    """
    Loads two storage mapping configurations, then ingests a sample Landsat 5 scene

    One storage configuration specifies Australian Albers Equal Area Projection,
    the other is simply latitude/longitude.

    The input dataset should be recorded in the index, and two sets of netcdf storage units
    should be created on disk and recorded in the index.
    :param db:
    :return:
    """
    assert default_collection  # default_collection has been added to database by fixture

    # Load a mapping config
    index.storage.types.add(load_test_storage_config(LS5_NBAR_STORAGE_TYPE))
    index.storage.types.add(load_test_storage_config(LS5_NBAR_ALBERS_STORAGE_TYPE))

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
        opts,
        catch_exceptions=False
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0

    ensure_dataset_is_indexed(index)

    # Check storage units are indexed and written
    sus = index.storage.search_eager()
    latlon_storageunits = [su for su in sus if su.storage_type.name == LS5_NBAR_NAME]
    assert len(latlon_storageunits) == EXPECTED_NUMBER_OF_STORAGE_UNITS

    albers_storageunits = [su for su in sus if su.storage_type.name == LS5_NBAR_ALBERS_NAME]
    assert len(albers_storageunits) == EXPECTED_NUMBER_OF_STORAGE_UNITS

    for su in (latlon_storageunits[0], albers_storageunits[0]):
        with netCDF4.Dataset(str(su.local_path)) as nco:
            check_data_shape(nco)
            check_cf_compliance(nco)
            check_dataset_metadata_in_storage_unit(nco, example_ls5_dataset)
        check_open_with_xray(su.local_path)


def ensure_dataset_is_indexed(index):
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1
    assert datasets[0].id == EXAMPLE_LS5_DATASET_ID


def check_data_shape(nco):
    assert nco.variables['band_10'].shape == EXPECTED_STORAGE_UNIT_DATA_SHAPE


def check_cf_compliance(dataset):
    if not six.PY2:
        warnings.warn('compliance_checker non-functional in Python 3. Skipping NetCDF-CF Compliance Checks')
        return

    try:
        from compliance_checker.runner import CheckSuite, ComplianceChecker
    except ImportError:
        warnings.warn('compliance_checker unavailable, skipping NetCDF-CF Compliance Checks')
        return

    cs = CheckSuite()
    cs.load_all_available_checkers()
    score_groups = cs.run(dataset, 'cf')

    groups = ComplianceChecker.stdout_output(cs, score_groups, verbose=1, limit=COMPLIANCE_CHECKER_NORMAL_LIMIT)
    assert cs.passtree(groups, limit=COMPLIANCE_CHECKER_NORMAL_LIMIT)


def check_dataset_metadata_in_storage_unit(nco, dataset_dir):
    assert len(nco.variables['extra_metadata']) == 1  # 1 time slice
    stored_metadata = netCDF4.chartostring(nco.variables['extra_metadata'][0])
    stored_metadata = str(np.char.decode(stored_metadata))
    ds_filename = dataset_dir / 'agdc-metadata.yaml'
    with ds_filename.open() as f:
        orig_metadata = f.read()
    stored = make_pgsqljson_match_yaml_load(yaml.safe_load(stored_metadata))
    original = make_pgsqljson_match_yaml_load(yaml.safe_load(orig_metadata))
    assert stored == original


def check_open_with_xray(file_path):
    import xray
    xray.open_dataset(str(file_path))


def test_shrink_storage_type():
    storage_type = load_storage_type_file(LS5_NBAR_STORAGE_TYPE)
    storage_type = alter_storage_type_for_testing(storage_type)
    assert len(storage_type['measurements']) <= TEST_STORAGE_NUM_MEASUREMENTS
    for var in GEOGRAPHIC_VARS:
        assert abs(storage_type['storage']['resolution'][var]) == 0.025
        assert storage_type['storage']['chunking'][var] == 5


def test_load_storage_type():
    storage_type = load_storage_type_file(LS5_NBAR_ALBERS_STORAGE_TYPE)
    assert storage_type
    assert 'name' in storage_type
    assert 'storage' in storage_type
    assert 'match' in storage_type


def load_test_storage_config(filename):
    storage_type = load_storage_type_file(filename)
    return alter_storage_type_for_testing(storage_type)


def load_storage_type_file(filename):
    with open(str(filename)) as f:
        return yaml.safe_load(f)


def alter_storage_type_for_testing(storage_type):
    storage_type = limit_num_measurements(storage_type)
    storage_type = use_test_storage(storage_type)
    if is_geogaphic_storage_type(storage_type):
        return shrink_storage_type(storage_type, GEOGRAPHIC_VARS)
    else:
        return shrink_storage_type(storage_type, PROJECTED_VARS)


def limit_num_measurements(storage_type):
    measurements = storage_type['measurements']
    if len(measurements) <= TEST_STORAGE_NUM_MEASUREMENTS:
        return storage_type
    else:
        measurements_to_delete = sorted(measurements)[TEST_STORAGE_NUM_MEASUREMENTS:]
        for key in measurements_to_delete:
            del measurements[key]
        return storage_type


def use_test_storage(storage_type):
    storage_type['location_name'] = 'testdata'
    return storage_type


def is_geogaphic_storage_type(storage_type):
    return 'latitude' in storage_type['storage']['resolution']


def shrink_storage_type(storage_type, variables):
    storage = storage_type['storage']
    for var in variables:
        storage['resolution'][var] = storage['resolution'][var] * TEST_STORAGE_SHRINK_FACTOR
        storage['chunking'][var] = storage['chunking'][var] / TEST_STORAGE_SHRINK_FACTOR
    return storage_type


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
