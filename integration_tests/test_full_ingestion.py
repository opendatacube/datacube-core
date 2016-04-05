from __future__ import absolute_import

import warnings
from datetime import datetime
from pathlib import Path

import six
import netCDF4
import numpy as np
import pytest

import yaml
from click.testing import CliRunner

import datacube.scripts.run_ingest
from .conftest import LS5_NBAR_NAME, LS5_NBAR_ALBERS_NAME, EXAMPLE_LS5_DATASET_ID

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs/config_samples/'
LS5_SAMPLES = CONFIG_SAMPLES / 'ga_landsat_5/'
LS5_NBAR_STORAGE_TYPE = LS5_SAMPLES / 'ls5_geographic.yaml'
LS5_NBAR_ALBERS_STORAGE_TYPE = LS5_SAMPLES / 'ls5_albers.yaml'

TEST_STORAGE_SHRINK_FACTOR = 100
TEST_STORAGE_NUM_MEASUREMENTS = 2
GEOGRAPHIC_VARS = ('latitude', 'longitude')
PROJECTED_VARS = ('x', 'y')

EXPECTED_STORAGE_UNIT_DATA_SHAPE = (1, 40, 40)
EXPECTED_NUMBER_OF_STORAGE_UNITS = 12

JSON_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'

COMPLIANCE_CHECKER_NORMAL_LIMIT = 2


@pytest.mark.usefixtures('default_collection',
                         'indexed_ls5_nbar_storage_type',
                         'indexed_ls5_nbar_albers_storage_type')
def test_full_ingestion(global_integration_cli_args, index, example_ls5_dataset):
    """
    Loads two storage mapping configurations, then ingests a sample Landsat 5 scene

    One storage configuration specifies Australian Albers Equal Area Projection,
    the other is simply latitude/longitude.

    The input dataset should be recorded in the index, and two sets of netcdf storage units
    should be created on disk and recorded in the index.
    """

    # Run Ingest script on a dataset
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-vv',
            'ingest',
            str(example_ls5_dataset)
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
        assert su.size_bytes > 0
        with netCDF4.Dataset(str(su.local_path)) as nco:
            check_data_shape(nco)
            check_grid_mapping(nco)
            check_cf_compliance(nco)
            check_dataset_metadata_in_storage_unit(nco, example_ls5_dataset)
            check_global_attributes(nco, su.storage_type.global_attributes)
        check_open_with_xray(su.local_path)
    check_open_with_api(index)


def ensure_dataset_is_indexed(index):
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1
    assert datasets[0].id == EXAMPLE_LS5_DATASET_ID


def check_grid_mapping(nco):
    assert 'grid_mapping' in nco.variables['band_1'].ncattrs()
    grid_mapping = nco.variables['band_1'].grid_mapping
    assert grid_mapping in nco.variables
    assert 'GeoTransform' in nco.variables[grid_mapping].ncattrs()
    assert 'spatial_ref' in nco.variables[grid_mapping].ncattrs()


def check_data_shape(nco):
    assert nco.variables['band_1'].shape == EXPECTED_STORAGE_UNIT_DATA_SHAPE


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


def check_global_attributes(nco, attrs):
    for k, v in attrs.items():
        assert nco.getncattr(k) == v


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
    import xarray
    xarray.open_dataset(str(file_path))


def check_open_with_api(index):
    import datacube.api
    api = datacube.api.API(index)
    fields = api.list_fields()
    assert 'product' in fields
    descriptor = api.get_descriptor()
    assert 'ls5_nbar' in descriptor
    storage_units = descriptor['ls5_nbar']['storage_units']
    query = {
        'variables': ['band_1'],
        'dimensions': {
            'latitude': {'range': (-34, -35)},
            'longitude': {'range': (149, 150)}}
    }
    data = api.get_data(query, storage_units=storage_units)
    assert data['arrays']['band_1'].size
    data_array = api.get_data_array(storage_type='ls5_nbar', variables=['band_1'],
                                    latitude=(-34, -35), longitude=(149, 150))
    assert data_array.size
    dataset = api.get_dataset(storage_type='ls5_nbar', variables=['band_1'],
                              latitude=(-34, -35), longitude=(149, 150))
    assert dataset['band_1'].size


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
