from __future__ import absolute_import

import warnings
from pathlib import Path

import netCDF4
import numpy as np
import pytest

import yaml
from click.testing import CliRunner
from affine import Affine
from datacube.api.query import query_group_by

import datacube.scripts.cli_app
from datacube.model import GeoBox, CRS
from datacube.utils import read_documents
from .conftest import EXAMPLE_LS5_DATASET_ID

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs/config_samples/'
LS5_SAMPLES = CONFIG_SAMPLES / 'ga_landsat_5/'
LS5_MATCH_RULES = CONFIG_SAMPLES / 'match_rules' / 'ls5_scenes.yaml'
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


@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_dataset_type')
def test_full_ingestion(global_integration_cli_args, index, example_ls5_dataset, ls5_nbar_ingest_config):
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-v',
            'dataset',
            'add',
            '--auto-match',
            str(example_ls5_dataset)
        ]
    )
    result = CliRunner().invoke(
        datacube.scripts.cli_app.cli,
        opts,
        catch_exceptions=False
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0

    ensure_dataset_is_indexed(index)

    config_path, config = ls5_nbar_ingest_config

    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-v',
            'ingest',
            '--config-file',
            str(config_path)
        ]
    )
    result = CliRunner().invoke(
        datacube.scripts.cli_app.cli,
        opts,
        catch_exceptions=False
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0

    datasets = index.datasets.search_eager(product='ls5_nbar_albers')
    assert len(datasets) > 0
    assert datasets[0].managed

    ds_path = str(datasets[0].local_path)
    with netCDF4.Dataset(ds_path) as nco:
        check_data_shape(nco)
        check_grid_mapping(nco)
        check_cf_compliance(nco)
        check_dataset_metadata_in_storage_unit(nco, example_ls5_dataset)
        check_attributes(nco, config['global_attributes'])

        name = config['measurements'][0]['name']
        check_attributes(nco[name], config['measurements'][0]['attrs'])
    check_open_with_xarray(ds_path)
    check_open_with_api(index)


def ensure_dataset_is_indexed(index):
    datasets = index.datasets.search_eager(product='ls5_nbar_scene')
    assert len(datasets) == 1
    assert datasets[0].id == EXAMPLE_LS5_DATASET_ID


def check_grid_mapping(nco):
    assert 'grid_mapping' in nco.variables['blue'].ncattrs()
    grid_mapping = nco.variables['blue'].grid_mapping
    assert grid_mapping in nco.variables
    assert 'GeoTransform' in nco.variables[grid_mapping].ncattrs()
    assert 'spatial_ref' in nco.variables[grid_mapping].ncattrs()


def check_data_shape(nco):
    assert nco.variables['blue'].shape == EXPECTED_STORAGE_UNIT_DATA_SHAPE


def check_cf_compliance(dataset):
    try:
        from compliance_checker.runner import CheckSuite, ComplianceChecker
        import compliance_checker
    except ImportError:
        warnings.warn('compliance_checker unavailable, skipping NetCDF-CF Compliance Checks')
        return

    cs = CheckSuite()
    cs.load_all_available_checkers()
    if compliance_checker.__version__ >= '2.3.0':
        # This skips a failing compliance check. Our files don't contain all the lats/lons
        # as an auxiliary cordinate var as it's unnecessary for any software we've tried.
        # It may be added at some point in the future, and this check should be re-enabled.
        score_groups = cs.run(dataset, ['check_dimension_order'], 'cf')
    else:
        warnings.warn('Please upgrade to compliance-checker 2.3.0 or higher.')
        score_groups = cs.run(dataset, 'cf')

    groups = ComplianceChecker.stdout_output(cs, score_groups, verbose=1, limit=COMPLIANCE_CHECKER_NORMAL_LIMIT)
    assert cs.passtree(groups, limit=COMPLIANCE_CHECKER_NORMAL_LIMIT)


def check_attributes(obj, attrs):
    for k, v in attrs.items():
        assert k in obj.ncattrs()
        assert obj.getncattr(k) == v


def check_dataset_metadata_in_storage_unit(nco, dataset_dir):
    assert len(nco.variables['dataset']) == 1  # 1 time slice
    stored_metadata = nco.variables['dataset'][0]
    if not isinstance(stored_metadata, str):
        stored_metadata = netCDF4.chartostring(stored_metadata)
        stored_metadata = str(np.char.decode(stored_metadata))
    ds_filename = dataset_dir / 'agdc-metadata.yaml'

    stored = yaml.safe_load(stored_metadata)
    [(_, original)] = read_documents(ds_filename)
    assert len(stored['lineage']['source_datasets']) == 1
    assert next(iter(stored['lineage']['source_datasets'].values())) == original


def check_open_with_xarray(file_path):
    import xarray
    xarray.open_dataset(str(file_path))


def check_open_with_api(index):
    from datacube import Datacube
    dc = Datacube(index=index)

    input_type_name = 'ls5_nbar_albers'
    input_type = dc.index.products.get_by_name(input_type_name)

    geobox = GeoBox(200, 200, Affine(25, 0.0, 1500000, 0.0, -25, -3900000), CRS('EPSG:3577'))
    observations = dc.find_datasets(product='ls5_nbar_albers', geopolygon=geobox.extent)
    group_by = query_group_by('time')
    sources = dc.group_datasets(observations, group_by)
    data = dc.load_data(sources, geobox, input_type.measurements.values())
    assert data.blue.shape == (1, 200, 200)
