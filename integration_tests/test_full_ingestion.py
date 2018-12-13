from __future__ import absolute_import

import hashlib
import warnings
from pathlib import Path
from uuid import UUID

import netCDF4
import pytest
import yaml
import rasterio
from affine import Affine

from datacube.api.query import query_group_by
from datacube.utils import geometry, read_documents, netcdf_extract_string
from integration_tests.utils import prepare_test_ingestion_configuration, GEOTIFF

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs/config_samples/'
LS5_SAMPLES = CONFIG_SAMPLES / 'ga_landsat_5/'
LS5_MATCH_RULES = CONFIG_SAMPLES / 'match_rules' / 'ls5_scenes.yaml'
LS5_NBAR_STORAGE_TYPE = LS5_SAMPLES / 'ls5_geographic.yaml'
LS5_NBAR_ALBERS_STORAGE_TYPE = LS5_SAMPLES / 'ls5_albers.yaml'

INGESTER_CONFIGS = CONFIG_SAMPLES / 'ingester'

TEST_STORAGE_SHRINK_FACTOR = 100
TEST_STORAGE_NUM_MEASUREMENTS = 2
GEOGRAPHIC_VARS = ('latitude', 'longitude')
PROJECTED_VARS = ('x', 'y')

EXPECTED_STORAGE_UNIT_DATA_SHAPE = (1, 40, 40)
EXPECTED_NUMBER_OF_STORAGE_UNITS = 12

JSON_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'

COMPLIANCE_CHECKER_NORMAL_LIMIT = 2


@pytest.mark.timeout(20)
@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_full_ingestion(clirunner, index, tmpdir, example_ls5_dataset_paths, ingest_configs):
    config = INGESTER_CONFIGS/ingest_configs['ls5_nbar_albers']
    config_path, config = prepare_test_ingestion_configuration(tmpdir, None, config, mode='fast_ingest')
    valid_uuids = []
    for uuid, example_ls5_dataset_path in example_ls5_dataset_paths.items():
        valid_uuids.append(uuid)
        clirunner([
            'dataset',
            'add',
            str(example_ls5_dataset_path)
        ])

    ensure_datasets_are_indexed(index, valid_uuids)

    # TODO(csiro) Set time dimension when testing
    # config['storage']['tile_size']['time'] = 2

    clirunner([
        'ingest',
        '--config-file',
        str(config_path)
    ])

    datasets = index.datasets.search_eager(product='ls5_nbar_albers')
    assert len(datasets) > 0
    assert datasets[0].managed

    check_open_with_api(index, len(valid_uuids))
    check_data_with_api(index, len(valid_uuids))

    # NetCDF specific checks, based on the saved NetCDF file
    ds_path = str(datasets[0].local_path)
    with netCDF4.Dataset(ds_path) as nco:
        check_data_shape(nco)
        check_grid_mapping(nco)
        check_cf_compliance(nco)
        check_dataset_metadata_in_storage_unit(nco, example_ls5_dataset_paths)
        check_attributes(nco, config['global_attributes'])

        name = config['measurements'][0]['name']
        check_attributes(nco[name], config['measurements'][0]['attrs'])
    check_open_with_xarray(ds_path)


@pytest.mark.timeout(20)
@pytest.mark.parametrize('datacube_env_name', ('s3aio_env',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_s3_full_ingestion(clirunner, index, tmpdir, example_ls5_dataset_paths, ingest_configs):
    config = INGESTER_CONFIGS/ingest_configs['ls5_nbar_albers']

    config_path, config = prepare_test_ingestion_configuration(tmpdir, None, config, mode='fast_ingest')
    valid_uuids = []
    for uuid, example_ls5_dataset_path in example_ls5_dataset_paths.items():
        valid_uuids.append(uuid)
        clirunner([
            'dataset',
            'add',
            str(example_ls5_dataset_path)
        ])

    ensure_datasets_are_indexed(index, valid_uuids)

    # TODO(csiro) Set time dimension when testing
    # config['storage']['tile_size']['time'] = 2

    result = clirunner([
        'ingest',
        '--config-file',
        str(config_path)
    ])

    print(result.output)

    datasets = index.datasets.search_eager(product='ls5_nbar_albers')
    assert len(datasets) > 0
    assert datasets[0].managed

    check_open_with_api(index, len(valid_uuids))
    check_data_with_api(index, len(valid_uuids))


@pytest.mark.timeout(20)
@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_process_all_ingest_jobs(clirunner, index, tmpdir, example_ls5_dataset_paths, ingest_configs):
    """
    Test for the case where ingestor processes upto `--queue-size` number of tasks and not all the available scenes
    """
    # Make a test ingestor configuration
    config = INGESTER_CONFIGS / ingest_configs['ls5_nbar_albers']
    config_path, config = prepare_test_ingestion_configuration(tmpdir, None,
                                                               config, mode='fast_ingest')

    def index_dataset(path):
        return clirunner(['dataset', 'add', str(path)])

    # Number of scenes generated is 3 (as per NUM_TIME_SLICES const from conftest.py)
    # Set the queue size to process 2 tiles
    queue_size = 2
    valid_uuids = []
    for uuid, ls5_dataset_path in example_ls5_dataset_paths.items():
        valid_uuids.append(uuid)
        index_dataset(ls5_dataset_path)

    # Ensure that datasets are actually indexed
    ensure_datasets_are_indexed(index, valid_uuids)

    # Ingest all scenes (Though the queue size is 2, all 3 tiles will be ingested)
    clirunner([
        'ingest',
        '--config-file',
        str(config_path),
        '--queue-size',
        queue_size,
        '--allow-product-changes',
    ])

    # Validate that the ingestion is working as expected
    datasets = index.datasets.search_eager(product='ls5_nbar_albers')
    assert len(datasets) > 0
    assert datasets[0].managed

    check_open_with_api(index, len(valid_uuids))

    # NetCDF specific checks, based on the saved NetCDF file
    ds_path = str(datasets[0].local_path)
    with netCDF4.Dataset(ds_path) as nco:
        check_data_shape(nco)
        check_grid_mapping(nco)
        check_cf_compliance(nco)
        check_dataset_metadata_in_storage_unit(nco, example_ls5_dataset_paths)
        check_attributes(nco, config['global_attributes'])

        name = config['measurements'][0]['name']
        check_attributes(nco[name], config['measurements'][0]['attrs'])
    check_open_with_xarray(ds_path)


def ensure_datasets_are_indexed(index, valid_uuids):
    datasets = index.datasets.search_eager(product='ls5_nbar_scene')
    assert len(datasets) == len(valid_uuids)
    for dataset in datasets:
        assert dataset.id in valid_uuids


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


def check_dataset_metadata_in_storage_unit(nco, dataset_dirs):
    """Check one of the NetCDF files metadata against the original
    metadata."""
    assert len(nco.variables['dataset']) == 1  # 1 time slice
    stored_metadata = netcdf_extract_string(nco.variables['dataset'][0])
    stored = yaml.safe_load(stored_metadata)

    assert 'lineage' in stored
    assert 'source_datasets' in stored['lineage']
    assert '0' in stored['lineage']['source_datasets']
    assert 'id' in stored['lineage']['source_datasets']['0']
    source_uuid = UUID(stored['lineage']['source_datasets']['0']['id'])
    assert source_uuid in dataset_dirs
    ds_filename = dataset_dirs[source_uuid] / 'agdc-metadata.yaml'
    [(_, original)] = read_documents(ds_filename)
    assert len(stored['lineage']['source_datasets']) == 1
    assert next(iter(stored['lineage']['source_datasets'].values())) == original


def check_open_with_xarray(file_path):
    import xarray
    xarray.open_dataset(str(file_path))


def check_open_with_api(index, time_slices):
    with rasterio.Env():
        from datacube import Datacube
        dc = Datacube(index=index)

        input_type_name = 'ls5_nbar_albers'
        input_type = dc.index.products.get_by_name(input_type_name)
        geobox = geometry.GeoBox(200, 200, Affine(25, 0.0, 638000, 0.0, -25, 6276000), geometry.CRS('EPSG:28355'))
        observations = dc.find_datasets(product='ls5_nbar_albers', geopolygon=geobox.extent)
        group_by = query_group_by('time')
        sources = dc.group_datasets(observations, group_by)
        data = dc.load_data(sources, geobox, input_type.measurements.values())
        assert data.blue.shape == (time_slices, 200, 200)

        chunk_profile = {'time': 1, 'x': 100, 'y': 100}
        lazy_data = dc.load_data(sources, geobox, input_type.measurements.values(), dask_chunks=chunk_profile)
        assert lazy_data.blue.shape == (time_slices, 200, 200)
        assert (lazy_data.blue.load() == data.blue).all()


def check_data_with_api(index, time_slices):
    """Chek retrieved data for specific values.

    We scale down by 100 and check for predefined values in the
    corners.
    """
    from datacube import Datacube
    dc = Datacube(index=index)

    # Make the retrieved data 100 less granular
    shape_x = int(GEOTIFF['shape']['x'] / 100.0)
    shape_y = int(GEOTIFF['shape']['y'] / 100.0)
    pixel_x = int(GEOTIFF['pixel_size']['x'] * 100)
    pixel_y = int(GEOTIFF['pixel_size']['y'] * 100)

    input_type_name = 'ls5_nbar_albers'
    input_type = dc.index.products.get_by_name(input_type_name)
    geobox = geometry.GeoBox(shape_x + 1, shape_y + 1,
                             Affine(pixel_x, 0.0, GEOTIFF['ul']['x'], 0.0, pixel_y, GEOTIFF['ul']['y']),
                             geometry.CRS(GEOTIFF['crs']))
    observations = dc.find_datasets(product='ls5_nbar_albers', geopolygon=geobox.extent)
    group_by = query_group_by('time')
    sources = dc.group_datasets(observations, group_by)
    data = dc.load_data(sources, geobox, input_type.measurements.values())
    assert hashlib.md5(data.green.data).hexdigest() == '147180327d0d9a3b0a52099fa0276eb2'
    assert hashlib.md5(data.blue.data).hexdigest() == '179c4f1be3ebfa45f1573727c335de7c'
    for time_slice in range(time_slices):
        assert data.blue.values[time_slice][-1, -1] == -999
