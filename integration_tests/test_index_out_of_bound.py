# coding=utf-8
import pytest

import datacube
from integration_tests.test_end_to_end import INGESTER_CONFIGS
from integration_tests.test_full_ingestion import (check_open_with_api,
                                                   ensure_datasets_are_indexed, check_data_shape,
                                                   check_grid_mapping, check_cf_compliance, check_attributes,
                                                   check_dataset_metadata_in_storage_unit,
                                                   check_open_with_xarray)
from integration_tests.utils import prepare_test_ingestion_configuration
import netCDF4


@pytest.mark.timeout(20)
@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_index_out_of_bound_error(clirunner, index, tmpdir, example_ls5_dataset_paths, ingest_configs):
    """
    Test for the case where ingestor processes upto `--queue-size` number of tasks and not all the available scenes
    """
    # Make a test ingestor configuration
    config = INGESTER_CONFIGS / ingest_configs['ls5_nbar_albers']
    config_path, config = prepare_test_ingestion_configuration(tmpdir, None,
                                                               config, mode='fast_ingest')

    def index_dataset(path):
        return clirunner(['dataset', 'add', str(path)])

    # Set the queue size to process 5 tiles
    queue_size = 5
    valid_uuids = []
    for uuid, ls5_dataset_path in example_ls5_dataset_paths.items():
        valid_uuids.append(uuid)
        index_dataset(ls5_dataset_path)

    # Ensure that datasets are actually indexed
    ensure_datasets_are_indexed(index, valid_uuids)

    # Locationless scenario within database arises when we run the sync tool (with --update-location option)
    # on the disk where the actual file is removed and regenerated again with new dataset id.
    for indexed_uuid in valid_uuids:
        dc1 = datacube.Datacube(index=index)
        datasets = dc1.find_datasets(product='ls5_nbar_scene')
        try:
            # Remove location from the index, to simulate indexed out of range scenario
            res = dc1.index.datasets.remove_location(indexed_uuid, datasets[0].local_uri)
        except AttributeError:
            # Do for one dataset, ignore any other attribute errors
            pass
        assert res is True, "Error for %r. output: %r" % (indexed_uuid, res)

    # Ingest scenes with locationless dataset
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
