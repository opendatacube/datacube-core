import pytest
import netCDF4

from integration_tests.utils import prepare_test_ingestion_configuration
from integration_tests.test_full_ingestion import (check_open_with_api, check_data_with_api,
                                                   ensure_datasets_are_indexed, check_data_shape,
                                                   check_grid_mapping, check_cf_compliance, check_attributes,
                                                   check_dataset_metadata_in_storage_unit,
                                                   check_open_with_xarray)

from integration_tests.test_end_to_end import INGESTER_CONFIGS


@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_double_ingestion(clirunner, index, tmpdir, ingest_configs, example_ls5_dataset_paths):
    """
    Test for the case where ingestor does not need to create a new product,
    but should re-use an existing target product.

    """
    # Make a test ingestor configuration
    config = INGESTER_CONFIGS / ingest_configs['ls5_nbar_albers']
    config_path, config = prepare_test_ingestion_configuration(tmpdir, None,
                                                               config, mode='fast_ingest')

    def index_dataset(path):
        return clirunner(['dataset', 'add', str(path)])

    def ingest_products():
        valid_uuids = []
        for uuid, ls5_dataset_path in example_ls5_dataset_paths.items():
            valid_uuids.append(uuid)
            index_dataset(ls5_dataset_path)

        # Ensure that datasets are actually indexed
        ensure_datasets_are_indexed(index, valid_uuids)

        # Ingest them
        clirunner([
            'ingest',
            '--config-file',
            str(config_path)
        ])

        # Validate that the ingestion is working as expected
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

    # Create and Index some example scene datasets
    ingest_products()

    ######################
    #  Double Ingestion  #
    ######################
    # Create and Index some more scene datasets
    ingest_products()
