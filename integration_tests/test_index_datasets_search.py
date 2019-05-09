import pytest
from uuid import UUID

from integration_tests.utils import prepare_test_ingestion_configuration
from integration_tests.test_end_to_end import INGESTER_CONFIGS
from integration_tests.test_full_ingestion import ensure_datasets_are_indexed


@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_index_datasets_search_light(index, ingest_configs, tmpdir, clirunner,
                                     example_ls5_dataset_paths):
    # Make a test ingestor configuration
    config = INGESTER_CONFIGS / ingest_configs['ls5_nbar_albers']
    config_path, config = prepare_test_ingestion_configuration(tmpdir, None,
                                                               config, mode='fast_ingest')

    def index_dataset(path):
        return clirunner(['dataset', 'add', str(path)])

    def index_products():
        valid_uuids = []
        for uuid, ls5_dataset_path in example_ls5_dataset_paths.items():
            valid_uuids.append(uuid)
            index_dataset(ls5_dataset_path)

        # Ensure that datasets are actually indexed
        ensure_datasets_are_indexed(index, valid_uuids)

        # Validate that the ingestion is working as expected
        datasets = index.datasets.search_returning_datasets_light(field_names=('id',), product='ls5_nbar_scene')
        assert len(list(datasets)) > 0
        return valid_uuids

    valid_uuids = index_products()

    results = list(index.datasets.search_returning_datasets_light(field_names=('id', 'extent', 'time'),
                                                                  product='ls5_nbar_scene'))
    for dataset in results:
        assert dataset.id in valid_uuids
        # Assume projection is defined as
        #         datum: GDA94
        #         ellipsoid: GRS80
        #         zone: -55
        # for all datasets. This should give us epsg 28355
        assert dataset.extent.crs.epsg == 28355

    results = list(index.datasets.search_returning_datasets_light(field_names=('id', 'zone'),
                                                                  custom_offsets={'zone': ['grid_spatial',
                                                                                           'projection', 'zone']},
                                                                  product='ls5_nbar_scene'))
    for dataset in results:
        assert dataset.zone == -55

    results = list(index.datasets.search_returning_datasets_light(field_names=('id', 'zone'),
                                                                  custom_offsets={'zone': ['grid_spatial',
                                                                                           'projection', 'zone']},
                                                                  product='ls5_nbar_scene',
                                                                  zone='-55'))
    assert len(results) > 0

    results = list(index.datasets.search_returning_datasets_light(field_names=('id', 'zone'),
                                                                  custom_offsets={'zone': ['grid_spatial',
                                                                                           'projection', 'zone']},
                                                                  product='ls5_nbar_scene',
                                                                  zone='-65'))
    assert len(results) == 0
