from pathlib import Path

import pytest

from integration_tests.data_utils import generate_test_scenes
from integration_tests.utils import prepare_test_ingestion_configuration
from integration_tests.test_full_ingestion import check_open_with_api, check_data_with_api, ensure_datasets_are_indexed


PROJECT_ROOT = Path(__file__).parents[1]

INGESTER_CONFIGS = PROJECT_ROOT / 'docs/config_samples/' / 'ingester'


@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_large_ingest_jobs(clirunner, index, tmpdir, ingest_configs):
    """
    Test for the case where ingestor does not need to create a new product,
    but should re-use an existing target product.

    """
    # Make a test ingestor configuration
    config = INGESTER_CONFIGS / ingest_configs['ls5_nbar_albers']
    config_path, config = prepare_test_ingestion_configuration(tmpdir,
                                                               None,
                                                               config,
                                                               mode='fast_ingest')

    # Create and Index 3202 ls5_nbar_scene datasets
    example_ls5_datasets = generate_test_scenes(tmpdir, num=3202)
    valid_uuids = []
    for uuid, ls5_dataset_path in example_ls5_datasets.items():
        valid_uuids.append(uuid)
        clirunner([
            'dataset',
            'add',
            str(ls5_dataset_path)
        ])

    ensure_datasets_are_indexed(index, valid_uuids)

    # Run ingest dry-run but do not index files
    clirunner([
        'ingest',
        '--config-file',
        str(config_path),
        '--queue-size',
        '3201',
        '--dry-run',
    ])

    # Ingest all scenes (Though the queue size is 3201, all tiles (> 3201) shall be ingested)
    clirunner([
        'ingest',
        '--config-file',
        str(config_path),
        '--queue-size',
        '3201',
        '--allow-product-changes',
    ])

    datasets = index.datasets.search_eager(product='ls5_nbar_albers')
    assert len(datasets) > 3201
    check_open_with_api(index, len(valid_uuids))
    check_data_with_api(index, len(valid_uuids))
