from pathlib import Path

import pytest

from integration_tests.data_utils import generate_test_scenes
from integration_tests.utils import prepare_test_ingestion_configuration

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

    # Create and Index some example scene datasets
    dataset_paths = generate_test_scenes(tmpdir, num=3000)
    for path in dataset_paths:
        clirunner(['dataset', 'add', str(path)])

    # Run ingest dry-run but do not index files
    clirunner([
        'ingest',
        '--config-file',
        str(config_path),
        '--year 2010',
        '--queue-size 3201',
        '--dry-run',
        '--allow-product-changes'
    ])

    # Create and Index some more scene datasets and ingest again
    dataset_paths = generate_test_scenes(tmpdir, num=500)
    for path in dataset_paths:
        clirunner(['dataset', 'add', str(path)])

    # Ingest all scenes (Though queue size is 3201, all tiles should be
    # processed)
    clirunner([
        'ingest',
        '--config-file',
        str(config_path),
        '--year 2010',
        '--queue-size 3201',
        '--allow-product-changes'
    ])

    datasets = index.datasets.search_eager(product='ls5_nbar_albers')
    assert len(datasets) > 3201
