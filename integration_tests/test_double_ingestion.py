from pathlib import Path

import pytest
from hypothesis import given
from hypothesis.strategies import lists

from integration_tests.data_utils import scene_datasets, write_test_scene_to_disk
from integration_tests.utils import prepare_test_ingestion_configuration

PROJECT_ROOT = Path(__file__).parents[1]

INGESTER_CONFIGS = PROJECT_ROOT / 'docs/config_samples/' / 'ingester'


@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
@given(scenes=lists(scene_datasets(), min_size=2, max_size=2))
def test_double_ingestion(clirunner, index, tmpdir, ingest_configs, scenes):
    """
    Test for the case where ingestor does not need to create a new product,
    but should re-use an existing target product.

    """
    # Make a test ingestor configuration
    config = INGESTER_CONFIGS / ingest_configs['ls5_nbar_albers']
    config_path, config = prepare_test_ingestion_configuration(
        tmpdir, None, config, mode='fast_ingest')

    for ls5_dataset in scenes:
        # Write and ingest each dataset in turn
        dataset_file = write_test_scene_to_disk(ls5_dataset, tmpdir)
        clirunner(['dataset', 'add', str(dataset_file)])
        clirunner(['ingest', '--config-file', str(config_path)])
