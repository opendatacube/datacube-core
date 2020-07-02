import pytest

from integration_tests.utils import prepare_test_ingestion_configuration
from integration_tests.test_end_to_end import PROJECT_ROOT

COMPLIANCE_CHECKER_NORMAL_LIMIT = 2


@pytest.mark.timeout(20)
@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_invalid_ingestor_config(clirunner, index, tmpdir):
    """
    Test that the ingestor correctly identifies an invalid ingestor config file.

    Note: We do not need to test valid config files as that is covered by the existing
          ingestor tests.
    """
    base = PROJECT_ROOT / 'integration_tests/data/ingester/'

    for cfg, err in (('invalid_config.yaml', "'src_varname' is a required property"),
                     ('invalid_src_name.yaml', 'No such variable in the source product:')):
        config = base / cfg
        config_path, config = prepare_test_ingestion_configuration(tmpdir, None, config)

        result = clirunner(['ingest', '--config-file', str(config_path)],
                           expect_success=False)

        assert result.exit_code != 0
        assert err in result.output
