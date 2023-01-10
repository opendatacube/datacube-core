import pytest


def test_index_clone(index_pair_populated_empty):
    pop_idx, empty_idx = index_pair_populated_empty
    assert list(empty_idx.products.get_all()) == []
    results = empty_idx.clone(pop_idx)
    assert "eo3" in results["metadata_types"].safe
    assert "ga_ls8c_ard_3" in results["products"].safe
    assert results["products"].skipped == 0
    assert results["datasets"].skipped == 0


def test_index_clone_cli(local_config_pair, index_pair_populated_empty, clirunner_raw):
    source_cfg, target_cfg = local_config_pair
    clirunner_raw([
        '-E', target_cfg._env,
        'system', 'clone',
        source_cfg._env
    ], expect_success=False)
