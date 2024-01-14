# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0


def test_index_clone(index_pair_populated_empty):
    pop_idx, empty_idx = index_pair_populated_empty
    assert list(empty_idx.products.get_all()) == []
    results = empty_idx.clone(pop_idx)
    assert "eo3" in results["metadata_types"].safe
    assert "ga_ls8c_ard_3" in results["products"].safe
    assert results["products"].skipped == 0
    assert results["datasets"].skipped == 0


def test_index_clone_small_batch(index_pair_populated_empty):
    pop_idx, empty_idx = index_pair_populated_empty
    assert list(empty_idx.products.get_all()) == []
    results = empty_idx.clone(pop_idx, batch_size=2)
    assert "eo3" in results["metadata_types"].safe
    assert "ga_ls8c_ard_3" in results["products"].safe
    assert results["products"].skipped == 0
    assert results["datasets"].skipped == 0


def test_index_clone_cli(cfg_env_pair, index_pair_populated_empty, clirunner):
    source_cfg, target_cfg = cfg_env_pair
    clirunner([
        '-E', target_cfg._name,
        'system', 'clone',
        '--lineage-only', '--skip-lineage',
        source_cfg._name
    ], skip_env=True, expect_success=False)
    clirunner([
        '-E', target_cfg._name,
        'system', 'clone',
        source_cfg._name
    ], skip_env=True, expect_success=True)


def test_index_clone_cli_small_batch(cfg_env_pair, index_pair_populated_empty, clirunner):
    source_cfg, target_cfg = cfg_env_pair
    clirunner([
        '-E', target_cfg._name,
        'system', 'clone',
        '--batch-size', '2',
        source_cfg._name
    ], skip_env=True, expect_success=True)
