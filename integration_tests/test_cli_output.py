# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0


def test_cli_product_subcommand(index_empty, clirunner, dataset_add_configs):
    runner = clirunner(['product', 'update'], verbose_flag=False, expect_success=False)
    assert "Usage:  [OPTIONS] [FILES]" in runner.output
    assert "Update existing products." in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['product', 'update', dataset_add_configs.empty_file], verbose_flag=False, expect_success=False)
    assert "All files are empty, exit" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['product', 'add'], verbose_flag=False, expect_success=False)
    assert "Usage:  [OPTIONS] [FILES]" in runner.output
    assert "Add or update products in" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['product', 'list'], verbose_flag=False, expect_success=False)
    assert "Usage:  [OPTIONS] [FILES]" not in runner.output
    assert runner.exit_code == 0

    runner = clirunner(['product', 'show', 'ga_ls8c_ard_3'], verbose_flag=False, expect_success=False)
    assert "No products" not in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['product', 'add', dataset_add_configs.empty_file], verbose_flag=False, expect_success=False)
    assert "All files are empty, exit" in runner.output
    assert runner.exit_code == 1


def test_cli_metadata_subcommand(index_empty, clirunner, dataset_add_configs):
    runner = clirunner(['metadata', 'update'], verbose_flag=False, expect_success=False)
    assert "Usage:  [OPTIONS] [FILES]" in runner.output
    assert "Update existing metadata types." in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['metadata', 'update', dataset_add_configs.empty_file], verbose_flag=False, expect_success=False)
    assert "All files are empty, exit" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['metadata', 'add'], verbose_flag=False, expect_success=False)
    assert "Usage:  [OPTIONS] [FILES]" in runner.output
    assert "Add or update metadata types in" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['metadata', 'add', dataset_add_configs.empty_file], verbose_flag=False, expect_success=False)
    assert "All files are empty, exit" in runner.output
    assert runner.exit_code == 1


def test_cli_dataset_subcommand(index, clirunner,
                                extended_eo3_metadata_type,
                                ls8_eo3_product, wo_eo3_product, africa_s2_eo3_product,
                                eo3_dataset_paths):
    # Tests with no datasets in db
    runner = clirunner(['dataset', 'add'], verbose_flag=False, expect_success=False)
    assert "Indexing datasets  [####################################]  100%" not in runner.output
    assert "Usage:  [OPTIONS] [DATASET_PATHS]" in runner.output
    assert "Add datasets" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['dataset', 'update'], verbose_flag=False, expect_success=False)
    assert "0 successful, 0 failed" not in runner.output
    assert "Usage:  [OPTIONS] [DATASET_PATHS]" in runner.output
    assert "Update datasets" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['dataset', 'info'], verbose_flag=False, expect_success=False)
    assert "Usage:  [OPTIONS] [IDS]" in runner.output
    assert "Display dataset information" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['dataset', 'uri-search'], verbose_flag=False, expect_success=False)
    assert "Usage:  [OPTIONS] [PATHS]" in runner.output
    assert "Search by dataset locations" in runner.output
    assert runner.exit_code == 1

    # Insert datasets
    for path in eo3_dataset_paths:
        result = clirunner(['dataset', 'add', "--confirm-ignore-lineage", path])

    runner = clirunner(['dataset', 'archive'], verbose_flag=False, expect_success=False)
    assert "Completed dataset archival." not in runner.output
    assert "Usage:  [OPTIONS] [IDS]" in runner.output
    assert "Archive datasets" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['dataset', 'archive', "--all"], verbose_flag=False)
    assert "Completed dataset archival." in runner.output
    assert "Usage:  [OPTIONS] [IDS]" not in runner.output
    assert "Archive datasets" not in runner.output
    assert runner.exit_code == 0

    runner = clirunner(['dataset', 'restore'], verbose_flag=False, expect_success=False)
    assert "Usage:  [OPTIONS] [IDS]" in runner.output
    assert "Restore datasets" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['dataset', 'restore', "--all"], verbose_flag=False)
    assert "restoring" in runner.output
    assert "Usage:  [OPTIONS] [IDS]" not in runner.output
    assert "Restore datasets" not in runner.output
    assert runner.exit_code == 0

    runner = clirunner(['dataset', 'purge'], verbose_flag=False, expect_success=False)
    assert "Completed dataset purge." not in runner.output
    assert "Usage:  [OPTIONS] [IDS]" in runner.output
    assert "Purge archived datasets" in runner.output
    assert runner.exit_code == 1

    runner = clirunner(['dataset', 'purge', "--all"], verbose_flag=False)
    assert "Completed dataset purge." in runner.output
    assert "Usage:  [OPTIONS] [IDS]" not in runner.output
    assert runner.exit_code == 0


def test_readd_and_update_metadata_product_dataset_command(index, clirunner,
                                                           ext_eo3_mdt_path,
                                                           eo3_product_paths,
                                                           eo3_dataset_paths,
                                                           eo3_dataset_update_path):
    clirunner(['metadata', 'add', ext_eo3_mdt_path])
    rerun_add = clirunner(['metadata', 'add', ext_eo3_mdt_path])
    assert "WARNING Metadata Type" in rerun_add.output
    assert "is already in the database" in rerun_add.output

    update = clirunner(['metadata', 'update', ext_eo3_mdt_path])
    assert "WARNING No changes detected for metadata type" in update.output

    for prod_path in eo3_product_paths:
        add = clirunner(['product', 'add', prod_path])
        rerun_add = clirunner(['product', 'add', prod_path])
        assert "WARNING Product" in rerun_add.output
        assert "is already in the database" in rerun_add.output

        update = clirunner(['product', 'update', prod_path])
        assert "WARNING No changes detected for product" in update.output

    # Update before add
    for ds_path in eo3_dataset_paths:
        update = clirunner(['dataset', 'update', ds_path])
        assert "No such dataset in the database" in update.output
        assert "Failure while processing" in update.output

        clirunner(['dataset', 'add', '--confirm-ignore-lineage', ds_path])
        rerun_add = clirunner(['dataset', 'add', '--confirm-ignore-lineage', ds_path])
        assert "WARNING Dataset" in rerun_add.output
        assert "is already in the database" in rerun_add.output

    update = clirunner(['dataset', 'update',
                        eo3_dataset_update_path])
    assert "Unsafe changes in" in update.output
    assert "0 successful, 1 failed" in update.output

    update = clirunner(['dataset', 'update', '--allow-any', 'properties.datetime',
                        eo3_dataset_update_path])
    assert "1 successful, 0 failed" in update.output
