import pytest


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


def test_cli_dataset_subcommand(index_empty, clirunner, dataset_add_configs):
    clirunner(['metadata', 'add', dataset_add_configs.metadata])
    clirunner(['product', 'add', dataset_add_configs.products])

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

    if index_empty.supports_legacy:
        clirunner(['dataset', 'add', dataset_add_configs.datasets])
    else:
        # Does not support legacy datasets
        with pytest.raises(ValueError):
            # Expect to fail with legacy datasets
            clirunner(['dataset', 'add', dataset_add_configs.datasets])
        # Use EO3 datasets to allow subsequent tests to run.
        clirunner(['dataset', 'add', dataset_add_configs.datasets_eo3])

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


def test_readd_and_update_metadata_product_dataset_command(index_empty, clirunner, dataset_add_configs):
    clirunner(['metadata', 'add', dataset_add_configs.metadata])
    rerun_add = clirunner(['metadata', 'add', dataset_add_configs.metadata])
    assert "WARNING Metadata Type" in rerun_add.output
    assert "is already in the database" in rerun_add.output

    update = clirunner(['metadata', 'update', dataset_add_configs.metadata])
    assert "WARNING No changes detected for metadata type" in update.output

    add = clirunner(['product', 'add', dataset_add_configs.products])
    rerun_add = clirunner(['product', 'add', dataset_add_configs.products])
    assert "WARNING Product" in rerun_add.output
    assert "is already in the database" in rerun_add.output

    update = clirunner(['product', 'update', dataset_add_configs.products])
    assert "WARNING No changes detected for product" in update.output

    clirunner(['dataset', 'add', dataset_add_configs.datasets_eo3])
    rerun_add = clirunner(['dataset', 'add', dataset_add_configs.datasets_eo3])
    assert "WARNING Dataset" in rerun_add.output
    assert "is already in the database" in rerun_add.output

    update = clirunner(['dataset', 'update', dataset_add_configs.datasets_eo3])
    assert "1 successful, 0 failed" in update.output