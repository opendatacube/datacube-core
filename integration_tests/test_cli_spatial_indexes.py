# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2023 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest


@pytest.mark.parametrize('datacube_env_name', ('experimental',))
def test_cli_spatial_indexes(index, clirunner):
    runner = clirunner(['spindex', 'list'], verbose_flag=False, expect_success=True)
    assert "epsg:4326" in runner.output
    assert "epsg:3577" not in runner.output
    assert runner.exit_code == 0

    runner = clirunner(['spindex', 'create', '3577'], verbose_flag=False, expect_success=True)
    assert runner.exit_code == 0

    runner = clirunner(['spindex', 'list'], verbose_flag=False, expect_success=True)
    assert "epsg:4326" in runner.output
    assert "epsg:3577" in runner.output
    assert runner.exit_code == 0

    runner = clirunner(['spindex', 'drop', '3577'], verbose_flag=False, expect_success=False)
    assert runner.exit_code == 1
    runner = clirunner(['spindex', 'drop', '--force', '3577'], verbose_flag=False, expect_success=True)
    assert runner.exit_code == 0

    runner = clirunner(['spindex', 'list'], verbose_flag=False, expect_success=True)
    assert "epsg:4326" in runner.output
    assert "epsg:3577" not in runner.output
    assert runner.exit_code == 0
