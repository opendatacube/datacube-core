from __future__ import absolute_import

import sys
import warnings
from datetime import datetime
from pathlib import Path
from subprocess import call, check_output, PIPE, CalledProcessError

import six
import netCDF4
import numpy as np
import pytest

import yaml
from click.testing import CliRunner

import datacube.scripts.run_ingest
import datacube.scripts.config_tool
from .conftest import LS5_NBAR_NAME, LS5_NBAR_ALBERS_NAME, EXAMPLE_LS5_DATASET_ID


PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs/config_samples/'
LS5_SAMPLES = CONFIG_SAMPLES / 'ga_landsat_5/'
LS5_NBAR_ALBERS_STORAGE_TYPE = LS5_SAMPLES / 'ls5_albers.yaml'

UTILS = PROJECT_ROOT / 'utils'
GA_LS_PREPARE_SCRIPT = UTILS / 'galsprepare.py'

TEST_DATA = PROJECT_ROOT / 'tests' / 'data'
LBG_SCENES = TEST_DATA / 'lbg'
LBG_NBAR = LBG_SCENES / 'LS5_TM_NBAR_P54_GANBAR01-002_090_084_19920323'
LBG_PQ = LBG_SCENES / 'LS5_TM_PQ_P55_GAPQ01-002_090_084_19920323'

ALBERS_ELEMENT_SIZE = 25

LBG_CELL_X = 15
LBG_CELL_Y = -40
LBG_CELL = (LBG_CELL_X, LBG_CELL_Y)


@pytest.mark.usefixtures('default_collection')
def test_end_to_end(global_integration_cli_args, index, example_ls5_dataset):
    """
    Loads two storage mapping configurations, then ingests a sample Landsat 5 scene

    One storage configuration specifies Australian Albers Equal Area Projection,
    the other is simply latitude/longitude.

    The input dataset should be recorded in the index, and two sets of netcdf storage units
    should be created on disk and recorded in the index.
    """

    # Copy scenes to a temp dir?
    # Run galsprepare.py on the NBAR and PQ scenes

    retcode = call(
        [
            'python',
            str(GA_LS_PREPARE_SCRIPT),
            str(LBG_NBAR)
        ],
        stderr=PIPE
    )
    assert retcode == 0

    # Add the LS5 Albers Example
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-vv',
            'storage',
            'add',
            str(LS5_NBAR_ALBERS_STORAGE_TYPE)
        ]
    )
    result = CliRunner().invoke(
        datacube.scripts.config_tool.cli,
        opts,
        catch_exceptions=False
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0

    # Run Ingest script on a dataset
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-vv',
            'ingest',
            str(LBG_NBAR)
        ]
    )
    result = CliRunner().invoke(
        datacube.scripts.run_ingest.cli,
        opts,
        catch_exceptions=False
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0

    # Run Ingest script on a dataset
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-vv',
            'ingest',
            str(LBG_PQ)
        ]
    )
    result = CliRunner().invoke(
        datacube.scripts.run_ingest.cli,
        opts,
        catch_exceptions=False
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0

    check_open_with_api(index)


def check_open_with_api(index):
    import datacube.api
    api = datacube.api.API(index=index)
    fields = api.list_fields()
    assert 'product' in fields
    descriptor = api.get_descriptor()
    assert 'ls5_nbar_albers' in descriptor
    storage_units = descriptor['ls5_nbar_albers']['storage_units']
    query = {
        'variables': ['blue'],
        'dimensions': {
            'latitude': {'range': (-34, -35)},
            'longitude': {'range': (149, 150)}}
    }
    data = api.get_data(query, storage_units=storage_units)
    assert abs(data['element_sizes'][1] - ALBERS_ELEMENT_SIZE) < .0000001
    assert abs(data['element_sizes'][2] - ALBERS_ELEMENT_SIZE) < .0000001

    data_array = api.get_data_array(storage_type='ls5_nbar_albers', variables=['blue'],
                                    latitude=(-34, -35), longitude=(149, 150))
    assert data_array.size

    dataset = api.get_dataset(storage_type='ls5_nbar_albers', variables=['blue'],
                              latitude=(-34, -35), longitude=(149, 150))
    assert dataset['blue'].size

    data_array_cell = api.get_data_array_by_cell(LBG_CELL, storage_type='ls5_nbar_albers', variables=['blue'])
    assert data_array_cell.size

    data_array_cell = api.get_data_array_by_cell(x_index=LBG_CELL_X, y_index=LBG_CELL_Y,
                                                 storage_type='ls5_nbar_albers', variables=['blue'])
    assert data_array_cell.size

    dataset_cell = api.get_dataset_by_cell(LBG_CELL, storage_type='ls5_nbar_albers', variables=['blue'])
    assert dataset_cell['blue'].size

    dataset_cell = api.get_dataset_by_cell([LBG_CELL], storage_type='ls5_nbar_albers', variables=['blue'])
    assert dataset_cell['blue'].size

    dataset_cell = api.get_dataset_by_cell(x_index=LBG_CELL_X, y_index=LBG_CELL_Y, storage_type='ls5_nbar_albers',
                                           variables=['blue'])
    assert dataset_cell['blue'].size

    tiles = api.list_tiles(x_index=LBG_CELL_X, y_index=LBG_CELL_Y, storage_type='ls5_nbar_albers')
    for tile_query, tile_attrs in tiles:
        dataset = api.get_dataset_by_cell(**tile_query)
        assert dataset['blue'].size
