from __future__ import absolute_import

import shutil
from subprocess import call

import pytest
from click.testing import CliRunner
from pathlib import Path

import datacube.scripts.agdc
import datacube.scripts.config_tool

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs/config_samples/'
LS5_DATASET_TYPES = CONFIG_SAMPLES / 'dataset_types/ls5_scenes.yaml'
TEST_DATA = PROJECT_ROOT / 'tests' / 'data' / 'lbg'

INGESTER_CONFIGS = CONFIG_SAMPLES / 'ingester'

LS5_NBAR_ALBERS = 'ls5_nbar_albers.yaml'
LS5_PQ_ALBERS = 'ls5_pq_albers.yaml'

MATCH_RULES = CONFIG_SAMPLES / 'match_rules/ls5_scenes.yaml'

GA_LS_PREPARE_SCRIPT = PROJECT_ROOT / 'utils/galsprepare.py'

LBG_NBAR = 'LS5_TM_NBAR_P54_GANBAR01-002_090_084_19920323'
LBG_PQ = 'LS5_TM_PQ_P55_GAPQ01-002_090_084_19920323'

ALBERS_ELEMENT_SIZE = 25

LBG_CELL_X = 15
LBG_CELL_Y = -40
LBG_CELL = (LBG_CELL_X, LBG_CELL_Y)


@pytest.fixture()
def testdata_dir(tmpdir):
    datadir = Path(str(tmpdir), 'data')
    datadir.mkdir()

    shutil.copytree(str(TEST_DATA), str(tmpdir / 'lbg'))

    copy_and_update_ingestion_configs(tmpdir, tmpdir,
                                      (INGESTER_CONFIGS / file for file in (LS5_NBAR_ALBERS, LS5_PQ_ALBERS)))

    return tmpdir


def copy_and_update_ingestion_configs(destination, output_dir, configs):
    for ingestion_config in configs:
        with ingestion_config.open() as input:
            output_file = destination / ingestion_config.name
            with output_file.open(mode='w') as output:
                for line in input:
                    if 'location: ' in line:
                        line = 'location: ' + str(output_dir) + '\n'
                    output.write(line)


ignore_me = pytest.mark.xfail(True, reason="get_data/get_description still to be fixed in Unification")


@ignore_me
@pytest.mark.usefixtures('default_metadata_type')
def test_end_to_end(global_integration_cli_args, index, testdata_dir):
    """
    Loads two dataset configurations, then ingests a sample Landsat 5 scene

    One dataset configuration specifies Australian Albers Equal Area Projection,
    the other is simply latitude/longitude.

    The input dataset should be recorded in the index, and two sets of netcdf storage units
    should be created on disk and recorded in the index.
    """

    lbg_nbar = testdata_dir / 'lbg' / LBG_NBAR
    lbg_pq = testdata_dir / 'lbg' / LBG_PQ
    ls5_nbar_albers_ingest_config = testdata_dir / LS5_NBAR_ALBERS
    ls5_pq_albers_ingest_config = testdata_dir / LS5_PQ_ALBERS

    # Run galsprepare.py on the NBAR and PQ scenes
    retcode = call(
        [
            'python',
            str(GA_LS_PREPARE_SCRIPT),
            str(lbg_nbar)
        ]
    )
    assert retcode == 0

    # Add the LS5 Dataset Types
    run_click_command(datacube.scripts.config_tool.cli,
                      global_integration_cli_args + ['-vv', 'type', 'add', str(LS5_DATASET_TYPES)])

    # Index the Datasets
    run_click_command(datacube.scripts.agdc.cli,
                      global_integration_cli_args +
                      ['-vv', 'index', '--match-rules', str(MATCH_RULES),
                       str(lbg_nbar), str(lbg_pq)])

    # Ingest NBAR
    run_click_command(datacube.scripts.agdc.cli,
                      global_integration_cli_args +
                      ['-vv', 'ingest', '-c', str(ls5_nbar_albers_ingest_config)])

    # Ingest PQ
    run_click_command(datacube.scripts.agdc.cli,
                      global_integration_cli_args +
                      ['-vv', 'ingest', '-c', str(ls5_pq_albers_ingest_config)])

    check_open_with_api(index)


def run_click_command(command, args):
    result = CliRunner().invoke(
        command,
        args=args,
        catch_exceptions=False
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0


def check_open_with_api(index):
    from datacube.api.core import API
    api = API(index=index)

    # fields = api.list_fields()
    # assert 'product' in fields

    descriptor = api.get_descriptor()
    assert 'ls5_nbar_albers' in descriptor
    groups = descriptor['ls5_nbar_albers']['groups']
    query = {
        'variables': ['blue'],
        'dimensions': {
            'latitude': {'range': (-34, -35)},
            'longitude': {'range': (149, 150)}}
    }
    data = api.get_data(query)  # , dataset_groups=groups)
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
