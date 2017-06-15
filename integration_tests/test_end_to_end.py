from __future__ import absolute_import

import shutil
from subprocess import call

import numpy
import rasterio

import pytest
from click.testing import CliRunner
from pathlib import Path

import datacube.scripts.cli_app
from datacube.compat import string_types

import imp

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs/config_samples/'
LS5_DATASET_TYPES = CONFIG_SAMPLES / 'dataset_types/ls5_scenes.yaml'
TEST_DATA = PROJECT_ROOT / 'tests' / 'data' / 'lbg'

INGESTER_CONFIGS = CONFIG_SAMPLES / 'ingester'

LS5_NBAR_ALBERS = 'ls5_nbar_albers.yaml'
LS5_PQ_ALBERS = 'ls5_pq_albers.yaml'

GA_LS_PREPARE_SCRIPT = PROJECT_ROOT / 'utils/galsprepare.py'

galsprepare = imp.load_source('module.name', str(GA_LS_PREPARE_SCRIPT))

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
    run_click_command(galsprepare.main, [str(lbg_nbar)])

    # Add the LS5 Dataset Types
    run_click_command(datacube.scripts.cli_app.cli,
                      global_integration_cli_args + ['-v', 'product', 'add', str(LS5_DATASET_TYPES)])

    # Index the Datasets
    run_click_command(datacube.scripts.cli_app.cli,
                      global_integration_cli_args +
                      ['-v', 'dataset', 'add', '--auto-match',
                       str(lbg_nbar), str(lbg_pq)])

    # Ingest NBAR
    run_click_command(datacube.scripts.cli_app.cli,
                      global_integration_cli_args +
                      ['-v', 'ingest', '-c', str(ls5_nbar_albers_ingest_config)])

    # Ingest PQ
    run_click_command(datacube.scripts.cli_app.cli,
                      global_integration_cli_args +
                      ['-v', 'ingest', '-c', str(ls5_pq_albers_ingest_config)])

    check_open_with_api(index)
    check_open_with_dc(index)
    check_open_with_grid_workflow(index)
    check_analytics_list_searchables(index)
    check_get_descriptor(index)
    check_get_data(index)
    check_get_data_subset(index)
    check_get_descriptor_data(index)
    check_get_descriptor_data_storage_type(index)
    check_analytics_create_array(index)
    check_analytics_ndvi_mask_median_expression(index)
    check_analytics_ndvi_mask_median_expression_storage_type(index)
    check_analytics_pixel_drill(index)


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
    from datacube.api import API
    api = API(index=index)

    # fields = api.list_fields()
    # assert 'product' in fields

    descriptor = api.get_descriptor()
    assert 'ls5_nbar_albers' in descriptor
    groups = descriptor['ls5_nbar_albers']['groups']
    query = {
        'variables': ['blue'],
        'dimensions': {
            'latitude': {'range': (-35, -36)},
            'longitude': {'range': (149, 150)}}
    }
    data = api.get_data(query)  # , dataset_groups=groups)
    assert abs(data['element_sizes'][1] - ALBERS_ELEMENT_SIZE) < .0000001
    assert abs(data['element_sizes'][2] - ALBERS_ELEMENT_SIZE) < .0000001


def check_open_with_dc(index):
    from datacube.api.core import Datacube
    dc = Datacube(index=index)

    data_array = dc.load(product='ls5_nbar_albers', measurements=['blue'], stack='variable')
    assert data_array.shape
    assert (data_array != -999).any()

    data_array = dc.load(product='ls5_nbar_albers', measurements=['blue'], time='1992-03-23T23:14:25.500000')
    assert data_array['blue'].shape[0] == 1
    assert (data_array.blue != -999).any()

    data_array = dc.load(product='ls5_nbar_albers', measurements=['blue'], latitude=-35.3, longitude=149.1)
    assert data_array['blue'].shape[1:] == (1, 1)
    assert (data_array.blue != -999).any()

    data_array = dc.load(product='ls5_nbar_albers', latitude=(-35, -36), longitude=(149, 150), stack='variable')
    assert data_array.ndim == 4
    assert 'variable' in data_array.dims
    assert (data_array != -999).any()

    with rasterio.Env():
        lazy_data_array = dc.load(product='ls5_nbar_albers', latitude=(-35, -36), longitude=(149, 150),
                                  stack='variable', dask_chunks={'time': 1, 'x': 1000, 'y': 1000})
        assert lazy_data_array.data.dask
        assert lazy_data_array.ndim == data_array.ndim
        assert 'variable' in lazy_data_array.dims
        assert lazy_data_array[1, :2, 950:1050, 950:1050].equals(data_array[1, :2, 950:1050, 950:1050])

    dataset = dc.load(product='ls5_nbar_albers', measurements=['blue'])
    assert dataset['blue'].size

    dataset = dc.load(product='ls5_nbar_albers', latitude=(-35.2, -35.3), longitude=(149.1, 149.2))
    assert dataset['blue'].size

    with rasterio.Env():
        lazy_dataset = dc.load(product='ls5_nbar_albers', latitude=(-35.2, -35.3), longitude=(149.1, 149.2),
                               dask_chunks={'time': 1})
        assert lazy_dataset['blue'].data.dask
        assert lazy_dataset.blue[:2, :100, :100].equals(dataset.blue[:2, :100, :100])
        assert lazy_dataset.isel(time=slice(0, 2), x=slice(950, 1050), y=slice(950, 1050)).equals(
            dataset.isel(time=slice(0, 2), x=slice(950, 1050), y=slice(950, 1050)))

    dataset_like = dc.load(product='ls5_nbar_albers', measurements=['blue'], like=dataset)
    assert (dataset.blue == dataset_like.blue).all()

    solar_day_dataset = dc.load(product='ls5_nbar_albers',
                                latitude=(-35, -36), longitude=(149, 150),
                                measurements=['blue'], group_by='solar_day')
    assert 0 < solar_day_dataset.time.size <= dataset.time.size

    dataset = dc.load(product='ls5_nbar_albers', latitude=(-35.2, -35.3), longitude=(149.1, 149.2), align=(5, 20))
    assert dataset.geobox.affine.f % abs(dataset.geobox.affine.e) == 5
    assert dataset.geobox.affine.c % abs(dataset.geobox.affine.a) == 20
    dataset_like = dc.load(product='ls5_nbar_albers', measurements=['blue'], like=dataset)
    assert (dataset.blue == dataset_like.blue).all()

    products_df = dc.list_products()
    assert len(products_df)
    assert len(products_df[products_df['name'].isin(['ls5_nbar_albers'])])
    assert len(products_df[products_df['name'].isin(['ls5_pq_albers'])])

    assert len(dc.list_measurements())

    resamp = ['nearest', 'cubic', 'bilinear', 'cubic_spline', 'lanczos', 'average']
    results = {}

    # WTF
    def calc_max_change(da):
        midline = int(da.shape[0] * 0.5)
        a = int(abs(da[midline, :-1].data - da[midline, 1:].data).max())

        centerline = int(da.shape[1] * 0.5)
        b = int(abs(da[:-1, centerline].data - da[1:, centerline].data).max())
        return a + b

    for resamp_meth in resamp:
        dataset = dc.load(product='ls5_nbar_albers', measurements=['blue'],
                          latitude=(-35.28, -35.285), longitude=(149.15, 149.155),
                          output_crs='EPSG:4326', resolution=(-0.0000125, 0.0000125), resampling=resamp_meth)
        results[resamp_meth] = calc_max_change(dataset.blue.isel(time=0))

    assert results['cubic_spline'] < results['nearest']
    assert results['lanczos'] < results['average']


def check_open_with_grid_workflow(index):
    type_name = 'ls5_nbar_albers'
    dt = index.products.get_by_name(type_name)

    from datacube.api.grid_workflow import GridWorkflow
    gw = GridWorkflow(index, dt.grid_spec)

    cells = gw.list_cells(product=type_name, cell_index=LBG_CELL)
    assert LBG_CELL in cells

    cells = gw.list_cells(product=type_name)
    assert LBG_CELL in cells

    tile = cells[LBG_CELL]
    assert 'x' in tile.dims
    assert 'y' in tile.dims
    assert 'time' in tile.dims
    assert tile.shape[1] == 4000
    assert tile.shape[2] == 4000
    assert tile[:1, :100, :100].shape == (1, 100, 100)
    dataset_cell = gw.load(tile, measurements=['blue'])
    assert dataset_cell['blue'].shape == tile.shape

    for timestamp, tile_slice in tile.split('time'):
        assert tile_slice.shape == (1, 4000, 4000)

    dataset_cell = gw.load(tile)
    assert all(m in dataset_cell for m in ['blue', 'green', 'red', 'nir', 'swir1', 'swir2'])

    ts = numpy.datetime64('1992-03-23T23:14:25.500000000')
    tile_key = LBG_CELL + (ts,)
    tiles = gw.list_tiles(product=type_name)
    assert tiles
    assert tile_key in tiles

    tile = tiles[tile_key]
    dataset_cell = gw.load(tile, measurements=['blue'])
    assert dataset_cell['blue'].size

    dataset_cell = gw.load(tile)
    assert all(m in dataset_cell for m in ['blue', 'green', 'red', 'nir', 'swir1', 'swir2'])


def check_analytics_list_searchables(index):
    from datacube.analytics.analytics_engine import AnalyticsEngine

    a = AnalyticsEngine(index=index)
    result = a.list_searchables()

    assert len(result) > 0
    for storage_type in result:
        assert len(result[storage_type]['bands']) > 0
        assert len(list(result[storage_type]['dimensions'])) > 0
        assert result[storage_type]['instrument']
        assert result[storage_type]['platform']
        assert result[storage_type]['product_type']
        assert result[storage_type]['storage_type']


def check_get_descriptor(index):
    from datetime import datetime
    from datacube.api import API

    g = API(index=index)

    platform = 'LANDSAT_5'
    product = 'nbar'
    var1 = 'red'
    var2 = 'nir'

    data_request_descriptor = {
        'platform': platform,
        'product': product,
        'variables': (var1, var2),
        'dimensions': {
            'longitude': {
                'range': (149.07, 149.18)
            },
            'latitude': {
                'range': (-35.32, -35.28)
            },
            'time': {
                'range': (datetime(1992, 1, 1), datetime(1992, 12, 31))
            }
        }
    }

    d = g.get_descriptor(data_request_descriptor)
    assert 'storage_units' in list(d.values())[0].keys()
    assert 'dimensions' in list(d.values())[0].keys()
    assert 'result_max' in list(d.values())[0].keys()
    assert 'irregular_indices' in list(d.values())[0].keys()
    assert 'variables' in list(d.values())[0].keys()
    assert 'result_min' in list(d.values())[0].keys()
    assert 'result_shape' in list(d.values())[0].keys()

    assert isinstance(list(d.values())[0]['storage_units'], dict)
    assert isinstance(list(d.values())[0]['dimensions'], list)
    assert isinstance(list(d.values())[0]['result_max'], tuple)
    assert isinstance(list(d.values())[0]['irregular_indices'], dict)
    assert isinstance(list(d.values())[0]['result_min'], tuple)
    assert isinstance(list(d.values())[0]['variables'], dict)
    assert isinstance(list(d.values())[0]['result_shape'], tuple)

    assert len(list(d.values())[0]['dimensions']) == \
        len(list(d.values())[0]['dimensions']) == \
        len(list(d.values())[0]['result_shape']) == \
        len(list(d.values())[0]['result_max']) == \
        len(list(d.values())[0]['result_min'])

    for key in list(d.values())[0]['irregular_indices'].keys():
        assert key in list(d.values())[0]['dimensions']

    assert var1 in list(d.values())[0]['variables']
    assert var2 in list(d.values())[0]['variables']

    assert 'datatype_name' in list(d.values())[0]['variables'][var1].keys()
    assert 'nodata_value' in list(d.values())[0]['variables'][var1].keys()

    assert 'datatype_name' in list(d.values())[0]['variables'][var2].keys()
    assert 'nodata_value' in list(d.values())[0]['variables'][var2].keys()

    for su in list(d.values())[0]['storage_units'].values():
        assert 'irregular_indices' in su
        assert 'storage_max' in su
        assert 'storage_min' in su
        assert 'storage_path' in su
        assert 'storage_shape' in su
        assert isinstance(su['irregular_indices'], dict)
        assert isinstance(su['storage_max'], tuple)
        assert isinstance(su['storage_min'], tuple)
        assert isinstance(su['storage_path'], string_types)
        assert isinstance(su['storage_shape'], tuple)


def check_get_data(index):
    import numpy as np
    import xarray as xr
    from datetime import datetime
    from datacube.api import API

    g = API(index=index)

    platform = 'LANDSAT_5'
    product = 'nbar'
    var1 = 'red'
    var2 = 'nir'

    data_request_descriptor = {
        'platform': platform,
        'product': product,
        'variables': (var1, var2),
        'dimensions': {
            'longitude': {
                'range': (149.07, 149.18)
            },
            'latitude': {
                'range': (-35.32, -35.28)
            },
            'time': {
                'range': (datetime(1992, 1, 1), datetime(1992, 12, 31))
            }
        }
    }

    d = g.get_data(data_request_descriptor)
    assert 'dimensions' in list(d.keys())
    assert 'arrays' in list(d.keys())
    assert 'element_sizes' in list(d.keys())
    assert 'indices' in list(d.keys())
    assert 'coordinate_reference_systems' in list(d.keys())
    assert 'size' in list(d.keys())

    assert isinstance(d['dimensions'], list)
    assert isinstance(d['arrays'], dict)
    assert isinstance(d['element_sizes'], list)
    assert isinstance(d['indices'], dict)
    assert isinstance(d['coordinate_reference_systems'], list)
    assert isinstance(d['size'], tuple)

    assert len(list(d['dimensions'])) == \
        len(list(d['coordinate_reference_systems'])) == \
        len(list(d['element_sizes'])) == \
        len(list(d['indices'])) == \
        len(list(d['size']))

    for key in list(d['indices'].keys()):
        assert key in list(d['dimensions'])

    assert var1 in list(d['arrays'].keys())
    assert var2 in list(d['arrays'].keys())

    for crs in d['coordinate_reference_systems']:
        assert 'reference_system_definition' in crs
        assert 'reference_system_unit' in crs
        assert isinstance(crs['reference_system_definition'], string_types)
        assert isinstance(crs['reference_system_unit'], string_types)

    for dim in d['indices']:
        assert isinstance(d['indices'][dim], np.ndarray)

    assert isinstance(d['arrays'][var1], xr.core.dataarray.DataArray)
    assert isinstance(d['arrays'][var2], xr.core.dataarray.DataArray)

    assert d['arrays'][var1].shape == d['size']
    assert d['arrays'][var2].shape == d['size']

    assert d['arrays'][var1].name == var1
    assert d['arrays'][var2].name == var2

    assert len(list(d['arrays'][var1].dims)) == len(list(d['dimensions']))
    assert len(list(d['arrays'][var2].dims)) == len(list(d['dimensions']))

    for dim in list(d['dimensions']):
        assert dim in list(d['arrays'][var1].dims)
        assert dim in list(d['arrays'][var2].dims)


def check_get_data_subset(index):
    from datetime import datetime
    from datacube.api import API

    g = API(index=index)

    platform = 'LANDSAT_5'
    product = 'nbar'
    var1 = 'red'
    var2 = 'nir'

    data_request_descriptor = {
        'platform': platform,
        'product': product,
        'variables': (var1, var2),
        'dimensions': {
            'x': {
                'range': (149.07, 149.18),
                'array_range': (5, 10)
            },
            'y': {
                'range': (-35.32, -35.28),
                'array_range': (5, 10)
            },
            'time': {
                'range': (datetime(1992, 1, 1), datetime(1992, 12, 31))
            }
        }
    }

    d = g.get_data(data_request_descriptor)

    assert d['arrays'][var1].shape == (1, 5, 5)
    assert d['arrays'][var2].shape == (1, 5, 5)


def check_get_descriptor_data(index):
    from datetime import datetime
    from datacube.api import API

    g = API(index=index)

    platform = 'LANDSAT_5'
    product = 'nbar'
    var1 = 'red'
    var2 = 'nir'

    data_request_descriptor = {
        'platform': platform,
        'product': product,
        'variables': (var1, var2),
        'dimensions': {
            'longitude': {
                'range': (149.07, 149.18)
            },
            'latitude': {
                'range': (-35.32, -35.28)
            },
            'time': {
                'range': (datetime(1992, 1, 1), datetime(1992, 12, 31))
            }
        }
    }

    d1 = g.get_descriptor(data_request_descriptor)
    d2 = g.get_data(data_request_descriptor)

    assert list(d1.values())[0]['result_shape'] == \
        d2['size'] == \
        d2['arrays'][var1].shape == \
        d2['arrays'][var2].shape

    assert d2['arrays'][var1].shape[0] > 0
    assert d2['arrays'][var1].shape[1] > 0
    assert d2['arrays'][var1].shape[2] > 0

    assert d2['arrays'][var1].shape[0] > 0
    assert d2['arrays'][var2].shape[1] > 0
    assert d2['arrays'][var2].shape[2] > 0


def check_get_descriptor_data_storage_type(index):
    from datetime import datetime
    from datacube.api import API

    g = API(index=index)

    storage_type = 'ls5_nbar_albers'
    var1 = 'red'
    var2 = 'nir'

    data_request_descriptor = {
        'storage_type': storage_type,
        'variables': (var1, var2),
        'dimensions': {
            'longitude': {
                'range': (149.07, 149.18)
            },
            'latitude': {
                'range': (-35.32, -35.28)
            },
            'time': {
                'range': (datetime(1992, 1, 1), datetime(1992, 12, 31))
            }
        }
    }

    d1 = g.get_descriptor(data_request_descriptor)
    d2 = g.get_data(data_request_descriptor)

    assert list(d1.values())[0]['result_shape'] == \
        d2['size'] == \
        d2['arrays'][var1].shape == \
        d2['arrays'][var2].shape

    assert d2['arrays'][var1].shape[0] > 0
    assert d2['arrays'][var1].shape[1] > 0
    assert d2['arrays'][var1].shape[2] > 0

    assert d2['arrays'][var1].shape[0] > 0
    assert d2['arrays'][var2].shape[1] > 0
    assert d2['arrays'][var2].shape[2] > 0


def check_analytics_create_array(index):
    from datetime import datetime
    from datacube.analytics.analytics_engine import AnalyticsEngine
    from datacube.execution.execution_engine import ExecutionEngine

    a = AnalyticsEngine(index=index)
    e = ExecutionEngine(index=index)

    platform = 'LANDSAT_5'
    product = 'nbar'
    var1 = 'red'
    var2 = 'nir'

    # Lake Burley Griffin
    dimensions = {'x':    {'range': (149.07, 149.18)},
                  'y':    {'range': (-35.32, -35.28)},
                  'time': {'range': (datetime(1992, 1, 1), datetime(1992, 12, 31))}}

    arrays = a.create_array((platform, product), [var1, var2], dimensions, 'get_data')

    e.execute_plan(a.plan)

    assert e.cache['get_data']


def check_analytics_ndvi_mask_median_expression(index):
    from datetime import datetime
    from datacube.analytics.analytics_engine import AnalyticsEngine
    from datacube.execution.execution_engine import ExecutionEngine

    a = AnalyticsEngine(index=index)
    e = ExecutionEngine(index=index)

    platform = 'LANDSAT_5'
    product = 'nbar'
    var1 = 'nir'
    var2 = 'red'
    pq_product = 'pqa'
    pq_var = 'pixelquality'

    # Lake Burley Griffin
    dimensions = {'x':    {'range': (149.07, 149.18)},
                  'y':    {'range': (-35.32, -35.28)},
                  'time': {'range': (datetime(1992, 1, 1), datetime(1992, 12, 31))}}

    b40 = a.create_array((platform, product), [var1], dimensions, 'b40')
    b30 = a.create_array((platform, product), [var2], dimensions, 'b30')
    pq = a.create_array((platform, pq_product), [pq_var], dimensions, 'pq')

    ndvi = a.apply_expression([b40, b30], '((array1 - array2) / (array1 + array2))', 'ndvi')
    mask = a.apply_expression([ndvi, pq], 'array1{(array2 == 32767) | (array2 == 16383) | (array2 == 2457)}', 'mask')
    median_t = a.apply_expression(mask, 'median(array1, 0)', 'medianT')

    result = e.execute_plan(a.plan)

    assert e.cache['b40']
    assert e.cache['b30']
    assert e.cache['pq']
    assert e.cache['b40']['array_result'][var1].size > 0
    assert e.cache['b30']['array_result'][var2].size > 0
    assert e.cache['pq']['array_result'][pq_var].size > 0

    assert e.cache['ndvi']
    assert e.cache['mask']
    assert e.cache['medianT']


def check_analytics_ndvi_mask_median_expression_storage_type(index):
    from datetime import datetime
    from datacube.analytics.analytics_engine import AnalyticsEngine
    from datacube.execution.execution_engine import ExecutionEngine

    a = AnalyticsEngine(index=index)
    e = ExecutionEngine(index=index)

    nbar_storage_type = 'ls5_nbar_albers'
    var1 = 'nir'
    var2 = 'red'
    pq_storage_type = 'ls5_pq_albers'
    pq_var = 'pixelquality'

    # Lake Burley Griffin
    dimensions = {'x':    {'range': (149.07, 149.18)},
                  'y':    {'range': (-35.32, -35.28)},
                  'time': {'range': (datetime(1992, 1, 1), datetime(1992, 12, 31))}}

    b40 = a.create_array(nbar_storage_type, [var1], dimensions, 'b40')
    b30 = a.create_array(nbar_storage_type, [var2], dimensions, 'b30')
    pq = a.create_array(pq_storage_type, [pq_var], dimensions, 'pq')

    ndvi = a.apply_expression([b40, b30], '((array1 - array2) / (array1 + array2))', 'ndvi')
    mask = a.apply_expression([ndvi, pq], 'array1{(array2 == 32767) | (array2 == 16383) | (array2 == 2457)}', 'mask')
    median_t = a.apply_expression(mask, 'median(array1, 0)', 'medianT')

    result = e.execute_plan(a.plan)

    assert e.cache['b40']
    assert e.cache['b30']
    assert e.cache['pq']
    assert e.cache['b40']['array_result'][var1].size > 0
    assert e.cache['b30']['array_result'][var2].size > 0
    assert e.cache['pq']['array_result'][pq_var].size > 0

    assert e.cache['ndvi']
    assert e.cache['mask']
    assert e.cache['medianT']


def check_analytics_pixel_drill(index):
    from datetime import datetime
    from datacube.analytics.analytics_engine import AnalyticsEngine
    from datacube.execution.execution_engine import ExecutionEngine

    a = AnalyticsEngine(index=index)
    e = ExecutionEngine(index=index)

    nbar_storage_type = 'ls5_nbar_albers'
    var1 = 'nir'
    var2 = 'red'
    pq_storage_type = 'ls5_pq_albers'
    pq_var = 'pixelquality'

    # Lake Burley Griffin
    dimensions = {'x':    {'range': (149.12)},
                  'y':    {'range': (-35.30)},
                  'time': {'range': (datetime(1992, 1, 1), datetime(1992, 12, 31))}}

    b40 = a.create_array(nbar_storage_type, [var1], dimensions, 'b40')
    b30 = a.create_array(nbar_storage_type, [var2], dimensions, 'b30')
    pq = a.create_array(pq_storage_type, [pq_var], dimensions, 'pq')

    result = e.execute_plan(a.plan)
    assert e.cache['b40']
    assert e.cache['b30']
    assert e.cache['pq']
    assert e.cache['b40']['array_result'][var1].size > 0
    assert e.cache['b30']['array_result'][var2].size > 0
    assert e.cache['pq']['array_result'][pq_var].size > 0
