import logging
import os
import shutil
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
import numpy as np
import rasterio
import yaml
from click.testing import CliRunner

from datacube.utils.documents import load_from_yaml

# On Windows, symlinks are not supported in Python 2 and require
# specific privileges otherwise, so we copy instead of linking
if os.name == 'nt' or not hasattr(os, 'symlink'):
    symlink = shutil.copy
else:
    symlink = os.symlink  # type: ignore

#: Number of bands to place in generated GeoTIFFs
NUM_BANDS = 3

# Resolution and chunking shrink factors
TEST_STORAGE_SHRINK_FACTORS = (100, 100)
TEST_STORAGE_NUM_MEASUREMENTS = 2
GEOGRAPHIC_VARS = ('latitude', 'longitude')
PROJECTED_VARS = ('x', 'y')

GEOTIFF = {
    'date': datetime(1990, 3, 2),
    'shape': {
        'x': 432,
        'y': 321
    },
    'pixel_size': {
        'x': 25.0,
        'y': -25.0
    },
    'crs': 'EPSG:28355',  # 'EPSG:28355'
    'ul': {
        'x': 638000.0,  # Coords must match crs
        'y': 6276000.0  # Coords must match crs
    }
}


@contextmanager
def alter_log_level(logger, level=logging.WARN):
    previous_level = logger.getEffectiveLevel()
    logger.setLevel(level)
    yield
    logger.setLevel(previous_level)


def assert_click_command(command, args):
    result = CliRunner().invoke(
        command,
        args=args,
        catch_exceptions=False
    )
    print(result.output)
    assert not result.exception
    assert result.exit_code == 0


def limit_num_measurements(dataset_type):
    if 'measurements' not in dataset_type:
        return
    measurements = dataset_type['measurements']
    if len(measurements) > TEST_STORAGE_NUM_MEASUREMENTS:
        dataset_type['measurements'] = measurements[:TEST_STORAGE_NUM_MEASUREMENTS]
    return dataset_type


def prepare_test_ingestion_configuration(tmpdir,
                                         output_dir,
                                         filename,
                                         mode=None):
    customizers = {
        'fast_ingest': edit_for_fast_ingest,
        'end2end': edit_for_end2end,
    }

    filename = Path(filename)
    if output_dir is None:
        output_dir = tmpdir.ensure(filename.stem, dir=True)
    config = load_yaml_file(filename)[0]

    if mode is not None:
        if mode not in customizers:
            raise ValueError('Wrong mode: ' + mode)
        config = customizers[mode](config)

    config['location'] = str(output_dir)

    # If ingesting with the s3test driver
    if 'bucket' in config['storage']:
        config['storage']['bucket'] = str(output_dir)

    config_path = tmpdir.join(filename.name)
    with open(str(config_path), 'w') as stream:
        yaml.safe_dump(config, stream)
    return config_path, config


def edit_for_end2end(config):
    storage = config.get('storage', {})

    storage['crs'] = 'EPSG:3577'
    storage['tile_size']['x'] = 100000.0
    storage['tile_size']['y'] = 100000.0

    config['storage'] = storage
    return config


def edit_for_fast_ingest(config):
    config = alter_product_for_testing(config)
    config['storage']['crs'] = 'EPSG:28355'
    config['storage']['chunking']['time'] = 1
    return config


def _make_geotiffs(tiffs_dir, day_offset, num_bands=NUM_BANDS):
    """
    Generate custom geotiff files, one per band.

    Create ``num_bands`` TIFF files inside ``tiffs_dir``.

    Return a dictionary mapping band_number to filename, eg::

        {
            0: '/tmp/tiffs/band01_time01.tif',
            1: '/tmp/tiffs/band02_time01.tif'
        }
    """
    tiffs = {}
    width = GEOTIFF['shape']['x']
    height = GEOTIFF['shape']['y']
    metadata = {'count': 1,
                'crs': GEOTIFF['crs'],
                'driver': 'GTiff',
                'dtype': 'int16',
                'width': width,
                'height': height,
                'nodata': -999.0,
                'transform': [GEOTIFF['pixel_size']['x'],
                              0.0,
                              GEOTIFF['ul']['x'],
                              0.0,
                              GEOTIFF['pixel_size']['y'],
                              GEOTIFF['ul']['y']]}

    for band in range(num_bands):
        path = str(tiffs_dir.join('band%02d_time%02d.tif' % ((band + 1), day_offset)))
        with rasterio.open(path, 'w', **metadata) as dst:
            # Write data in "corners" (rounded down by 100, for a size of 100x100)
            data = np.zeros((height, width), dtype=np.int16)
            data[:] = np.arange(height * width
                                ).reshape((height, width)) + 10 * band + day_offset
            '''
            lr = (100 * int(GEOTIFF['shape']['y'] / 100.0),
                  100 * int(GEOTIFF['shape']['x'] / 100.0))
            data[0:100, 0:100] = 100 + day_offset
            data[lr[0] - 100:lr[0], 0:100] = 200 + day_offset
            data[0:100, lr[1] - 100:lr[1]] = 300 + day_offset
            data[lr[0] - 100:lr[0], lr[1] - 100:lr[1]] = 400 + day_offset
            '''
            dst.write(data, 1)
        tiffs[band] = path
    return tiffs


def _make_ls5_scene_datasets(geotiffs, tmpdir):
    """

    Create directory structures like::

        LS5_TM_NBAR_P54_GANBAR01-002_090_084_01
        |---scene01
        |     |----- report.txt
        |     |----- LS5_TM_NBAR_P54_GANBAR01-002_090_084_01_B10.tif
        |     |----- LS5_TM_NBAR_P54_GANBAR01-002_090_084_01_B20.tif
        |     |----- LS5_TM_NBAR_P54_GANBAR01-002_090_084_01_B30.tif
        |--- agdc-metadata.yaml

    :param geotiffs: A list of dictionaries as output by :func:`geotiffs`
    :param tmpdir:
    :return:
    """
    dataset_dirs = {}
    dataset_dir = tmpdir.mkdir('ls5_dataset')
    for geotiff in geotiffs:
        # Make a directory for the dataset
        obs_name = 'LS5_TM_NBAR_P54_GANBAR01-002_090_084_%s' % geotiff['day']
        obs_dir = dataset_dir.mkdir(obs_name)
        symlink(str(geotiff['path']), str(obs_dir.join('agdc-metadata.yaml')))

        scene_dir = obs_dir.mkdir('scene01')
        scene_dir.join('report.txt').write('Example')
        geotiff_name = '%s_B{}0.tif' % obs_name
        for band in range(NUM_BANDS):
            path = scene_dir.join(geotiff_name.format(band + 1))
            symlink(str(geotiff['tiffs'][band]), str(path))
        dataset_dirs[geotiff['uuid']] = Path(str(obs_dir))
    return dataset_dirs


def load_yaml_file(filename):
    with open(str(filename)) as f:
        return list(load_from_yaml(f, parse_dates=True))


def is_geogaphic(storage_type):
    return 'latitude' in storage_type['storage']['resolution']


def shrink_storage_type(storage_type, variables, shrink_factors):
    storage = storage_type['storage']
    for var in variables:
        storage['resolution'][var] = storage['resolution'][var] * shrink_factors[0]
        storage['chunking'][var] = storage['chunking'][var] / shrink_factors[1]
    return storage_type


def load_test_products(filename, metadata_type=None):
    dataset_types = load_yaml_file(filename)
    return [alter_product_for_testing(dataset_type, metadata_type=metadata_type) for dataset_type in dataset_types]


def alter_product_for_testing(product, metadata_type=None):
    limit_num_measurements(product)
    if 'storage' in product:
        spatial_variables = GEOGRAPHIC_VARS if is_geogaphic(product) else PROJECTED_VARS
        product = shrink_storage_type(product,
                                      spatial_variables,
                                      TEST_STORAGE_SHRINK_FACTORS)
    if metadata_type:
        product['metadata_type'] = metadata_type.name
    return product
