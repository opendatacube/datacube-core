import logging
from contextlib import contextmanager
from pathlib import Path

import rasterio
import yaml
from click.testing import CliRunner

from integration_tests.conftest import TEST_STORAGE_NUM_MEASUREMENTS, load_yaml_file, alter_product_for_testing


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


def create_empty_geotiff(path):
    # Example method, not used
    metadata = {'count': 1,
                'crs': 'EPSG:28355',
                'driver': 'GTiff',
                'dtype': 'int16',
                'height': 8521,
                'nodata': -999.0,
                'transform': [25.0, 0.0, 638000.0, 0.0, -25.0, 6276000.0],
                'compress': 'lzw',
                'width': 9721}
    with rasterio.open(path, 'w', **metadata) as dst:
        pass


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
        output_dir = tmpdir.mkdir(filename.stem)
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
        yaml.dump(config, stream)
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