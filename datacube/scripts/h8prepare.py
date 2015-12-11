# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

import uuid
import logging
import re
from pathlib import Path
import yaml
import netCDF4
import rasterio.warp
import click


def get_projection(proj, extent):
    left, bottom, right, top = [int(x) for x in
                                rasterio.warp.transform_bounds({'init': 'EPSG:4326'}, proj, *extent)]
    return {
        'spatial_reference': proj,
        'geo_ref_points': {
            'ul': {'x': left, 'y': top},
            'ur': {'x': right, 'y': top},
            'll': {'x': left, 'y': bottom},
            'lr': {'x': right, 'y': bottom},
            }
        }


def get_coords(left, bottom, right, top):
    return {
        'ul': {'lon': left, 'lat': top},
        'ur': {'lon': right, 'lat': top},
        'll': {'lon': left, 'lat': bottom},
        'lr': {'lon': right, 'lat': bottom},
    }


def get_skeleton(path, prod, bands, extent):
    image = netCDF4.Dataset(path)
    times = image['time']
    sensing_time = str(netCDF4.num2date(times[0], units=times.units, calendar=times.calendar))

    if 'geostationary' in image.variables:
        projection = str(image['geostationary'].spatial_ref)
    else:
        projection = str(image['geostationary_satellite'].spatial_ref)

    return {
        'id': str(uuid.uuid4()),
        'ga_label': str(image.id),
        'ga_level': str(image.processing_level),
        'product_type': prod,
        'creation_dt': str(image.date_created),
        'platform': {'code': 'HIMAWARI_8'},
        'instrument': {'name': str(image.instrument)},
        # 'acquisition': {'groundstation': {'code': station}},
        'extent': {
            'coord': get_coords(*extent),
            'from_dt': sensing_time,
            'to_dt': sensing_time,
            'center_dt': sensing_time
        },
        'format': {'name': 'NetCDF4'},
        'grid_spatial': {
            'projection': get_projection(projection, extent)
        },
        'image': {
            'bands': bands
        },
        'lineage': {'source_datasets': {}},
    }


def get_ang_dataset(path, extent):
    band_re = re.compile('.*-P1S-ABOM_GEOM_(.*)-PRJ.*_(500|1000|2000)-HIMAWARI8-AHI.nc')
    images = {}
    for image in path.glob('*-P1S-ABOM_GEOM_*-HIMAWARI8-AHI.nc'):
        match = band_re.match(str(image)).groups()
        images['%s_%s' % match] = {
            'path': str(image),
            'layer': 'solar_zenith_angle',
        }
    if not images:
        return None
    return get_skeleton(str(images['SOLAR_2000']['path']), 'GEOM_SOLAR', images, extent)


def get_obs_dataset(path, extent):
    band_re = re.compile('.*-P1S-ABOM_OBS_B(.*)-PRJ.*_(500|1000|2000)-HIMAWARI8-AHI.nc')
    images = {}
    for image in path.glob('*-P1S-ABOM_OBS_*-HIMAWARI8-AHI.nc'):
        match = band_re.match(str(image)).groups()
        images['%s_%s' % match] = {
            'path': str(image),
            'layer': 'channel_00' + match[0] + '_scaled_radiance',
        }
    if not images:
        return None
    return get_skeleton(str(images['01_2000']['path']), 'OBS', images, extent)


def get_brf_dataset(path, extent):
    extent = 110, -40, 155, 3
    band_re = re.compile('.*-P1S-ABOM_BRF_B(.*)-PRJ.*_(500|1000|2000)-HIMAWARI8-AHI.nc')
    images = {}
    for image in path.glob('*-P1S-ABOM_BRF_*-HIMAWARI8-AHI.nc'):
        match = band_re.match(str(image)).groups()
        images['%s_%s' % match] = {
            'path': str(image),
            'layer': 'channel_00' + match[0] + '_brf',
        }
    if not images:
        return None
    return get_skeleton(str(images['01_2000']['path']), 'BRF', images, extent)


def prepare_dataset(path):
    extent = 110, -40, 155, 3
    brf = get_brf_dataset(path, extent)
    if not brf:
        return []
    ang = get_ang_dataset(path, extent)
    obs = get_obs_dataset(path, extent)
    brf['lineage']['source_datasets'] = {ds['id'] for ds in [ang, obs] if ds}
    return [brf]


@click.command(help="Prepare Himawari 8 dataset for ingestion into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=-1)
def main(datasets):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    for dataset in datasets:
        path = Path(dataset)

        logging.info("Processing %s", path)
        documents = prepare_dataset(path)
        if documents:
            yaml_path = str(path.joinpath('agdc-metadata.yaml'))
            logging.info("Writing %s dataset(s) into %s", len(documents), yaml_path)
            with open(yaml_path, 'w') as stream:
                yaml.dump_all(documents, stream)
        else:
            logging.info("No datasets discovered. Bye!")


if __name__ == "__main__":
    main()
