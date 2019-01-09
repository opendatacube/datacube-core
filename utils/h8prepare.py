# coding=utf-8
"""
Ingest data from the command-line.
"""

import uuid
import logging
import re
from pathlib import Path
import yaml
import netCDF4
import click


def get_projection(image):
    if 'geostationary' in image.variables:
        projection = str(image['geostationary'].spatial_ref)
        transform = image['geostationary'].GeoTransform
    else:
        projection = str(image['geostationary_satellite'].spatial_ref)
        transform = image['geostationary_satellite'].GeoTransform

    left = transform[0].item()
    bottom = transform[3].item()
    right = left + transform[1].item() * image['x'].size
    top = bottom + transform[5].item() * image['y'].size
    if left > right:
        left, right = right, left
    if bottom > top:
        bottom, top = top, bottom
    return {
        'spatial_reference': projection,
        'geo_ref_points': {
            'ul': {'x': left, 'y': top},
            'ur': {'x': right, 'y': top},
            'll': {'x': left, 'y': bottom},
            'lr': {'x': right, 'y': bottom},
        }
    }


def get_extent(image):
    left = float(image.getncattr('geospatial_lon_min'))
    bottom = float(image.getncattr('geospatial_lat_min'))
    right = float(image.getncattr('geospatial_lon_max'))
    top = float(image.getncattr('geospatial_lat_max'))
    return {
        'ul': {'lon': left, 'lat': top},
        'ur': {'lon': right, 'lat': top},
        'll': {'lon': left, 'lat': bottom},
        'lr': {'lon': right, 'lat': bottom},
    }


def get_skeleton(path, prod, bands):
    image = netCDF4.Dataset(path)
    times = image['time']
    sensing_time = str(netCDF4.num2date(times[0], units=times.units, calendar=times.calendar))

    return {
        'id': str(uuid.uuid4()),
        'processing_level': str(image.processing_level),
        'product_type': prod,
        'creation_dt': str(image.date_created),
        'platform': {'code': 'HIMAWARI_8'},
        'instrument': {'name': str(image.instrument)},
        # 'acquisition': {'groundstation': {'code': station}},
        'extent': {
            'coord': get_extent(image),
            'from_dt': sensing_time,
            'to_dt': sensing_time,
            'center_dt': sensing_time
        },
        'format': {'name': 'NETCDF'},
        'grid_spatial': {
            'projection': get_projection(image)
        },
        'image': {
            'bands': bands
        },
        'lineage': {'source_datasets': {}},
    }


def get_ang_dataset(path):
    band_re = re.compile('.*-P1S-ABOM_GEOM_(.*)-PRJ.*_(500|1000|2000)-HIMAWARI8-AHI.nc')
    images = {}
    for image in path.glob('*-P1S-ABOM_GEOM_*-HIMAWARI8-AHI.nc'):
        match = band_re.match(str(image)).groups()
        images['%s_%s' % match] = {
            'path': image.name,
            'layer': 'solar_zenith_angle',
        }
    if not images:
        return None
    return get_skeleton(str(path / images['SOLAR_2000']['path']), 'GEOM_SOLAR', images)


def get_obs_dataset(path):
    band_re = re.compile('.*-P1S-ABOM_OBS_B(.*)-PRJ.*_(500|1000|2000)-HIMAWARI8-AHI.nc')
    images = {}
    for image in path.glob('*-P1S-ABOM_OBS_*-HIMAWARI8-AHI.nc'):
        match = band_re.match(str(image)).groups()
        images['%s_%s' % match] = {
            'path': image.name,
            'layer': 'channel_00' + match[0] + '_scaled_radiance',
        }
    if not images:
        return None
    return get_skeleton(str(path / images['01_2000']['path']), 'OBS', images)


def get_brf_dataset(path):
    band_re = re.compile('.*-P1S-ABOM_BRF_B(.*)-PRJ.*_(500|1000|2000)-HIMAWARI8-AHI.nc')
    images = {}
    for image in path.glob('*-P1S-ABOM_BRF_*-HIMAWARI8-AHI.nc'):
        match = band_re.match(str(image)).groups()
        images['%s_%s' % match] = {
            'path': image.name,
            'layer': 'channel_00' + match[0] + '_brf',
        }
    if not images:
        return None
    return get_skeleton(str(path / images['01_2000']['path']), 'BRF', images)


def prepare_dataset(path):
    brf = get_brf_dataset(path)
    if not brf:
        return []
    ang = get_ang_dataset(path)
    obs = get_obs_dataset(path)
    brf['lineage']['source_datasets'] = {ds['id']: ds for ds in [ang, obs] if ds}
    return [brf]


def make_datasets(datasets):
    for dataset in datasets:
        path = Path(dataset)

        logging.info("Processing %s", path)
        documents = prepare_dataset(path)
        if not documents:
            logging.info("No datasets found in %s", path)
            continue
        yield path, documents


def absolutify_paths(doc, path):
    for band in doc['image']['bands'].values():
        band['path'] = str(path / band['path'])
    return doc


@click.command(help="Prepare Himawari 8 dataset for ingestion into the Data Cube.")
@click.option('--output', help="Write datasets into this file",
              type=click.Path(exists=False, writable=True, dir_okay=False))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
def main(output, datasets):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    if output:
        docs = (absolutify_paths(doc, path) for path, docs in make_datasets(datasets) for doc in docs)
        with open(output, 'w') as stream:
            yaml.dump_all(docs, stream)
    else:
        for path, docs in make_datasets(datasets):
            yaml_path = str(path.joinpath('agdc-metadata.yaml'))
            logging.info("Writing %s dataset(s) into %s", len(docs), yaml_path)
            with open(yaml_path, 'w') as stream:
                yaml.dump_all(docs, stream)


if __name__ == "__main__":
    main()
