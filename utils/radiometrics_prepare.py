# coding=utf-8
"""
Ingest data from the command-line.

python utils/radiometrics_prepare.py --output radiometrics.yaml /g/data1/rr2/radiometrics/National/*/*nc
"""

import uuid
from dateutil.parser import parse
import yaml
import click
import netCDF4
import os


def prepare_layers(images):
    layerdict = {}
    for i in images:
        image = netCDF4.Dataset(i)
        layerpath = str(image.filepath())
        for targetlayer in image.variables.values():
            if targetlayer.name not in ['crs', 'lat', 'lon']:
                layername = str(targetlayer.name)
                layerdict[layername] = {'path': layerpath, 'layer': layername, }
    return layerdict


def prepare_dataset(image, datasets):
    image = netCDF4.Dataset(image)

    projection = str(image.geospatial_bounds_crs)
    left, right = float(image.geospatial_lon_min), float(image.geospatial_lon_max)
    bottom, top = float(image.geospatial_lat_min), float(image.geospatial_lat_max)

    return {
        'id': str(uuid.uuid4()),
        'processing_level': 'modelled',
        'product_type': 'gamma_ray',
        'creation_dt': parse(image.date_created).isoformat(),
        'platform': {'code': 'aircraft'},
        'instrument': {'name': 'gamma_ray spectrometer'},
        'extent': {
            'coord': {
                'ul': {'lon': left, 'lat': top},
                'ur': {'lon': right, 'lat': top},
                'll': {'lon': left, 'lat': bottom},
                'lr': {'lon': right, 'lat': bottom},
            },
            'from_dt': parse(image.date_created).isoformat(),
            'to_dt': parse(image.date_created).isoformat(),
            'center_dt': parse(image.date_created).isoformat(),
        },
        'format': {'name': 'NETCDF'},
        'grid_spatial': {
            'projection': {
                'spatial_reference': projection,
                'geo_ref_points': {
                    'ul': {'x': left, 'y': top},
                    'ur': {'x': right, 'y': top},
                    'll': {'x': left, 'y': bottom},
                    'lr': {'x': right, 'y': bottom},
                }
            }
        },
        'image': {
            'bands': prepare_layers(datasets)
        },
        'lineage': {'source_datasets': {}},
    }


@click.command(help="Prepare single layer netcdf with common grid spec for ingestion to Data Cube.")
@click.argument('datasets', type=click.Path(exists=True, readable=True), nargs=-1)
@click.option('--output', help="Write datasets into this file", type=click.Path(exists=False, writable=True))
def main(datasets, output):
    with open(output, 'w') as stream:
        yaml.dump((prepare_dataset(datasets[0], datasets)), stream)


if __name__ == "__main__":
    main()
