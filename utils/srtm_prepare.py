# coding=utf-8
"""
Ingest data from the command-line.

python srtm_prepare.py --output Elevation_1secSRTM_DEMs_v1.0_DEM_Mosaic_dem1sv1_0.yaml \
  /g/data/rr1/Elevation/NetCDF/1secSRTM_DEMs_v1.0/DEM/Elevation_1secSRTM_DEMs_v1.0_DEM_Mosaic_dem1sv1_0.nc
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
    projection = image.variables.values()[0].spatial_ref
    geotransform = ((str(image.variables.values()[0].GeoTransform)).split())
    fgeotransform = [float(i) for i in geotransform]
    lon_pixels = int(image.dimensions.values()[0].size)
    lat_pixels = int(image.dimensions.values()[1].size)

    left, right = float(fgeotransform[0]), float(fgeotransform[0] + (lon_pixels * fgeotransform[1]))
    bottom, top = float(fgeotransform[3] + (lat_pixels * fgeotransform[5])), float(fgeotransform[3])

    return {
        'id': str(uuid.uuid4()),
        'processing_level': 'modelled',
        'product_type': 'DEM',
        'creation_dt': parse(image.history[0:24]).isoformat(),
        'platform': {'code': 'Space Shuttle Endeavour'},
        'instrument': {'name': 'SIR'},
        'extent': {
            'coord': {
                'ul': {'lon': left, 'lat': top},
                'ur': {'lon': right, 'lat': top},
                'll': {'lon': left, 'lat': bottom},
                'lr': {'lon': right, 'lat': bottom},
            },
            'from_dt': parse(image.history[0:24]).isoformat(),
            'to_dt': parse(image.history[0:24]).isoformat(),
            'center_dt': parse(image.history[0:24]).isoformat(),
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
