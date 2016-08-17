# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

import uuid
import logging
from pathlib import Path
import yaml
import click
from osgeo import osr
import os
import netCDF4

def prepare_dataset(path):
    images = {}
    documents = []
    for image in path.glob('*.nc'):
        fromtime = image.name[3:11]
        totime = image.name[11:19]
        image = netCDF4.Dataset(image)
        times = image['time']
        sensing_time = str(netCDF4.num2date(times[0], units=times.units, calendar=times.calendar))
        projection = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
        left = float('111.975')
        bottom = float('-44.525')
        right = float('156.275')
        top = float('-9.9750')

        documents.append({
            'id': str(uuid.uuid4()),
            'processing_level': 'modelled_rainfall',
            'product_type': str(image.cdm_data_type),
            'creation_dt': str(image.date_created),
            'platform': {'code': 'BoM'},
            'instrument': {'name': 'rain gauge'},
            'extent': {
                'coord': {
                    'ul': {'lon': left, 'lat': top},
                    'ur': {'lon': right, 'lat': top},
                    'll': {'lon': left, 'lat': bottom},
                    'lr': {'lon': right, 'lat': bottom},
                },
                'from_dt': sensing_time,
                'to_dt': sensing_time,
                'center_dt': sensing_time
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
                'bands': {
                    'rainfall': {
                        'path': str(image.filepath()),
                    }
                }
            },
            'lineage': {'source_datasets': {}},
        })
    return documents

@click.command(help="Prepare BoM interpolated rainfall grid dataset for ingestion into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=-1)
@click.argument('output_path',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=1)
def main(datasets, output_path):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    for dataset in datasets:
        path = Path(dataset)

        logging.info("Processing %s", path)
        documents = prepare_dataset(path)
        if documents:
            yaml_path = os.path.join(output_path, 'agdc_metadata.yaml')
            logging.info("Writing %s dataset(s) into %s", len(documents), yaml_path)
            with open(yaml_path, 'w') as stream:
                yaml.dump_all(documents, stream)
        else:
            logging.info("No datasets discovered. Bye!")


if __name__ == "__main__":
    main()
