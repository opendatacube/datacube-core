# coding=utf-8
"""
Ingest data from the command-line.

python utils/bom_rainfall_prepare.py --output rainfall.yaml /g/data/rr5/agcd/0_05/rainfall/daily/*/*
"""

import uuid
from dateutil.parser import parse
import yaml
import click
import netCDF4


def prepare_dataset(image):
    image = netCDF4.Dataset(image)
    times = image['time']
    sensing_time = netCDF4.num2date(times[0], units=times.units, calendar=times.calendar).isoformat()

    projection = 'EPSG:4326'
    left, right = 111.975, 156.275
    bottom, top = -44.525, -9.975

    return {
        'id': str(uuid.uuid4()),
        'processing_level': 'modelled',
        'product_type': 'rainfall',
        'creation_dt': parse(image.date_created).isoformat(),
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
                    'layer': 'rain_day',
                }
            }
        },
        'lineage': {'source_datasets': {}},
    }


@click.command(help="Prepare BoM interpolated rainfall grid dataset for ingestion into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
@click.option('--output', help="Write datasets into this file",
              type=click.Path(exists=False, writable=True))
def main(datasets, output):
    with open(output, 'w') as stream:
        yaml.dump_all((prepare_dataset(path) for path in datasets), stream)


if __name__ == "__main__":
    main()
