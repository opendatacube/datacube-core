# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import, division

import uuid
import logging
from pathlib import Path
import yaml
import click
import rasterio
from datetime import datetime


def get_projection(img):
    left, bottom, right, top = img.bounds
    return {
        'spatial_reference': str(img.crs_wkt),
        'geo_ref_points': {
            'ul': {'x': left, 'y': top},
            'ur': {'x': right, 'y': top},
            'll': {'x': left, 'y': bottom},
            'lr': {'x': right, 'y': bottom},
            }
    }


def prepare_dataset(path):
    documents = []
    for dspath in path.glob('[ew][01][0-9][0-9][sn][0-9][0-9]dem[sh]'):
        dspath_str = str(dspath)
        product_type = 'DEM-' + dspath_str[-1].upper()
        band_name = 'dem' + dspath_str[-1].lower()
        creation_dt = datetime.fromtimestamp(dspath.stat().st_ctime).isoformat()
        im = rasterio.open(dspath_str)
        documents.append({
            'id': str(uuid.uuid4()),
            #'processing_level': level.replace('Level-', 'L'),
            'product_type': product_type,
            'creation_dt': creation_dt,
            'platform': {'code': 'ENDEAVOUR'},
            'instrument': {'name': 'SRTM'},
            #'acquisition': {'groundstation': {'code': station}},
            'extent': {
                'from_dt': creation_dt,
                'to_dt': creation_dt,
                'center_dt': creation_dt,
                #'coord': get_coords(geo_ref_points, spatial_ref),
            },
            'format': {'name': str(im.driver)},
            'grid_spatial': {
                'projection': get_projection(im),
            },
            'image': {
                'bands': {
                        band_name: {
                            'path': dspath_str
                        }
                    }
            },
            # TODO: provenance chain is DSM -> DEM -> DEM-S -> DEM-H
            'lineage': {'source_datasets': {}},
        })
    return documents

@click.command(help="Prepare DEM-S datasets for ingestion into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=-1)
def main(datasets):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    for dataset in datasets:
        path = Path(dataset)

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
