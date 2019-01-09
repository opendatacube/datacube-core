# coding=utf-8
"""
Ingest data from the command-line.
"""

import uuid
import logging
from pathlib import Path
import yaml
import click
import rasterio
from datetime import datetime
from osgeo import osr


def get_projection(img):
    left, bottom, right, top = img.bounds
    return {
        'spatial_reference': str(str(getattr(img, 'crs_wkt', None) or img.crs.wkt)),
        'geo_ref_points': {
            'ul': {'x': left, 'y': top},
            'ur': {'x': right, 'y': top},
            'll': {'x': left, 'y': bottom},
            'lr': {'x': right, 'y': bottom},
        }
    }


def get_coords(geo_ref_points, spatial_ref):
    spatial_ref = osr.SpatialReference(spatial_ref)
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}


def populate_coord(doc):
    proj = doc['grid_spatial']['projection']
    doc['extent']['coord'] = get_coords(proj['geo_ref_points'], proj['spatial_reference'])


def prepare_dataset(path):
    documents = []
    creation_dt = datetime.fromtimestamp(path.stat().st_ctime).isoformat()
    for dspath in path.glob('[ew][01][0-9][0-9][sn][0-9][0-9]dem[sh]'):
        dspath_str = str(dspath)
        product_type = 'DEM-' + dspath_str[-1].upper()
        band_name = 'dem' + dspath_str[-1].lower()
        im = rasterio.open(dspath_str)
        doc = {
            'id': str(uuid.uuid4()),
            # 'processing_level': level.replace('Level-', 'L'),
            'product_type': product_type,
            'creation_dt': creation_dt,
            'platform': {'code': 'ENDEAVOUR'},
            'instrument': {'name': 'SRTM'},
            # 'acquisition': {'groundstation': {'code': station}},
            'extent': {
                'from_dt': creation_dt,
                'to_dt': creation_dt,
                'center_dt': creation_dt,
                # 'coord': get_coords(geo_ref_points, spatial_ref),
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
        }
        populate_coord(doc)
        documents.append(doc)
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
