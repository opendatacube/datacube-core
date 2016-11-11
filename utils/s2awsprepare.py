# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

import uuid
import logging
from xml.etree import ElementTree
from pathlib import Path
import yaml
import json
import click
from osgeo import osr
import os
# image boundary imports
import rasterio
from rasterio.errors import RasterioIOError
import rasterio.features
import shapely.affinity
import shapely.geometry
import shapely.ops

try:
  from urllib.request import urlopen
  from urllib.parse import urlparse, urljoin
except ImportError:
  from urlparse import urlparse, urljoin
  from urllib2 import urlopen

###IMAGE BOUNDARY CODE

def safe_valid_region(images, mask_value=None):
    try:
        return valid_region(images, mask_value)
    except (OSError, RasterioIOError):
        return None


def valid_region(images, mask_value=None):
    mask = None
    for fname in images:
        ## ensure formats match
        with rasterio.open(str(fname), 'r') as ds:
            transform = ds.affine
            img = ds.read(1)

            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                #new_mask = img != ds.nodata
                new_mask = img != 0
            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask

    shapes = rasterio.features.shapes(mask.astype('uint8'), mask=mask)
    shape = shapely.ops.unary_union([shapely.geometry.shape(shape) for shape, val in shapes if val == 1])
    type(shapes)
    # convex hull
    geom = shape.convex_hull

    # buffer by 1 pixel
    geom = geom.buffer(1, join_style=3, cap_style=3)

    # simplify with 1 pixel radius
    geom = geom.simplify(1)

    # intersect with image bounding box
    geom = geom.intersection(shapely.geometry.box(0, 0, mask.shape[1], mask.shape[0]))

    # transform from pixel space into CRS space
    geom = shapely.affinity.affine_transform(geom, (transform.a, transform.b, transform.d,
                                                    transform.e, transform.xoff, transform.yoff))

    output = shapely.geometry.mapping(geom)
    
    return geom
    #output['coordinates'] = _to_lists(output['coordinates'])
    #return output


def _to_lists(x):
    """
    Returns lists of lists when given tuples of tuples
    """
    if isinstance(x, tuple):
        return [_to_lists(el) for el in x]

    return x

def get_geo_ref_points(tileInfo):
    coords = tileInfo['tileGeometry']['coordinates'][0]

    left = min(coord[0] for coord in coords)
    right = max(coord[0] for coord in coords)
    upper = min(coord[1] for coord in coords)
    lower = max(coord[1] for coord in coords)

    return {
        'ul': {'x': left, 'y': upper},
        'ur': {'x': right, 'y': upper},
        'll': {'x': left, 'y': lower},
        'lr': {'x': right, 'y': lower},
    }


def get_coords(geo_ref_points, spatial_ref):
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}
    return {key: transform(p) for key, p in geo_ref_points.items()}


def get_json(path):
    with urlopen(path) as f:
        return json.loads(f.read().decode('utf-8'))


def prepare_dataset(path):
    tileInfo = get_json(urljoin(path, 'tileInfo.json'))
    # productInfo = urlopen(urljoin(path, 'productInfo.json')).json

    level = tileInfo['datastrip']['id'].split('_')[3]
    product_type = 'S2' + tileInfo['datastrip']['id'].split('_')[2] + level[1:]

    sensing_time = tileInfo['timestamp']

    cs_code = (32600 if tileInfo['latitudeBand'] == "S" else 32700) + tileInfo['utmZone']
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(cs_code)

    geo_ref_points = get_geo_ref_points(tileInfo)

    images = ['B02', 'B03', 'B04', 'B08'] + ['B05', 'B06', 'B07', 'B11', 'B12', 'B8A'] + ['B01', 'B09', 'B10']

    return {
        'id': str(uuid.uuid4()),
        'processing_level': level.replace('Level-', 'L'),
        'product_type': product_type,
        #'creation_dt': ct_time,
        'platform': {'code': 'SENTINEL_2A'},
        'instrument': {'name': 'MSI'},
        # 'acquisition': {'groundstation': {'code': station}},
        'extent': {
            'from_dt': sensing_time,
            'to_dt': sensing_time,
            'center_dt': sensing_time,
            'coord': get_coords(geo_ref_points, spatial_ref),
        },
        'format': {'name': 'JPEG2000'},
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': 'EPSG:%s' % cs_code,
                'valid_data': {
                        'coordinates': tileInfo['tileDataGeometry']['coordinates'],
                        'type': tileInfo['tileDataGeometry']['type']}
            }
        },
        'image': {
            'bands': {
                image[-2:]: {
                    'path': image + '.jp2',
                    'layer': 1,
                } for image in images
            }
        },

        'lineage': {'source_datasets': {}},
    }


def absolutify_paths(doc, path):
    for band in doc['image']['bands'].values():
        band['path'] = urljoin(path, band['path'])
    return doc


@click.command(help="Prepare Sentinel 2 dataset for ingestion into the Data Cube.")
@click.option('--output', help="Write datasets into this file",
              type=click.Path(exists=False, writable=True, dir_okay=False))
@click.argument('datasets',
                nargs=-1)
def main(output, datasets):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    if output:
        docs = (absolutify_paths(prepare_dataset(path), path) for path in datasets)
        with open(output, 'w') as stream:
            yaml.dump_all(docs, stream)
    else:
        raise RuntimeError('must specify --output')


if __name__ == "__main__":
    main()
