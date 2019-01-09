# coding=utf-8
"""
Ingest data from the command-line.
"""

import uuid
import logging
import yaml
import re
import click
from osgeo import osr

try:
    from urllib.request import urlopen
    from urllib.parse import urlparse, urljoin
except ImportError:
    from urlparse import urlparse, urljoin
    from urllib2 import urlopen

MTL_PAIRS_RE = re.compile(r'(\w+)\s=\s(.*)')


def _parse_value(s):
    s = s.strip('"')
    for parser in [int, float]:
        try:
            return parser(s)
        except ValueError:
            pass
    return s


def _parse_group(lines):
    tree = {}

    for line in lines:
        match = MTL_PAIRS_RE.findall(line.decode('utf-8'))
        if match:
            key, value = match[0]
            if key == 'GROUP':
                tree[value] = _parse_group(lines)
            elif key == 'END_GROUP':
                break
            else:
                tree[key] = _parse_value(value)
    return tree


def get_geo_ref_points(info):
    return {
        'ul': {'x': info['CORNER_UL_PROJECTION_X_PRODUCT'], 'y': info['CORNER_UL_PROJECTION_Y_PRODUCT']},
        'ur': {'x': info['CORNER_UR_PROJECTION_X_PRODUCT'], 'y': info['CORNER_UR_PROJECTION_Y_PRODUCT']},
        'll': {'x': info['CORNER_LL_PROJECTION_X_PRODUCT'], 'y': info['CORNER_LL_PROJECTION_Y_PRODUCT']},
        'lr': {'x': info['CORNER_LR_PROJECTION_X_PRODUCT'], 'y': info['CORNER_LR_PROJECTION_Y_PRODUCT']},
    }


def get_coords(geo_ref_points, spatial_ref):
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}


def get_mtl(path):
    return _parse_group(urlopen(path))['L1_METADATA_FILE']


def prepare_dataset(path):
    info = get_mtl(path)

    level = info['PRODUCT_METADATA']['DATA_TYPE']
    product_type = info['PRODUCT_METADATA']['DATA_TYPE']

    sensing_time = info['PRODUCT_METADATA']['DATE_ACQUIRED'] + ' ' + info['PRODUCT_METADATA']['SCENE_CENTER_TIME']

    cs_code = 32600 + info['PROJECTION_PARAMETERS']['UTM_ZONE']
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(cs_code)

    geo_ref_points = get_geo_ref_points(info['PRODUCT_METADATA'])

    images = [('1', 'coastal_aerosol'),
              ('2', 'blue'),
              ('3', 'green'),
              ('4', 'red'),
              ('5', 'nir'),
              ('6', 'swir1'),
              ('7', 'swir2'),
              ('8', 'panchromatic'),
              ('9', 'cirrus'),
              ('10', 'lwir1'),
              ('11', 'lwir2'),
              ('QUALITY', 'quality')]

    return {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, path)),
        'processing_level': level,
        'product_type': product_type,
        # 'creation_dt': ct_time,
        'label': info['METADATA_FILE_INFO']['LANDSAT_SCENE_ID'],
        'platform': {'code': info['PRODUCT_METADATA']['SPACECRAFT_ID']},
        'instrument': {'name': info['PRODUCT_METADATA']['SENSOR_ID']},
        # 'acquisition': {'groundstation': {'code': station}},
        'extent': {
            'from_dt': sensing_time,
            'to_dt': sensing_time,
            'center_dt': sensing_time,
            'coord': get_coords(geo_ref_points, spatial_ref),
        },
        'format': {'name': info['PRODUCT_METADATA']['OUTPUT_FORMAT']},
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': 'EPSG:%s' % cs_code,
                #     'valid_data': {
                #         'coordinates': tileInfo['tileDataGeometry']['coordinates'],
                #         'type': tileInfo['tileDataGeometry']['type']}
            }
        },
        'image': {
            'bands': {
                image[1]: {
                    'path': info['PRODUCT_METADATA']['FILE_NAME_BAND_' + image[0]],
                    'layer': 1,
                } for image in images
            }
        },
        'L1_METADATA_FILE': info,
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
