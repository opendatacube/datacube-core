# coding=utf-8
"""
Ingest data from the command-line.
"""
from __future__ import absolute_import, division

import uuid
import logging
from xml.etree import ElementTree
from pathlib import Path
import yaml
import click
from osgeo import gdal, osr
from dateutil import parser


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


def fill_image_data(doc, granule_path):
    format_ = None
    bands = {}

    gran_file = gdal.Open(str(granule_path))
    quoted = '"' + str(granule_path) + '"'
    for subds in gran_file.GetSubDatasets():
        index = subds[0].find(quoted)
        if not format_:
            ds = gdal.Open(subds[0])
            projection = ds.GetProjection()
            t = ds.GetGeoTransform()
            bounds = t[0], t[3], t[0] + t[1] * ds.RasterXSize, t[3] + t[5] * ds.RasterYSize
            del ds
            format_ = subds[0][:index - 1]
        else:
            assert format_ == subds[0][:index - 1]

        layer = subds[0][index + len(quoted) + 1:]
        bands[layer.split(':')[-1]] = {
            'path': granule_path.name,
            'layer': layer
        }
    del gran_file

    if not format_:
        raise RuntimeError('empty dataset')

    doc['image'] = {'bands': bands}
    doc['format'] = {'name': format_}
    doc['grid_spatial'] = {
        'projection': {
            'geo_ref_points': {
                'ul': {'x': min(bounds[0], bounds[2]), 'y': max(bounds[1], bounds[3])},
                'ur': {'x': max(bounds[0], bounds[2]), 'y': max(bounds[1], bounds[3])},
                'll': {'x': min(bounds[0], bounds[2]), 'y': min(bounds[1], bounds[3])},
                'lr': {'x': max(bounds[0], bounds[2]), 'y': min(bounds[1], bounds[3])},
            },
            'spatial_reference': projection,
        }
    }


def prepare_dataset(path):
    root = ElementTree.parse(str(path)).getroot()

    # level = root.findall('./*/Product_Info/PROCESSING_LEVEL')[0].text
    product_type = root.findall('./GranuleURMetaData/CollectionMetaData/ShortName')[0].text
    station = root.findall('./DataCenterId')[0].text
    ct_time = parser.parse(root.findall('./GranuleURMetaData/InsertTime')[0].text)
    from_dt = parser.parse('%s %s' % (root.findall('./GranuleURMetaData/RangeDateTime/RangeBeginningDate')[0].text,
                                      root.findall('./GranuleURMetaData/RangeDateTime/RangeBeginningTime')[0].text))
    to_dt = parser.parse('%s %s' % (root.findall('./GranuleURMetaData/RangeDateTime/RangeEndingDate')[0].text,
                                    root.findall('./GranuleURMetaData/RangeDateTime/RangeEndingTime')[0].text))

    granules = [granule.text for granule in
                root.findall('./GranuleURMetaData/DataFiles/DataFileContainer/DistributedFileName')]

    documents = []
    for granule in granules:
        doc = {
            'id': str(uuid.uuid4()),
            # 'processing_level': level.replace('Level-', 'L'),
            'product_type': product_type,
            'creation_dt': ct_time.isoformat(),
            'platform': {'code': 'AQUA_TERRA'},
            'instrument': {'name': 'MODIS'},
            'acquisition': {'groundstation': {'code': station}},
            'extent': {
                'from_dt': from_dt.isoformat(),
                'to_dt': to_dt.isoformat(),
                'center_dt': (from_dt + (to_dt - from_dt) // 2).isoformat(),
                # 'coord': get_coords(geo_ref_points, spatial_ref),
            },
            'lineage': {'source_datasets': {}},
        }
        documents.append(doc)
        fill_image_data(doc, path.parent.joinpath(granule))
        populate_coord(doc)
    return documents


@click.command(help="Prepare MODIS datasets for ingestion into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=-1)
def main(datasets):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    for dataset in datasets:
        path = Path(dataset)

        if path.is_dir():
            paths = list(path.glob('*.xml'))
        elif path.suffix != '.xml':
            raise RuntimeError('want xml')
        else:
            paths = [path]

        documents = []
        for path in paths:
            logging.info("Processing %s...", path)
            try:
                documents += prepare_dataset(path)
            except Exception as e:
                logging.info("Failed: %s", e)

        if documents:
            yaml_path = str(path.parent.joinpath('agdc-metadata.yaml'))
            logging.info("Writing %s dataset(s) into %s", len(documents), yaml_path)
            with open(yaml_path, 'w') as stream:
                yaml.dump_all(documents, stream)
        else:
            logging.info("No datasets discovered. Bye!")


if __name__ == "__main__":
    main()
