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
import click
from osgeo import osr


def get_geo_ref_points(root):
    nrows = int(root.findall('./*/Tile_Geocoding/Size[@resolution="10"]/NROWS')[0].text)
    ncols = int(root.findall('./*/Tile_Geocoding/Size[@resolution="10"]/NCOLS')[0].text)

    ulx = int(root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/ULX')[0].text)
    uly = int(root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/ULY')[0].text)

    xdim = int(root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/XDIM')[0].text)
    ydim = int(root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/YDIM')[0].text)

    return {
        'ul': {'x': ulx, 'y': uly},
        'ur': {'x': ulx + ncols * abs(xdim), 'y': uly},
        'll': {'x': ulx, 'y': uly - nrows * abs(ydim)},
        'lr': {'x': ulx + ncols * abs(xdim), 'y': uly - nrows * abs(ydim)},
    }


def get_coords(geo_ref_points, spatial_ref):
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}
    return {key: transform(p) for key, p in geo_ref_points.items()}


def prepare_dataset(path):
    root = ElementTree.parse(str(path)).getroot()

    level = root.findall('./*/Product_Info/PROCESSING_LEVEL')[0].text
    product_type = root.findall('./*/Product_Info/PRODUCT_TYPE')[0].text
    ct_time = root.findall('./*/Product_Info/GENERATION_TIME')[0].text

    granules = {granule.get('granuleIdentifier'): [imid.text for imid in granule.findall('IMAGE_ID')] for granule in
                root.findall('./*/Product_Info/Product_Organisation/Granule_List/Granules')}

    documents = []
    for granule_id, images in granules.items():
        gran_path = str(path.parent.joinpath('GRANULE', granule_id, granule_id[:-7].replace('MSI', 'MTD')+'.xml'))
        root = ElementTree.parse(gran_path).getroot()
        sensing_time = root.findall('./*/SENSING_TIME')[0].text

        station = root.findall('./*/Archiving_Info/ARCHIVING_CENTRE')[0].text

        cs_code = root.findall('./*/Tile_Geocoding/HORIZONTAL_CS_CODE')[0].text
        spatial_ref = osr.SpatialReference()
        spatial_ref.SetFromUserInput(cs_code)

        geo_ref_points = get_geo_ref_points(root)

        documents.append({
            'id': str(uuid.uuid4()),
            'processing_level': level.replace('Level-', 'L'),
            'product_type': product_type,
            'creation_dt': ct_time,
            'platform': {'code': 'SENTINEL_2A'},
            'instrument': {'name': 'MSI'},
            'acquisition': {'groundstation': {'code': station}},
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
                    'spatial_reference': spatial_ref.ExportToWkt(),
                }
            },
            'image': {
                'bands': {
                    image[-2:]: {
                        'path': str(Path('GRANULE', granule_id, 'IMG_DATA', image + '.jp2')),
                        'layer': 1,
                    } for image in images
                }
            },
            'lineage': {'source_datasets': {}},
        })
    return documents


@click.command(help="Prepare Sentinel 2 dataset for ingestion into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=-1)
def main(datasets):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    for dataset in datasets:
        path = Path(dataset)

        if path.is_dir():
            path = Path(path.joinpath(path.stem.replace('PRD_MSIL1C', 'MTD_SAFL1C')+'.xml'))
        if path.suffix != '.xml':
            raise RuntimeError('want xml')

        logging.info("Processing %s", path)
        documents = prepare_dataset(path)
        if documents:
            yaml_path = str(path.parent.joinpath('agdc-metadata.yaml'))
            logging.info("Writing %s dataset(s) into %s", len(documents), yaml_path)
            with open(yaml_path, 'w') as stream:
                yaml.dump_all(documents, stream)
        else:
            logging.info("No datasets discovered. Bye!")


if __name__ == "__main__":
    main()
