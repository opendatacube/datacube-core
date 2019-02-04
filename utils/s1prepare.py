# coding=utf-8
"""
Ingest data from the command-line.
"""

import logging
import uuid
from xml.etree import ElementTree
import re
from pathlib import Path
import yaml
from dateutil import parser
from datetime import timedelta
import rasterio.warp
import click
from osgeo import osr
import os

_STATIONS = {'023': 'TKSC', '022': 'SGS', '010': 'GNC', '011': 'HOA',
             '012': 'HEOC', '013': 'IKR', '014': 'KIS', '015': 'LGS',
             '016': 'MGR', '017': 'MOR', '032': 'LGN', '019': 'MTI', '030': 'KHC',
             '031': 'MLK', '018': 'MPS', '003': 'BJC', '002': 'ASN', '001': 'AGS',
             '007': 'DKI', '006': 'CUB', '005': 'CHM', '004': 'BKT', '009': 'GLC',
             '008': 'EDC', '029': 'JSA', '028': 'COA', '021': 'PFS', '020': 'PAC'}


def band_name(path):
    name = path.stem
    # position = name.find('_')
    if 'VH' in str(path):
        layername = 'vh_gamma0'
    if 'VV' in str(path):
        layername = 'vv_gamma0'
    # if position == -1:
    #    raise ValueError('Unexpected tif image in eods: %r' % path)
    # if re.match(r"[Bb]\d+", name[position+1:]):
    #    layername = name[position+2:]

    # else:
    #    layername = name[position+1:]
    return layername


def get_projection(path):
    with rasterio.open(str(path)) as img:
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


def crazy_parse(timestr):
    try:
        return parser.parse(timestr)
    except ValueError:
        if not timestr[-2:] == "60":
            raise
        return parser.parse(timestr[:-2] + '00') + timedelta(minutes=1)


def prep_dataset(fields, path):
    # for file in os.listdir(str(path)):
    #    if file.endswith(".xml") and (not file.endswith('aux.xml')):
    #        metafile = file
    # Parse xml ElementTree gives me a headache so using lxml
    # doc = ElementTree.parse(os.path.join(str(path), metafile))
    # TODO root method doesn't work here - need to include xlmns...

    # for global_metadata in doc.findall('{http://espa.cr.usgs.gov/v1.2}global_metadata'):
    #    satellite = (global_metadata.find('{http://espa.cr.usgs.gov/v1.2}satellite')).text
    #    instrument = (global_metadata.find('{http://espa.cr.usgs.gov/v1.2}instrument')).text
    #    acquisition_date = str((global_metadata
    #             .find('{http://espa.cr.usgs.gov/v1.2}acquisition_date')).text).replace("-","")
    #    scene_center_time = (global_metadata.find('{http://espa.cr.usgs.gov/v1.2}scene_center_time')).text[:8]
    #    center_dt = crazy_parse(acquisition_date+"T"+scene_center_time)
    #    aos = crazy_parse(acquisition_date+"T"+scene_center_time)-timedelta(seconds=(24/2))
    #    los = aos + timedelta(seconds=24)
    #    lpgs_metadata_file = (global_metadata.find('{http://espa.cr.usgs.gov/v1.2}lpgs_metadata_file')).text
    #    groundstation = lpgs_metadata_file[16:19]
    #    fields.update({'instrument': instrument, 'satellite': satellite})
    aos = crazy_parse('2016-03-01T23:59:59')
    los = aos
    fields['creation_dt'] = aos
    fields['satellite'] = 'SENTINEL_1A'
    start_time = aos
    end_time = los
    images = {band_name(im_path): {
        'path': str(im_path.relative_to(path))
    } for im_path in path.glob('*.tif')}

    doc = {
        'id': str(uuid.uuid4()),
        'processing_level': "terrain",
        'product_type': "gamma0",
        'creation_dt': aos,
        'platform': {'code': 'SENTINEL_1A'},
        'instrument': {'name': 'SAR'},
        'acquisition': {'groundstation': {'code': '023', 'aos': str(aos), 'los': str(los)}
                        },
        'extent': {
            'from_dt': str(start_time),
            'to_dt': str(end_time),
            'center_dt': str(aos)
        },
        'format': {'name': 'GeoTiff'},
        'grid_spatial': {
            'projection': get_projection(path / next(iter(images.values()))['path'])
        },
        'image': {
            # 'satellite_ref_point_start': {'path': 0, 'row': 0},
            # 'satellite_ref_point_end': {'path': 0, 'row': 0},
            'bands': images
        },
        # TODO include 'lineage': {'source_datasets': {'lpgs_metadata_file': lpgs_metadata_file}}
        'lineage': {'source_datasets': {}}
    }
    populate_coord(doc)
    return doc


def dataset_folder(fields):
    fmt_str = "{vehicle}_{instrument}_{type}_{level}_{type}{product}-{groundstation}_{path}_{row}_{date}"
    return fmt_str.format(**fields)


# INPUT path is parsed for elements - below hardcoded for testing


def prepare_datasets(s1a_path):
    print(s1a_path)
    fields = re.match(
        (
            r"(?P<platform>SENTINEL_1A)"
        ), s1a_path.stem).groupdict()

    # timedelta(days=int(fields["julianday"]))
    # , 'creation_dt': ((crazy_parse(fields["productyear"]+'0101T00:00:00'))+timedelta(days=int(fields["julianday"])))})
    fields.update({'level': 'gamma0', 'type': 'intensity'})
    s1a = prep_dataset(fields, s1a_path)
    return (s1a, s1a_path)


@click.command(help="Prepare SENTINEL_1A SAR dataset for ingestion into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=-1)
def main(datasets):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    for dataset in datasets:
        path = Path(dataset)

        logging.info("Processing %s", path)
        documents = prepare_datasets(path)

        dataset, folder = documents
        yaml_path = str(folder.joinpath('agdc-metadata.yaml'))
        logging.info("Writing %s", yaml_path)
        with open(yaml_path, 'w') as stream:
            yaml.dump(dataset, stream)


if __name__ == "__main__":
    main()
