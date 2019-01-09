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

_STATIONS = {'023': 'TKSC', '022': 'SGS', '010': 'GNC', '011': 'HOA',
             '012': 'HEOC', '013': 'IKR', '014': 'KIS', '015': 'LGS',
             '016': 'MGR', '017': 'MOR', '032': 'LGN', '019': 'MTI', '030': 'KHC',
             '031': 'MLK', '018': 'MPS', '003': 'BJC', '002': 'ASN', '001': 'AGS',
             '007': 'DKI', '006': 'CUB', '005': 'CHM', '004': 'BKT', '009': 'GLC',
             '008': 'EDC', '029': 'JSA', '028': 'COA', '021': 'PFS', '020': 'PAC'}

_PRODUCTS = {
    'NBART': 'nbart',
    'NBAR': 'nbar',
    'PQ': 'pqa',
    'FC': 'fc'
}


def band_name(path):
    name = path.stem
    position = name.rfind('_')
    if position == -1:
        raise ValueError('Unexpected tif image in eods: %r' % path)
    if re.match(r"[Bb]\d+", name[position + 1:]):
        band_number = name[position + 2:position + 3]
    elif name[position + 1:].startswith('1111111111111100'):
        band_number = 'pqa'
    else:
        band_number = name[position + 1:]
    return band_number


def get_projection(path):
    with rasterio.open(str(path)) as img:
        left, bottom, right, top = img.bounds
        return {
            'spatial_reference': str(getattr(img, 'crs_wkt', None) or img.crs.wkt),
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
    doc = ElementTree.parse(str(path.joinpath('metadata.xml')))
    aos = crazy_parse(doc.findall("./ACQUISITIONINFORMATION/EVENT/AOS")[0].text)
    los = crazy_parse(doc.findall("./ACQUISITIONINFORMATION/EVENT/LOS")[0].text)
    start_time = crazy_parse(doc.findall("./EXEXTENT/TEMPORALEXTENTFROM")[0].text)
    end_time = crazy_parse(doc.findall("./EXEXTENT/TEMPORALEXTENTTO")[0].text)

    images = {band_name(im_path): {
        'path': str(im_path.relative_to(path))
    } for im_path in path.glob('scene01/*.tif')}

    doc = {
        'id': str(uuid.uuid4()),
        'processing_level': fields["level"],
        'product_type': _PRODUCTS[fields["type"]],
        'creation_dt': str(aos),
        'platform': {'code': "LANDSAT_" + fields["vehicle"][2]},
        'instrument': {'name': fields["instrument"]},
        'acquisition': {
            'groundstation': {
                'code': _STATIONS[fields["groundstation"]]
            },
            'aos': str(aos),
            'los': str(los)
        },
        'extent': {
            'from_dt': str(start_time),
            'to_dt': str(end_time),
            'center_dt': str(start_time + (end_time - start_time) // 2)
        },
        'format': {'name': 'GeoTiff'},
        'grid_spatial': {
            'projection': get_projection(path / next(iter(images.values()))['path'])
        },
        'image': {
            'satellite_ref_point_start': {'x': int(fields["path"]), 'y': int(fields["row"])},
            'satellite_ref_point_end': {'x': int(fields["path"]), 'y': int(fields["row"])},
            'bands': images
        },
        'lineage': {'source_datasets': {}}
    }
    populate_coord(doc)
    return doc


def dataset_folder(fields):
    fmt_str = "{vehicle}_{instrument}_{type}_{level}_GA{type}{product}-{groundstation}_{path}_{row}_{date}"
    return fmt_str.format(**fields)


def prepare_datasets(nbar_path, pq_path=None, fc_path=None):
    fields = re.match(
        (
            r"(?P<vehicle>LS[578])"
            r"_(?P<instrument>OLI_TIRS|OLI|TIRS|TM|ETM)"
            r"_(?P<type>NBAR)"
            r"_(?P<level>P54)"
            r"_GA(?P=type)(?P<product>[0-9][0-9])"
            r"-(?P<groundstation>[0-9]{3})"
            r"_(?P<path>[0-9]{3})"
            r"_(?P<row>[0-9]{3})"
            r"_(?P<date>[12][0-9]{7})"
            "$"
        ),
        nbar_path.stem).groupdict()

    nbar = prep_dataset(fields, nbar_path)

    fields.update({'type': 'PQ', 'level': 'P55'})
    pq_path = (pq_path or nbar_path.parent).joinpath(dataset_folder(fields))
    if not pq_path.exists():
        return [(nbar, nbar_path)]

    pq = prep_dataset(fields, pq_path or nbar_path.parent)
    pq['lineage']['source_datasets'] = {
        nbar['id']: nbar
    }

    fields.update({'type': 'FC', 'level': 'P54'})
    fc_path = (fc_path or nbar_path.parent).joinpath(dataset_folder(fields))
    if not fc_path.exists():
        return (nbar, nbar_path), (pq, pq_path)

    fc = prep_dataset(fields, fc_path or nbar_path.parent)
    fc['lineage']['source_datasets'] = {
        nbar['id']: nbar,
        pq['id']: pq
    }

    return (nbar, nbar_path), (pq, pq_path), (fc, fc_path)


@click.command(help="Prepare GA LS dataset for ingestion into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=-1)
def main(datasets):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    if not datasets:
        logging.info('Please provide the path to some datasets.')

    for dataset in datasets:
        path = Path(dataset)

        logging.info("Processing %s", path)
        documents = prepare_datasets(path)
        if documents:
            for dataset, folder in documents:
                yaml_path = str(folder.joinpath('agdc-metadata.yaml'))
                logging.info("Writing %s", yaml_path)
                with open(yaml_path, 'w') as stream:
                    yaml.dump(dataset, stream)
        else:
            logging.info("No datasets discovered. Bye!")


if __name__ == "__main__":
    main()
