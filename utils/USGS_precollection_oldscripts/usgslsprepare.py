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
# image boundary imports
import rasterio
from rasterio.errors import RasterioIOError
import rasterio.features
import shapely.affinity
import shapely.geometry
import shapely.ops

_STATIONS = {'023': 'TKSC', '022': 'SGS', '010': 'GNC', '011': 'HOA',
             '012': 'HEOC', '013': 'IKR', '014': 'KIS', '015': 'LGS',
             '016': 'MGR', '017': 'MOR', '032': 'LGN', '019': 'MTI', '030': 'KHC',
             '031': 'MLK', '018': 'MPS', '003': 'BJC', '002': 'ASN', '001': 'AGS',
             '007': 'DKI', '006': 'CUB', '005': 'CHM', '004': 'BKT', '009': 'GLC',
             '008': 'EDC', '029': 'JSA', '028': 'COA', '021': 'PFS', '020': 'PAC'}

BANDS = ['coastal_aerosol', 'blue', 'green', 'red', 'nir', 'swir1', 'swir2']


# IMAGE BOUNDARY CODE


def safe_valid_region(images, mask_value=None):
    try:
        return valid_region(images, mask_value)
    except (OSError, RasterioIOError):
        return None


def valid_region(images, mask_value=None):
    mask = None

    for fname in images:
        # ensure formats match
        with rasterio.open(str(fname), 'r') as ds:
            transform = ds.transform
            img = ds.read(1)

            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                new_mask = img != ds.nodata
            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask

    shapes = rasterio.features.shapes(mask.astype('uint8'), mask=mask)
    shape = shapely.ops.unary_union([shapely.geometry.shape(shape) for shape, val in shapes if val == 1])

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
    output['coordinates'] = _to_lists(output['coordinates'])
    return output


def _to_lists(x):
    """
    Returns lists of lists when given tuples of tuples
    """
    if isinstance(x, tuple):
        return [_to_lists(el) for el in x]

    return x


# END IMAGE BOUNDARY CODE

def band_name(sat, path):
    name = path.stem
    position = name.find('_')

    if position == -1:
        raise ValueError('Unexpected tif image in eods: %r' % path)
    if re.match(r"sr_band\d+", name[position + 1:]):
        band = int(name[position + 8:])
        if sat == 'LANDSAT_8' or band > 6:
            band -= 1
        layername = BANDS[band]
    else:
        layername = name[position + 1:]

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
    images_list = []
    for file in os.listdir(str(path)):
        if file.endswith(".xml") and (not file.endswith('aux.xml')):
            metafile = file
        if file.endswith(".tif") and ("band" in file):
            images_list.append(os.path.join(str(path), file))
    with open(os.path.join(str(path), metafile)) as f:
        xmlstring = f.read()
    xmlstring = re.sub(r'\sxmlns="[^"]+"', '', xmlstring, count=1)
    doc = ElementTree.fromstring(xmlstring)

    satellite = doc.find('.//satellite').text
    instrument = doc.find('.//instrument').text
    acquisition_date = doc.find('.//acquisition_date').text.replace("-", "")
    scene_center_time = doc.find('.//scene_center_time').text[:8]
    center_dt = crazy_parse(acquisition_date + "T" + scene_center_time)
    aos = crazy_parse(acquisition_date + "T" + scene_center_time) - timedelta(seconds=(24 / 2))
    los = aos + timedelta(seconds=24)
    lpgs_metadata_file = doc.find('.//lpgs_metadata_file').text
    groundstation = lpgs_metadata_file[16:19]
    fields.update({'instrument': instrument, 'satellite': satellite})

    start_time = aos
    end_time = los
    images = {band_name(satellite, im_path): {
        'path': str(im_path.relative_to(path))
    } for im_path in path.glob('*.tif')}
    projdict = get_projection(path / next(iter(images.values()))['path'])
    projdict['valid_data'] = safe_valid_region(images_list)
    doc = {
        'id': str(uuid.uuid4()),
        'processing_level': fields["level"],
        'product_type': fields["type"],
        'creation_dt': fields["creation_dt"],
        'platform': {'code': fields["satellite"]},
        'instrument': {'name': fields["instrument"]},
        'acquisition': {
            'groundstation': {
                'code': groundstation,
            },
            'aos': str(aos),
            'los': str(los)
        },
        'extent': {
            'from_dt': str(start_time),
            'to_dt': str(end_time),
            'center_dt': str(center_dt)
        },
        'format': {'name': 'GeoTiff'},
        'grid_spatial': {
            'projection': projdict
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


def prepare_datasets(nbar_path):
    fields = re.match(
        (
            r"(?P<code>LC8|LE7|LT5)"
            r"(?P<path>[0-9]{3})"
            r"(?P<row>[0-9]{3})"
            r"(?P<productyear>[0-9]{4})"
            r"(?P<julianday>[0-9]{3})"

        ), nbar_path.stem).groupdict()

    timedelta(days=int(fields["julianday"]))
    fields.update({'level': 'sr_refl', 'type': 'LEDAPS', 'creation_dt': (
            (crazy_parse(fields["productyear"] + '0101T00:00:00')) + timedelta(days=int(fields["julianday"])))})
    nbar = prep_dataset(fields, nbar_path)
    return (nbar, nbar_path)


@click.command(help="Prepare USGS LS dataset for ingestion into the Data Cube.")
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
