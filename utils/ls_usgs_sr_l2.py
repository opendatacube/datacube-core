# coding=utf-8
"""
Ingest data from the command-line.
"""

import logging
import uuid
from xml.etree import ElementTree
import re
import yaml
import rasterio.warp
import click
from osgeo import osr
import os
from pathlib import Path
# image boundary imports
import rasterio
from rasterio.errors import RasterioIOError
import rasterio.features
import shapely.affinity
import shapely.geometry
import shapely.ops

ls8_images = {
    'sr_band1': 'coastal_aerosol',
    'sr_band2': 'blue',
    'sr_band3': 'green',
    'sr_band4': 'red',
    'sr_band5': 'nir',
    'sr_band6': 'swir1',
    'sr_band7': 'swir2',
    'pixel_qa': 'pixel_qa',
    'radsat_qa': 'radsat_qa',
    'sr_aerosol': 'sr_aerosol',
    'bt_band10': 'lwir1',
    'bt_band11': 'lwir2',
}

ls5_7_images = {
    'sr_band1': 'blue',
    'sr_band2': 'green',
    'sr_band3': 'red',
    'sr_band4': 'nir',
    'sr_band5': 'swir1',
    'sr_band7': 'swir2',
    'bt_band6': 'lwir',
    'pixel_qa': 'pixel_qa',
    'radsat_qa': 'radsat_qa',
    'sr_atmos_opacity': 'sr_atmos_opacity',
    'sr_cloud_qa': 'sr_cloud_qa',
}


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

def satellite_ref(sat):
    """
    To load the band_names for referencing either LANDSAT8 or LANDSAT7 or LANDSAT5 bands
    Landsat7 and Landsat5 have same band names
    """
    lookup = dict(
        LANDSAT_8=(ls8_images, 'LaSRC'),
        LANDSAT_7=(ls5_7_images, 'LEDAPS'),
        LANDSAT_5=(ls5_7_images, 'LEDAPS'),
    )
    r = lookup.get(sat, None)
    if r is None:
        raise ValueError("Landsat Error")
    return r


def get_projection(realpath, path):
    with rasterio.open(os.path.join(str(realpath), str(path))) as img:
        left, bottom, right, top = img.bounds
        spatial_reference = str(str(getattr(img, 'crs_wkt', None) or img.crs.wkt))
        geo_ref_points = {
            'ul': {'x': left, 'y': top},
            'ur': {'x': right, 'y': top},
            'll': {'x': left, 'y': bottom},
            'lr': {'x': right, 'y': bottom},
        }
        return geo_ref_points, spatial_reference


def get_coords(geo_ref_points, spatial_ref):
    spatial_ref = osr.SpatialReference(spatial_ref)
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}


def prep_dataset(path, metadata):

    with open(os.path.join(str(path), metadata)) as f:
        xmlstring = f.read()
    xmlstring = re.sub(r'\sxmlns="[^"]+"', '', xmlstring, count=1)
    doc = ElementTree.fromstring(xmlstring)

    satellite = doc.find('.//satellite').text
    instrument = doc.find('.//instrument').text
    acquisition_date = doc.find('.//acquisition_date').text
    scene_center_time = doc.find('.//scene_center_time').text
    center_dt = acquisition_date + " " + scene_center_time
    level = doc.find('.//product_id').text.split('_')[1]
    start_time = center_dt
    end_time = center_dt
    images, product_type = satellite_ref(satellite)
    image_path = doc.find('.//product_id').text

    image_files = {db_name: '{prefix}_{band}.tif'.format(prefix=image_path,
                                                         band=disk_name)
                   for disk_name, db_name in images.items()}

    # prune to only include files that are present on disk
    image_files = {k: file
                   for k, file in image_files.items()
                   if (Path(path)/file).exists()}

    sample_file = image_files['blue']
    geo_ref_points, spatial_ref = get_projection(path, sample_file)
    doc = {
        'id': str(uuid.uuid4()),
        'processing_level': str(level),
        'product_type': product_type,
        'creation_dt': acquisition_date,
        'platform': {'code': satellite},
        'instrument': {'name': instrument},
        'extent': {
            'from_dt': str(start_time),
            'to_dt': str(end_time),
            'center_dt': str(center_dt),
            'coord': get_coords(geo_ref_points, spatial_ref),
        },
        'format': {'name': 'GeoTiff'},
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': spatial_ref,
            }
        },
        'image': {
            'bands': {
                db_name: {
                    'path': path,
                    'layer': 1,
                } for db_name, path in image_files.items()
            }
        },

        'lineage': {'source_datasets': {}}
    }
    return doc


def prepare_datasets(path, metadata, output):
    doc = prep_dataset(path, metadata)
    if os.path.dirname((os.path.join(path, metadata))) == os.path.dirname(output):
        return doc
    else:
        return absolutify_paths(doc, path)


def absolutify_paths(doc, path):
    for band in doc['image']['bands'].values():
        band['path'] = os.path.join(path, band['path'])
    return doc


@click.command(help="Prepare USGS Landsat Surface Reflectance product to index into datacube")
@click.option('--output', required=False, help="Write datasets into this file",
              type=click.Path(exists=False, writable=True, dir_okay=False))
@click.argument('metadata',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
def main(output, metadata):
    output = os.path.abspath(output)
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    with open(output, 'w') as stream:
        for dataset in metadata:
            path = os.path.abspath(dataset)
            logging.info("Processing %s", path)
            for file in os.listdir(str(path)):
                if file.endswith(".xml") and (not file.endswith('aux.xml')):
                    yaml.dump(prepare_datasets(path, file, output), stream, explicit_start=True)
                    logging.info("Writing %s", output)


if __name__ == "__main__":
    main()
