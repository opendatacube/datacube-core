from osgeo import gdal, gdalconst, osr

import os
import yaml

from netcdf_writer import append_to_netcdf, TileSpec


class SimpleObject(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _get_point(gt, px, py):
    x = gt[0] + (px * gt[1]) + (py * gt[2])
    y = gt[3] + (px * gt[4]) + (py * gt[5])
    return x, y


def _get_extent(gt, cols, rows):
    return (
        _get_point(gt, 0, 0),
        _get_point(gt, 0, rows),
        _get_point(gt, cols, 0),
        _get_point(gt, cols, rows),
    )


def extract_region(width, height, geotransform, projection, type_, bands=1):
    driver = gdal.GetDriverByName('MEM')
    img = driver.Create('temp', width, height, 1, eType=type_)
    img.SetGeoTransform(geotransform)
    img.SetProjection(projection)
    return img


def create_tiles(src_ds, dst_size, dst_res, dst_srs=None, src_srs=None, src_tr=None, dst_type=None):
    """
    Takes a gdal dataset, and yield a set of tiles
    """
    src_tr = src_tr or src_ds.GetGeoTransform()
    src_srs = src_srs or osr.SpatialReference(src_ds.GetProjectionRef())
    dst_srs = dst_srs or src_srs
    dst_type = dst_type or src_ds.GetRasterBand(1).DataType

    src_ext = _get_extent(src_ds.GetGeoTransform(), src_ds.RasterXSize, src_ds.RasterYSize)
    transform = osr.CoordinateTransformation(src_srs, dst_srs)
    dst_ext = [transform.TransformPoint(x, y)[:2] for x, y in src_ext]

    min_x = int(min(x // dst_size['x'] for x, _ in dst_ext))
    min_y = int(min(y // dst_size['y'] for _, y in dst_ext))

    max_x = int(max(x // dst_size['x'] for x, _ in dst_ext))
    max_y = int(max(y // dst_size['y'] for _, y in dst_ext))

    for y in xrange(min_y, max_y + 1):
        for x in xrange(min_x, max_x + 1):
            # TODO: check that it intersects with dst_ext
            transform = [x * dst_size['x'], dst_res['x'], 0.0, y * dst_size['y'], 0.0, dst_res['y']]
            print transform
            print x * dst_size['x'], y * dst_size['y'], (x + 1) * dst_size['x'], (y + 1) * dst_size['y']
            width = int(dst_size['x'] / dst_res['x'])
            height = int(dst_size['y'] / dst_res['y'])
            region = extract_region(width, height, transform, dst_srs.ExportToWkt(), dst_type)
            r = gdal.ReprojectImage(src_ds, region)
            assert (r == 0)
            yield region


def make_input_specs(ingest_config, storage_configs, eodataset):
    for storage in ingest_config['storage']:
        if storage['name'] not in storage_configs:
            print('Error: Storage name "%s" is not found Storage Configurations. Skipping' % storage['name'])
            continue
        storage_spec = storage_configs[storage['name']]

        yield SimpleObject(
            storage_spec=storage_spec,
            bands={
                name: SimpleObject(**vals) for name, vals in storage['bands'].items()
            },
            dataset=eodataset
        )


def generate_filename(filename_format, eodataset, tile_spec):
    merged = eodataset.copy()
    merged.update(tile_spec.__dict__)
    return filename_format.format(**merged)


def ingest(input_spec):
    os.chdir(input_spec.storage_spec['base_path'])

    for band_name in input_spec.bands.keys():
        input_filename = input_spec.dataset['image']['bands'][band_name]['path']
        src_ds = gdal.Open(input_filename, gdalconst.GA_ReadOnly)
        print "doing", band_name, input_filename
        for im in create_tiles(src_ds,
                               input_spec.storage_spec['tile_size'],
                               input_spec.storage_spec['resolution'],
                               dst_srs=osr.SpatialReference(input_spec.storage_spec['projection']['spatial_ref'])):

            tile_spec = TileSpec(im)
            # we have tile_spec, input_spec.storage_spec, input_spec.eodataset, input_spec.bands
            # also, im has the SRS we want to use

            out_filename = generate_filename(input_spec.storage_spec['filename_format'], input_spec.dataset, tile_spec)

            print(os.getcwd(), out_filename)
            append_to_netcdf(im, out_filename, input_spec, band_name, input_filename)

            print im


def load_yaml(filename):
    # yaml_path = os.path.join(os.path.dirname(__file__), filename)
    return yaml.load(open(filename).read())


def load_eodataset(dataset_path):
    # dataset_path = "/short/u46/gxr547/GA/NBAR/LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228/"
    with open(dataset_path + "ga-metadata.yaml", "r") as stream:
        dataset_config = yaml.load(stream)

    for name in dataset_config['image']['bands']:
        dataset_config['image']['bands'][name]['path'] = dataset_path + dataset_config['image']['bands'][name]['path']

    return dataset_config


def run_ingest(storage_config, ingest_config, dataset_path):
    ingest_config = load_yaml(ingest_config)
    storage_config = load_yaml(storage_config)
    storage_configs = {storage_config['name']: storage_config}
    eodataset = load_eodataset(dataset_path)
    for input_spec in make_input_specs(ingest_config, storage_configs, eodataset):
        ingest(input_spec)