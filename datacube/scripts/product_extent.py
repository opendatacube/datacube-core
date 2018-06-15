import getopt
import logging
import sys
from datetime import datetime

import click
from yaml import load

from datacube.db_extent import ExtentUpload, parse_time
from datacube.drivers.postgres import PostgresDb
from datacube.index.index import Index
from datacube.ui import click as ui
from datacube.ui.click import cli

_LOG = logging.getLogger('datacube-md-type')


@cli.group(name='product_extent', help='Product Extents Tools')
def product_extents():
    pass


def extent_upload_periodic(product, index, db, extent_upload, to_time, offset_alias):
    """

    :param product:
    :param index:
    :param db:
    :param extent_upload:
    :param to_time:
    :param offset_alias:
    :return:
    """
    # Process product-extents
    dataset_type_ref = index.products.get_by_name(product).id
    bounds = index.products.ranges(product)

    # Extents for yearly durations
    with db.connect() as connection:
        metadata = connection.get_db_extent_meta(dataset_type_ref, offset_alias)
    if metadata:
        end = parse_time(to_time)
        start = metadata['end']
        if start < end:
            extent_upload.store_extent(product_name=product, start=start, end=end,
                                       offset_alias=offset_alias, projection=metadata['crs'])
    else:
        extent_upload.store_extent(product_name=product, start=bounds['time_min'], end=bounds['time_max'],
                                   offset_alias=offset_alias, projection=bounds['crs'])


@product_extents.command('update')
@click.option('--to-time', default=datetime.now(), help='Time to update to. Defaults to now.')
@click.option('--crs', default='EPSG:4326')
@click.argument('product-name')
@ui.pass_config
@ui.pass_index()
def update(index, config, to_time, crs, product_name):
    destination_index = index

    extent_upload = ExtentUpload(hostname=config['db_hostname'], port=config['db_port'],
                                 database=config['db_database'], username=config['db_user'],
                                 destination_index=destination_index)

    # Process product-bounds
    if destination_index.products.ranges(product_name):
        extent_upload.update_bounds(product_name=product_name, to_time=to_time)
    else:
        extent_upload.store_bounds(product_name, projection=crs)

    extent_upload_periodic(product_name, destination_index, index._db, extent_upload, to_time, '1Y')
    extent_upload_periodic(product_name, destination_index, index._db, extent_upload, to_time, '1M')

