from datacube.db_extent import ExtentUpload, parse_time
from datacube.drivers.postgres import PostgresDb
from datacube.index import Index
from yaml import load
import sys
import getopt
from datetime import datetime


def extent_upload_periodic(product, index, db, extent_upload, to_time, offset_alias):
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


def main(argv):
    # initialize to_time
    to_time = datetime.now()

    # Get to_time option
    try:
        opts, args = getopt.getopt(argv, "h:", ["to_time="])
    except getopt.GetoptError:
        print('db_extent_update.py to_time=<date-string>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('db_extent_update.py to_time=<date-string>')
            sys.exit()
        elif opt == '--to_time':
            to_time = arg

    # load configs
    with open('db_extent_cfg.yaml', 'r') as stream:
        cfg_data = load(stream)
        products = cfg_data['product_names']
        crs = cfg_data['crs']
        dc = cfg_data['datacube']
        dc_from = cfg_data.get('datacube-from')
        extent_db = PostgresDb.create(hostname=dc['hostname'], database=dc['database'],
                                      port=dc['port'], username=dc['username'])
        dc_index = Index(extent_db)
        if dc_from:
            extent_upload = ExtentUpload(hostname=dc_from['hostname'], port=dc_from['port'],
                                         database=dc_from['database'], username=dc_from['username'],
                                         extent_index=Index(extent_db))
        else:
            extent_upload = ExtentUpload(hostname=dc['hostname'], port=dc['port'],
                                         database=dc['database'], username=dc['username'],
                                         extent_index=Index(extent_db))

    for product_name in products:
        # Process product-bounds
        if dc_index.products.ranges(product_name):
            extent_upload.update_bounds(product_name=product_name, to_time=to_time)
        else:
            extent_upload.store_bounds(product_name, projection=crs)

        extent_upload_periodic(product_name, dc_index, extent_db, extent_upload, to_time, '1Y')
        extent_upload_periodic(product_name, dc_index, extent_db, extent_upload, to_time, '1M')


if __name__ == "__main__":
    main(sys.argv[1:])
