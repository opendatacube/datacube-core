from datacube.db_extent import ExtentUpload, ExtentIndex, parse_time
from datacube.drivers.postgres import PostgresDb
from datacube.index import Index
from yaml import load

# ToDo: This constant must be replaced with an argument
TO_TIME = '06-06-2018'

# Get the Connections to the databases
EXTENT_DB = PostgresDb.create(hostname='agdcdev-db.nci.org.au', database='datacube', port=6432, username='aj9439')
EXTENT_UPLOAD = ExtentUpload(hostname='agdc-db.nci.org.au', database='datacube', port=6432,
                             username='aj9439', extent_index=Index(EXTENT_DB))
DC_INDEX = Index(EXTENT_DB)
EXTENT_IDX = ExtentIndex(datacube_index=DC_INDEX)

with open('db_extent_cfg.yaml', 'r') as stream:
    CFG_DATA = load(stream)
    PRODUCTS = CFG_DATA['product_names']

METADATA = EXTENT_IDX.metadata
for product in PRODUCTS:
    # Process product-bounds
    if EXTENT_IDX.get_bounds(product):
        EXTENT_UPLOAD.update_bounds(product_name=product, to_time=TO_TIME)
    else:
        EXTENT_UPLOAD.store_bounds(product, projection='EPSG:4326')

    # Process product-extents
    dataset_type_ref = DC_INDEX.products.get_by_name(product).id
    bounds = EXTENT_IDX.get_bounds(product)

    # TODO: Needs more checks on TO_TIME
    # Extents for yearly durations
    product_meta = METADATA.get((dataset_type_ref, '1Y'))
    if product_meta:
        end = parse_time(TO_TIME)
        start = product_meta['end']
        EXTENT_UPLOAD.store_extent(product_name=product, start=start, end=end,
                                   offset_alias='1Y', projection=product_meta['crs'])
    else:
        EXTENT_UPLOAD.store_extent(product_name=product, start=bounds['start'], end=bounds['end'],
                                   offset_alias='1Y', projection=bounds['crs'])

    # Extents for monthly durations
    product_meta = METADATA.get((dataset_type_ref, '1M'))
    if product_meta:
        end = parse_time(TO_TIME)
        start = product_meta['end']
        EXTENT_UPLOAD.store_extent(product_name=product, start=start, end=end,
                                   offset_alias='1M', projection=product_meta['crs'])
    else:
        EXTENT_UPLOAD.store_extent(product_name=product, start=bounds['start'], end=bounds['end'],
                                   offset_alias='1M', projection=bounds['crs'])

    # Extents for daily durations
    product_meta = METADATA.get((dataset_type_ref, '1D'))
    if product_meta:
        end = parse_time(TO_TIME)
        start = product_meta['end']
        EXTENT_UPLOAD.store_extent(product_name=product, start=start, end=end,
                                   offset_alias='1D', projection=product_meta['crs'])
    else:
        EXTENT_UPLOAD.store_extent(product_name=product, start=bounds['start'], end=bounds['end'],
                                   offset_alias='1D', projection=bounds['crs'])
