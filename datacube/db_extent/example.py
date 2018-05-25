import geopandas as gpd
from datacube.index import Index
from datacube.drivers.postgres import PostgresDb
from datacube.db_extent import ExtentIndex
from geopandas import GeoDataFrame
from shapely.geometry import shape, Polygon, mapping
import matplotlib.pyplot as plt
import datetime
import json


def bounds_to_poly(bounds):
    return Polygon([(bounds['left'], bounds['top']), (bounds['left'], bounds['bottom']),
                    (bounds['right'], bounds['bottom']), (bounds['right'], bounds['top']),
                    (bounds['left'], bounds['top'])])


world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
aus = world[world.name == 'Australia']
base = aus.plot()

# Get the Connections to the databases
extent_db = PostgresDb.create(hostname='agdcdev-db.nci.org.au', database='datacube', port=6432, username='aj9439')
extent_idx = ExtentIndex(hostname='agdc-db.nci.org.au', database='datacube', port=6432,
                         username='aj9439', extent_index=Index(extent_db))

# Get extent of a month
dataset_type_ref = extent_idx.get_dataset_type_ref('ls8_nbar_albers')
start = datetime.datetime(year=2017, month=1, day=1)
extent = extent_idx.get_extent_direct(start=start, offset_alias='1M', dataset_type_ref=dataset_type_ref)
if extent:
    # Plot extents
    ft1 = {'type': 'Feature',
           'geometry': shape(extent)}

    gs1 = GeoDataFrame(ft1)
    gs_base1 = gs1.plot(ax=base, color='red', alpha=0.4, edgecolor='green')

    # Plot bounds
    bounds_data = extent_idx.get_bounds('ls8_nbar_albers')
    bounds = json.loads(bounds_data['bounds'])
    if bounds:
        poly_bounds = shape(mapping(bounds_to_poly(bounds)))
        ft2 = {'type': 'Feature',
               'geometry': [poly_bounds]}
        gs2 = GeoDataFrame(ft2)
        gs_base2 = gs2.plot(ax=gs_base1, color='red', alpha=0.4, edgecolor='green')

# Show the figure
plt.show(block=True)
