import numpy
import xarray
import datacube

query = {
    'time': ('2013-01-01', '2014-01-01'),
    'lat': (-35.2, -35.4),
    'lon': (149.0, 149.2),
}

products = ['ls7_nbar_albers', 'ls8_nbar_albers']

dc = datacube.Datacube(app='multi-prod-recipe')

# find similarly named measurements
measurements = set(dc.index.products.get_by_name(products[0]).measurements.keys())
for prod in products[1:]:
    measurements.intersection(dc.index.products.get_by_name(products[0]).measurements.keys())

datasets = []
for prod in products:
    ds = dc.load(product=prod, measurements=measurements, **query)
    ds['product'] = ('time', numpy.repeat(prod, ds.time.size))
    datasets.append(ds)

combined = xarray.concat(datasets, dim='time')
combined = combined.sortby('time')  # sort along time dim
