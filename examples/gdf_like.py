from __future__ import absolute_import
from __future__ import print_function

import matplotlib.pyplot as plt
import pprint

from datacube import api
import dask
import xray, xray.plot


def main():
    data_request = {
        'satellite': 'LANDSAT_8',
        'variables': ('band_1', 'band_4', 'band_7', 'band_pixelquality'),
        'dimensions': {
            'lon': {
                'range': (148, 150.25),
                'array_range': (0, 127),
            },
            'lat': {
                'range': (-36.1, -35),
                'array_range': (0, 127),
            }
        }
    }

    goo = api.API()
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(goo.get_descriptor(data_request))

    #dask.set_options(get=dask.async.get_sync)
    data = goo.get_data(data_request)
    pp.pprint(data)
    band_1 = data['arrays']['band_1']
    band_4 = data['arrays']['band_4']
    band_7 = data['arrays']['band_7']
    band_pq = data['arrays']['band_pixelquality']
    d = band_1[0,3500:4500,:] - band_1[1,3500:4500,:]
    #dd = band_1[0,:,:] ** 2

    #plt.imshow(dd)
    pp.pprint(band_4.mean(axis=0).shape)
    rgb = xray.concat([band_4.mean(axis=0), band_1.mean(axis=0), band_7.mean(axis=0)], dim='rgb')
    pp.pprint(rgb)
    xray.plot.imshow(band_7.mean(axis=0))
    pp.pprint(d)

if __name__ == '__main__':
    main()
