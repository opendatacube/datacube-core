from __future__ import absolute_import
from __future__ import print_function

import datetime
import pprint
# import matplotlib.pyplot as plt
# import dask
import xray
import xray.plot
# import numpy

from datacube import api


def main():
    data_request = {
        'satellite': 'LANDSAT_8',
        'product': 'EODS_NBAR',
        'variables': ('band_1', 'band_4', 'band_6'),
        'dimensions': {
            'longitude': {
                'range': (148, 150.25),
            },
            'latitude': {
                'range': (-36.1, -35),
            },
            'time': {
                'range': (datetime.datetime(2000, 8, 1), datetime.datetime(2015, 8, 1, 9, 50, 23))
            }
        }
    }

    goo = api.API()
    pp = pprint.PrettyPrinter(indent=4)
    descriptor = goo.get_descriptor(data_request)
    pp.pprint(descriptor)
    #
    # data_request['dimensions']['longitude']['array_range'] = (3000, 5000)
    # data_request['dimensions']['latitude']['array_range'] = (3000, 5000)

    # dask.set_options(get=dask.async.get_sync)
    data = goo.get_data(data_request)
    pp.pprint(data)

    print("Dimension names match: {}".format(descriptor['LANDSAT_8_OLI_TIRS']['dimensions'] == data['dimensions']))
    for d in zip(data['dimensions'], data['element_sizes'], descriptor['LANDSAT_8_OLI_TIRS']['result_shape']):
        print("  Dimension: {}\tSize:\t{}\t{}".format(*d))
    print("Sizes match: {}".format(data['element_sizes'] == descriptor['LANDSAT_8_OLI_TIRS']['result_shape']))

    for (dim, da, de) in zip(data['dimensions'], data['indices'],  descriptor['LANDSAT_8_OLI_TIRS']['result_min']):
        print("  Dimension: {}\tStart:\t{}\t{}".format(dim, da[0], de))
    for (dim, da, de) in zip(data['dimensions'], data['indices'],  descriptor['LANDSAT_8_OLI_TIRS']['result_max']):
        print("  Dimension: {}\tEnd:\t{}\t{}".format(dim, da[-1], de))

    band_1 = data['arrays']['band_1']
    band_4 = data['arrays']['band_4']
    band_6 = data['arrays']['band_6']

    band_1_mean = band_1.mean(axis=0)
    band_4_mean = band_4.mean(axis=0)
    band_6_mean = band_6.mean(axis=0)

    band_1_scaled = (band_1_mean - band_1_mean.min()) / float(band_1_mean.max() - band_1_mean.min())
    band_4_scaled = (band_4_mean - band_4_mean.min()) / float(band_4_mean.max() - band_4_mean.min())
    band_6_scaled = (band_6_mean - band_6_mean.min()) / float(band_6_mean.max() - band_6_mean.min())

    rgb = xray.concat([band_6_scaled, band_4_scaled, band_1_scaled], 'color')

    # plt.imshow(dd)
    # pp.pprint(band_4.mean(axis=0).shape)
    # rgb = xray.concat([band_6.mean(axis=0), band_4.mean(axis=0), band_1.mean(axis=0)], 'color')
    rgb = rgb.transpose('latitude', 'longitude', 'color')
    # rgb = numpy.stack([band_6.mean(axis=0), band_4.mean(axis=0), band_1.mean(axis=0)], axis=-1)
    pp.pprint(rgb)

    #rgb_scaled = (rgb - rgb.min()) / float(rgb.max() - rgb.min())
    #plt.imshow(rgb)
    pp.pprint(rgb)

if __name__ == '__main__':
    main()
