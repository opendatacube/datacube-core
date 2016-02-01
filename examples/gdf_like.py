from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

import datetime
import pprint
# import matplotlib.pyplot as plt
# import dask
import xarray
# import xarray.plot
# import numpy

from datacube import api


def compare_descriptor_with_data(descriptor, data):
    print("Dimension names match: {}".format(list(descriptor['dimensions']) == data['dimensions']))

    for d in zip(data['dimensions'], data['size'], descriptor['result_shape']):
        print("  Dimension: {}\tSize:\t{}\t{}".format(*d))
    print("Sizes match: {}".format(data['size'] == descriptor['result_shape']))

    for dim, de in zip(descriptor['dimensions'], descriptor['result_min']):
        print("  Dimension: {}\tStart:\t{}\t{}".format(dim, data['indices'][dim][0], de))
    for dim, de in zip(descriptor['dimensions'], descriptor['result_max']):
        print("  Dimension: {}\tEnd:\t{}\t{}".format(dim, data['indices'][dim][-1], de))

def create_rgb(data):
    band_1 = data['arrays']['band_1']
    band_4 = data['arrays']['band_4']
    band_6 = data['arrays']['band_6']

    band_1_mean = band_1.mean(axis=0)
    band_4_mean = band_4.mean(axis=0)
    band_6_mean = band_6.mean(axis=0)

    band_1_scaled = (band_1_mean - band_1_mean.min()) / float(band_1_mean.max() - band_1_mean.min())
    band_4_scaled = (band_4_mean - band_4_mean.min()) / float(band_4_mean.max() - band_4_mean.min())
    band_6_scaled = (band_6_mean - band_6_mean.min()) / float(band_6_mean.max() - band_6_mean.min())

    rgb = xarray.concat([band_6_scaled, band_4_scaled, band_1_scaled], 'color')

    # plt.imshow(dd)
    # pp.pprint(band_4.mean(axis=0).shape)
    # rgb = xarray.concat([band_6.mean(axis=0), band_4.mean(axis=0), band_1.mean(axis=0)], 'color')
    rgb = rgb.transpose('latitude', 'longitude', 'color')
    # rgb = numpy.stack([band_6.mean(axis=0), band_4.mean(axis=0), band_1.mean(axis=0)], axis=-1)

    # rgb_scaled = (rgb - rgb.min()) / float(rgb.max() - rgb.min())
    # plt.imshow(rgb)
    return rgb


def main():
    data_request = {
        # 'platform': 'LANDSAT_8',
        # 'product': 'EODS NBAR',
        'variables': ('band_10', 'band_40', 'band_60'),
        'dimensions': {
            'longitude': {
                'range': (148, 149),
            },
            'latitude': {
                'range': (-36.1, -35.1),
            },
            # 'time': {
            #     'range': (datetime.datetime(2015, 2, 19),
            #               datetime.datetime(2015, 2, 22, 9, 50, 23))
            # }
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
    data = goo.get_data(data_request, descriptor['ls5_nbar']['storage_units'])
    pp.pprint(data)

    compare_descriptor_with_data(descriptor['ls5_nbar'], data)

    print('Mean value of all cells is: {}'.format(float(data['arrays']['band_10'].mean())))
    # rgb = create_rgb(data)
    # pp.pprint(rgb)


if __name__ == '__main__':
    main()
