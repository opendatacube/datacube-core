# ------------------------------------------------------------------------------
# Name:       ndvi_mask_median_reduction.py
# Purpose:    ndvi + mask + median reduction example for ndexpr
#             pre-integration into Analytics Engine & Execution Engine.
#             post-integration with Data Access API.
#
# Author:     Peter Wang
#
# Created:    22 December 2015
# License:    This software is open source under the Apache v2.0 License
#             as provided in the accompanying LICENSE file or available from
#             https://github.com/data-cube/agdc-v2/blob/master/LICENSE
#             By continuing, you acknowledge that you have read and you accept
#             and will abide by the terms of the License.
#
# ------------------------------------------------------------------------------

from __future__ import absolute_import
from __future__ import print_function

from datetime import datetime
from datacube.api import API
from datacube.ndexpr import NDexpr


def main():
    print('Instantiating API and NDexpr')
    g = API()
    nd = NDexpr()

    print('Retrieving data from API')
    # construct data request parameters for band_30 and band_40
    data_request_descriptor = {
        'platform': 'LANDSAT_5',
        'product': 'nbar',
        'variables': ('red', 'nir'),
        'dimensions': {
            'longitude': {
                'range': (149.07, 149.18)
            },
            'latitude': {
                'range': (-35.32, -35.28)
            },
            'time': {
                'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))
            }
        }
    }

    # get data
    d1 = g.get_data(data_request_descriptor)

    # construct data request parameters for PQ
    pq_request_descriptor = {
        'platform': 'LANDSAT_5',
        'product': 'pqa',
        'variables': ('pixelquality'),
        'dimensions': {
            'longitude': {
                'range': (149.07, 149.18)
            },
            'latitude': {
                'range': (-35.32, -35.28)
            },
            'time': {
                'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))
            }
        }
    }

    # get data
    d2 = g.get_data(pq_request_descriptor)

    # The following 3 lines shouldn't be done like this
    # Currently done like this for the sake of the example.
    b30 = d1['arrays']['red']
    b40 = d1['arrays']['nir']
    pq = d2['arrays']['pixelquality']

    print('NDexpr demo begins here')
    # perform ndvi as expressed in this language.
    ndvi = nd.evaluate('((b40 - b30) / (b40 + b30))')
    # perform mask on ndvi as expressed in this language.
    masked_ndvi = nd.evaluate('ndvi{(pq == 32767) | (pq == 16383) | (pq == 2457)}')

    # currently dimensions are integer indices, later will be labels when
    # Data Access API Interface has been finalised.
    reduction_on_dim0 = nd.evaluate('median(masked_ndvi, 0)')
    reduction_on_dim01 = nd.evaluate('median(masked_ndvi, 0, 1)')
    reduction_on_dim012 = nd.evaluate('median(masked_ndvi, 0, 1, 2)')
    print(reduction_on_dim0)
    print(reduction_on_dim01)
    print(reduction_on_dim012)

if __name__ == '__main__':
    main()
