from __future__ import absolute_import
from __future__ import print_function

import pprint

from datacube import gdf


def main():
    data_request = {
        'satellite': 'LANDSAT_5',
        'variables': ('band_30', 'band_40', 'band_pixelquality'),
        'dimensions': {
            'lon': {
                'range': (149, 149.25),
                'array_range': (0, 127),
            },
            'lat': {
                'range': (-35.1, -35),
                'array_range': (0, 127),
            }
        }
    }

    goo = gdf.GDF()
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(goo.get_descriptor(data_request))

    data = goo.get_data(data_request)
    pp.pprint(data)


if __name__ == '__main__':
    main()
