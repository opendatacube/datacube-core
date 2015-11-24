# ------------------------------------------------------------------------------
# Name:       generic_ndvi.py
# Purpose:    generic ndvi example for Analytics Engine & Execution Engine.
#             pre-integration with NDExpr.
#             Taken from the GDF Trial.
#
# Author:     Peter Wang
#
# Created:    20 November 2015
# Copyright:  2015 Commonwealth Scientific and Industrial Research Organisation
#             (CSIRO)
# License:    This software is open source under the Apache v2.0 License
#             as provided in the accompanying LICENSE file or available from
#             https://github.com/data-cube/agdc-v2/blob/master/LICENSE
#             By continuing, you acknowledge that you have read and you accept
#             and will abide by the terms of the License.
#
# ------------------------------------------------------------------------------

from __future__ import absolute_import
from __future__ import print_function

from datacube import gdf
import pprint

def main():
    # a = Analytics()
    # e = ExecutionEngine()
    #
    # dimensions = {'X': {'range': (147.0, 147.256)},
    #               'Y': {'range': (-37.0, -36.744)}}
    #
    # arrays = a.createArray('LANDSAT_5_TM', ['B40', 'B30'], dimensions, 'get_data')
    # ndvi = a.applyBandMath(arrays, '((array1 - array2) / (array1 + array2))', 'ndvi')
    #
    # e.executePlan(a.plan)
    #
    # r = e.cache['ndvi']
    # print(r)

    #print (gdf.get_descriptors())

    goo = gdf.GDF()
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(goo.get_descriptor())

if __name__ == '__main__':
    main()
