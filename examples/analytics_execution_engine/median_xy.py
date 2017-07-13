# ------------------------------------------------------------------------------
# Name:       median_xy.py
# Purpose:    median xy example for Analytics Engine & Execution Engine.
#             pre-integration with NDExpr.
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

# pylint: disable=too-many-locals

# temporary fix to pass Travis CI
# pylint: disable=wrong-import-order, import-error


from pprint import pprint
from datetime import datetime
from datacube.analytics.analytics_engine import AnalyticsEngine
from datacube.execution.execution_engine import ExecutionEngine
from datacube.analytics.utils.analytics_utils import plot

from glue.core import Data, DataCollection
from glue.app.qt.application import GlueApplication


def main():
    a = AnalyticsEngine()
    e = ExecutionEngine()

    # Lake Burley Griffin
    dimensions = {'x':    {'range': (149.07, 149.18)},
                  'y':    {'range': (-35.32, -35.28)},
                  'time': {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT_5', 'nbar'), ['nir'], dimensions, 'get_data')

    median_xy = a.apply_generic_reduction(arrays, ['y', 'x'], 'median(array1)', 'medianXY')

    result = e.execute_plan(a.plan)

    plot(e.cache['medianXY'])


if __name__ == '__main__':
    main()
