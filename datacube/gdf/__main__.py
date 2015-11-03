#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ===============================================================================
"""
GDF Class main routine for quick and dirty testing
Created on 12/03/2015

@author: Alex Ip
"""
from __future__ import absolute_import
from __future__ import print_function

from datetime import datetime
from pprint import pprint

from datacube.gdf import GDF


def main():
    # Testing stuff
    g = GDF()
    # g.debug = True
    # pprint(g.storage_config['LS5TM'])
    # pprint(dict(g.storage_config['LS5TM']['dimensions']))
    # pprint(dict(g.storage_config['LS5TM']['measurement_types']))
    # pprint(g.storage_config['LS8OLI'])
    # pprint(dict(g.storage_config['LS8OLI']['dimensions']))
    # pprint(dict(g.storage_config['LS8OLI']['measurement_types']))

    data_request_descriptor = {'storage_type': 'LS5TM',
                               'variables': ('B30',),
                               'dimensions': {'X': {'range': (147.968, 148.032)},
                                              'Y': {'range': (-36.032, -35.968)},
                                              'T': {'range': (1262304000.0, 1325375999.999999)},
                                              # 2010-01-01 00:00:00.0 - 2011-12-31 23:59:59.999999
                                              }
                               }
    t0 = datetime.now()
    print('Starting 256 x 256 single-band cross-boundary descriptor at ', t0)
    d = g.get_descriptor(data_request_descriptor)
    t1 = datetime.now()
    print('Finishing 256 x 256 cross-boundary descriptor at %s (Elapsed time %s)' % (t1, t1 - t0))
    pprint(d)

    t0 = datetime.now()
    print('Starting 256 x 256 single-band cross-boundary selection at ', t0)
    a = g.get_data(data_request_descriptor)
    t1 = datetime.now()
    print('Finishing 256 x 256 cross-boundary selection at %s (Elapsed time %s)' % (t1, t1 - t0))
    pprint(a)


# ===============================================================================
#     t0 = datetime.now()
#     print 'Starting 128 x 128 single-cell, dual-band selection at ', t0
#     data_request_descriptor = {'storage_type': 'LS5TM',
#                                'variables': ('B30', 'B40'),
#                                'dimensions': {'X': {'range': (140.0, 140.032)},
#                                               'Y': {'range': (-36.0, -35.968)}
#                                               }
#                                }
# #                                              'T': {'range': (1262304000.0, 1267401599.999999)}, # 2010-01-01 00:00:00.0 - 2010-02-28 23:59:59.999999
#     d = g.get_data(data_request_descriptor)
#     t1 = datetime.now()
#     print 'Finishing 128 x 128 single-cell, dual-band selection at %s (Elapsed time %s)' % (t1, t1 - t0)
#     pprint(d)
#
#     t0 = datetime.now()
#     print 'Starting 1024 x 1024 single-cell, dual-band selection at ', datetime.now()
#     data_request_descriptor = {'storage_type': 'LS5TM',
#                                'variables': ('B30', 'B40'),
#                                'dimensions': {'X': {'range': (140.0, 140.256)},
#                                               'Y': {'range': (-36.0, -35.744)}
#                                               }
#                                }
# #                                              'T': {'range': (1262304000.0, 1267401599.999999)}, # 2010-01-01 00:00:00.0 - 2010-02-28 23:59:59.999999
#     d = g.get_data(data_request_descriptor)
#     t1 = datetime.now()
#     print 'Finishing 1024 x 1024 single-cell, dual-band selection at %s (Elapsed time %s)' % (t1, t1 - t0)
#     pprint(d)
# ===============================================================================

if __name__ == '__main__':
    main()
