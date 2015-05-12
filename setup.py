#!/usr/bin/env python

#===============================================================================
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
#===============================================================================

from distutils.core import setup

version = 0.0.0'

setup(name='gdf',
      version = version,
      package_dir = {
                     'gdf' : 'gdf'
                     },
      packages = [
                  'gdf'
                  ],
      package_data = {
                      'gdf': ['gdf_default.conf']
                      },
      scripts = [
                 ],
      requires = [
                  'EOtools',
                  'psycopg2',
                  'gdal',
                  'numexpr',
                  'scipy',
                  'pytz'
                  ],
      url = 'https://github.com/GeoscienceAustralia/gdf'
      author = 'Alex Ip',
      maintainer = 'Alex Ip, Geoscience Australia',
      maintainer_email = 'alex.ip@ga.gov.au',
      description = 'Generalsed Data Framework (GDF)',
      long_description = 'Generalised Data Framework (GDF). Developed under HPD program within Geoinformatics and Data Services Section',
      license = 'BSD 3'
     )
