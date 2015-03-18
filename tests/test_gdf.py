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

'''
Created on 12/03/2015

@author: Alex Ip

Tests for the gdf._GDF.py module.
'''


import unittest
import os
from gdf import GDF


#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


class TestGDF(unittest.TestCase):
    """Unit tests for utility functions."""

    def test_GDF(self):
        "Test GDF constructor"
        test_gdf = GDF() # Test default configuration
        
        assert test_gdf.code_root == os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'gdf')), 'Code root is incorrect'
        assert len(test_gdf.config_files) == 1, 'Default config path list should have one entry'
        assert test_gdf.config_files[0] == os.path.abspath(os.path.join(test_gdf.code_root, GDF.DEFAULT_CONFIG_FILE)), 'Default config path is incorrect'
        assert test_gdf.configuration.get('command_line') is not None, 'Command line parameter dict not set'
        assert test_gdf.configuration.get('configurations') is not None, 'Configurations dict not set'
        assert len(test_gdf.configuration['configurations']) > 0, 'Config files must define at least one setup'
        assert len(test_gdf.databases) > 0, 'At least one database must be set up'
#
# Define test suites
#
def test_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestGDF
                    ]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#
if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(test_suite())
