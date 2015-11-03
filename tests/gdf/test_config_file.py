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
Created on 12/03/2015

@author: Alex Ip

Tests for the gdf._ConfigFile.py module.
"""
from __future__ import absolute_import
import unittest
import inspect

import os

from datacube.gdf._config_file import ConfigFile


#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


class TestConfigFile(unittest.TestCase):
    """
    Unit tests for utility functions.
    """

    MODULE = 'gdf._config_file'
    SUITE = 'TestConfigFile'

    def test_ConfigFile(self):
        """
        Test ConfigFile constructor
        """

        # Default config file should be ../gdf/gdf_default.conf
        default_config_file = os.path.join(os.path.dirname(inspect.getfile(ConfigFile)), 'gdf_default.conf')

        config_file_object = ConfigFile(default_config_file)

        assert config_file_object.path == os.path.abspath(default_config_file), 'path property is not set correctly'


#
# Define test suites
#


def test_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestConfigFile
                    ]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite


# Define main function


def main():
    unittest.TextTestRunner(verbosity=2).run(test_suite())


#
# Run unit tests if in __main__
#
if __name__ == '__main__':
    main()
