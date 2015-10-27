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

Tests for the gdf._CommandLineArgs.py module.
"""

import sys
import unittest
from gdf._arguments import CommandLineArgs


#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


class TestCommandLineArgs(unittest.TestCase):
    """
    Unit tests for utility functions.
    """

    MODULE = 'gdf._arguments'
    SUITE = 'TestCommandLineArgs'

    TEST_ARG_DESCRIPTOR = {'test': {'short_flag': '-t',
                                    'long_flag': '--test',
                                    'default': False,
                                    'action': 'store_const',
                                    'const': True,
                                    'help': 'Test mode flag'
                                    },
                           'test_value': {'short_flag': '-v',
                                          'long_flag': '--value',
                                          'default': 'default_test_value',
                                          'action': 'store_const',
                                          'const': None,
                                          'help': 'Test mode flag'
                                          }
                           }

    def test_CommandLineArgs(self):
        """
        Test CommandLineArgs constructor
        """

        # Remember actual arguments in order to avoid side-effects
        original_sys_argv = list(sys.argv)

        # Simulate command line arguments
        sys.argv.append("--config=config_file.conf")
        sys.argv.append("--debug")
        sys.argv.append("--test")
        sys.argv.append("unnkown")

        command_line_args = CommandLineArgs(TestCommandLineArgs.TEST_ARG_DESCRIPTOR)

        assert command_line_args.arguments[
                   'config_files'] == 'config_file.conf', 'Default --config argument not parsed'
        assert command_line_args.arguments['debug'], 'Default --debug argument not parsed'
        assert command_line_args.arguments['test'], 'Custom --test argument not parsed'
        assert command_line_args.arguments[
                   'test_value'] == 'default_test_value', 'Default value for custom test_value argument not set'

        # Remove simulated test arguments
        sys.argv = original_sys_argv


#
# Define test suites
#


def test_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestCommandLineArgs
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
