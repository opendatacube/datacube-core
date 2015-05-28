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

Tests for the gdf._SQLAlchemyDB.py module.
'''


import unittest
from gdf._sqlalchemy_db import SQLAlchemyDB, StorageType, Dimension, Domain


#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


class TestSQLAlchemyDB(unittest.TestCase):
    """Unit tests for utility functions."""

    MODULE = 'gdf._sqlalchemy_db'
    SUITE = 'TestSQLAlchemyDB'

    # Test DB connection parameters
    TEST_DBREF = 'test'
#    TEST_HOST = '130.56.244.228' 
#    TEST_PORT = 6432
    TEST_HOST = 'localhost' 
    TEST_PORT = 5432
    TEST_DBNAME = 'gdf'
    TEST_USER = 'cube_user'
    TEST_PASSWORD = 'GAcube0'
    TEST_QUERY = 'select 1 as test_field'

    def test_SQLAlchemyDB(self):
        "Test SQLAlchemyDB constructor"
        test_db = SQLAlchemyDB(self.TEST_DBREF, 
                               self.TEST_HOST, 
                               self.TEST_PORT, 
                               self.TEST_DBNAME, 
                               self.TEST_USER, 
                               self.TEST_PASSWORD
                               )
        
        assert len(test_db.storage_types) > 0, 'No NDArrayType objects created'
        


#
# Define test suites
#
def test_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestSQLAlchemyDB
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
