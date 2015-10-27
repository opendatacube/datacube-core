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

Tests for the gdf._database.py module.
"""

import unittest
from gdf._database import Database


#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


class TestDatabase(unittest.TestCase):
    """Unit tests for utility functions."""

    MODULE = 'gdf._database'
    SUITE = 'TestDatabase'

    # Test DB connection parameters
    TEST_DB_REF = 'test_db'

    # TEST_HOST = '130.56.244.228'
    # TEST_PORT = 6432
    # TEST_DBNAME = 'gdf_landsat'
    TEST_HOST = 'localhost'
    TEST_PORT = 5432
    TEST_DBNAME = 'gdf_test_ls'
    TEST_USER = 'cube_user'
    TEST_PASSWORD = 'GAcube0'
    TEST_QUERY = 'select 1 as test_field'

    def test_database(self):
        """
        Test Database constructor
        """
        test_db = Database(self.TEST_DB_REF,
                           self.TEST_HOST,
                           self.TEST_PORT,
                           self.TEST_DBNAME,
                           self.TEST_USER,
                           self.TEST_PASSWORD
                           )

        test_db._default_cursor.execute(self.TEST_QUERY)
        assert test_db._default_cursor.description is not None, 'No rows returned'

    def test_execSQL(self):
        """Test execSQL function"""
        test_db = Database(self.TEST_DB_REF,
                           self.TEST_HOST,
                           self.TEST_PORT,
                           self.TEST_DBNAME,
                           self.TEST_USER,
                           self.TEST_PASSWORD
                           )

        cursor = test_db.exec_sql(self.TEST_QUERY)
        assert cursor.description is not None, 'No rows returned'

    def test_query(self):
        """Test query function"""
        test_db = Database(self.TEST_DB_REF,
                           self.TEST_HOST,
                           self.TEST_PORT,
                           self.TEST_DBNAME,
                           self.TEST_USER,
                           self.TEST_PASSWORD
                           )

        cached_result_set = test_db.submit_query(self.TEST_QUERY)

        assert cached_result_set.record_count == 1, 'Query should return exactly one row'
        assert cached_result_set.field_count == 1, 'Query should return exactly one field'
        assert cached_result_set.field_names[0] == 'test_field', 'Field name should be "test_field"'
        assert list(cached_result_set.record_generator())[0]['test_field'] == 1, 'Field value should be 1'
        assert len(cached_result_set.field_values) == 1, 'field_values dict property should have only one item'

    def test_add_values(self):
        """Test query function"""
        test_db = Database(self.TEST_DB_REF,
                           self.TEST_HOST,
                           self.TEST_PORT,
                           self.TEST_DBNAME,
                           self.TEST_USER,
                           self.TEST_PASSWORD
                           )

        cached_result_set = test_db.submit_query(self.TEST_QUERY)

        field_name = cached_result_set.field_names[0]
        new_field_name = field_name + '_plus_one'
        new_field_values = [field_value + 1 for field_value in cached_result_set.field_values[field_name]]

        cached_result_set.add_values({new_field_name: new_field_values})

        assert cached_result_set.record_count == 1, 'Modified result set should have exactly one row'
        assert cached_result_set.field_count == 2, 'Modified result set should have exactly two fields'
        assert cached_result_set.field_names[0] == 'test_field', 'First field name should be "test_field"'
        assert cached_result_set.field_names[
                   1] == 'test_field_plus_one', 'Second field name should be "test_field_plus_one"'
        first_record_dict = list(cached_result_set.record_generator())[0]
        assert first_record_dict['test_field'] == 1, 'First field value should be 1'
        assert first_record_dict['test_field_plus_one'] == 2, 'Second field value should be 2'
        assert len(cached_result_set.field_values) == 2, 'field_values dict property should have two items'
        assert len(cached_result_set.field_names) == 2, 'field_names list property should have two items'

    def test_calc_values(self):
        """Test generator with calculated fields function"""

        def plus_one(record_dict):
            return record_dict['test_field'] + 1

        def plus_two(record_dict):
            return record_dict['test_field'] + 2

        # Define calculated fields using previously defined functions
        calculated_field_dict = {'plus_one': plus_one,
                                 'plus_two': plus_two
                                 }

        test_db = Database(self.TEST_DB_REF,
                           self.TEST_HOST,
                           self.TEST_PORT,
                           self.TEST_DBNAME,
                           self.TEST_USER,
                           self.TEST_PASSWORD
                           )

        cached_result_set = test_db.submit_query(self.TEST_QUERY)

        first_record_dict = list(cached_result_set.record_generator(calculated_field_dict))[0]
        assert first_record_dict['test_field'] == 1, 'First field value should be 1'
        assert first_record_dict['plus_one'] == 2, 'Second field value should be 2'
        assert first_record_dict['plus_two'] == 3, 'Third field value should be 3'


#
# Define test suites
#
def test_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestDatabase
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
