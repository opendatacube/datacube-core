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
GDF Class
Created on 12/03/2015

@author: Alex Ip
"""
import os
import sys
import threading
import traceback
from datetime import datetime
import collections
import logging
import cPickle
import itertools
from math import floor
from distutils.util import strtobool

import numpy as np

from _database import Database
from _arguments import CommandLineArgs
from _config_file import ConfigFile
from _gdfnetcdf import GDFNetCDF
from _gdfutils import dt2secs, secs2dt, dt2days, directory_writable, log_multiline

# Set handler for root logger to standard output
console_handler = logging.StreamHandler(sys.stdout)
# console_handler.setLevel(logging.INFO)
console_handler.setLevel(logging.DEBUG)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)
logging.root.addHandler(console_handler)

logger = logging.getLogger(__name__)

thread_exception = None


def _do_db_query(databases, args):
    """
    Generic function to execute a function across multiple databases, each function in its own thread
    Returns a dict which must be updated by db_function in a thread-safe manner

    Parameters:
        databases: dict of database objects keyed by db_ref
        args: list containing db_function to be multi-threaded and its arguments.
            NB: Last two arguments of db_function must be database and result_dict
    """

    def check_thread_exception():
        """
        Check for exception raised by previous thread and raise it if found.
        Note that any other threads already underway will be allowed to finish normally.
        """
        global thread_exception
        logger.debug('thread_exception: %s', thread_exception)
        # Check for exception raised by previous thread and raise it if found
        if thread_exception:
            logger.error('Thread error: ' + thread_exception.message)
            raise thread_exception  # Raise the exception in the main thread

    def thread_execute(db_function, *args, **kwargs):
        """
        Helper function to capture exception within the thread and set a global
        variable to be checked in the main thread
        N.B: THIS FUNCTION RUNS WITHIN THE SPAWNED THREAD
        """
        global thread_exception
        try:
            db_function(*args, **kwargs)
        except Exception as e:
            thread_exception = e
            log_multiline(logger.error, traceback.format_exc(), 'Error in thread: ' + e.message, '\t')
            raise thread_exception  # Re-raise the exception within the thread
        finally:
            logger.debug('Thread finished')

    result_dict = {}  # Nested dict to contain query results - must be updated in a thread-safe manner

    thread_list = []
    for db_ref in sorted(databases.keys()):
        check_thread_exception()

        database = databases[db_ref]
        process_thread = threading.Thread(target=thread_execute,
                                          args=args + [database, result_dict],
                                          name=db_ref
                                          )
        thread_list.append(process_thread)
        process_thread.setDaemon(False)
        process_thread.start()
        logger.debug('Started thread %s', db_ref)

    # Wait for all threads to finish
    for process_thread in thread_list:
        check_thread_exception()
        process_thread.join()

    check_thread_exception()
    logger.debug('All threads finished')

    log_multiline(logger.debug, result_dict, 'result_dict', '\t')
    return result_dict


def _get_command_line_params(arg_descriptors=None):
    """
    Function to return a dict of command line parameters

    Parameters:
        arg_descriptors: dict keyed by dest variable name containing sub-dicts as follows:
            'short_flag': '-d',
            'long_flag': '--debug',
            'default': <Boolean>,
            'action': 'store_const',
            'const': <Boolean>,
            'help': <help string>

    """
    if not arg_descriptors:
        arg_descriptors = {}
    command_line_args_object = CommandLineArgs(arg_descriptors)

    return command_line_args_object.arguments


def null_grouping(record_dict):
    """
    Function which takes a record_dict containing all values from a query in the get_db_slices function
    and returns the slice_index_value unmodified
    """
    return record_dict['slice_index_value']


def solar_date(record_dict):
    """
    Function which takes a record_dict containing all values from a query in the get_db_slices function
    and returns the solar date of the observation
    """
    # Assumes slice_index_value is time in seconds since epoch and x values are in degrees
    # TODO: Make more general (if possible)
    # Note: Solar time offset = average X ordinate in degrees converted to solar time offset in seconds
    return datetime.fromtimestamp(
        record_dict['slice_index_value'] + (record_dict['x_min'] + record_dict['x_max']) * 120).date()


def solar_days_since_epoch(record_dict):
    """
    Function which takes a record_dict containing all values from a query in the get_db_slices function
    and returns the number of days since 1/1/1970
    """
    # TODO: Make more general (if possible)
    return dt2days(solar_date(record_dict))


def solar_year_month(record_dict):
    """
    Function which takes a record_dict containing all values from a query in the get_db_slices function
    and returns a (year, month) tuple from the solar date of the observation
    """
    # Assumes slice_index_value is time in seconds since epoch and x values are in degrees
    # TODO: Make more general (if possible)
    # Note: Solar time offset = average X ordinate in degrees converted to solar time offset in seconds
    sdate = solar_date(record_dict)
    return (sdate.year,
            sdate.month)


def solar_year(record_dict):
    """
    Function which takes a record_dict containing all values from a query in the get_db_slices function
    and returns the solar year of the observation
    """
    # Assumes slice_index_value is time in seconds since epoch and x values are in degrees
    # TODO: Make more general (if possible)
    # Note: Solar time offset = average X ordinate in degrees converted to solar time offset in seconds
    sdate = solar_date(record_dict)
    return sdate.year


def solar_month(record_dict):
    """
    Function which takes a record_dict containing all values from a query in the get_db_slices function
    and returns the solar month of the observation
    """
    # Assumes slice_index_value is time in seconds since epoch and x values are in degrees
    # TODO: Make more general (if possible)
    # Note: Solar time offset = average X ordinate in degrees converted to solar time offset in seconds
    sdate = solar_date(record_dict)
    return sdate.month


class GDF(object):
    """
    Class definition for GDF (General Data Framework).
    Manages configuration and database connections.
    """
    DEFAULT_CONFIG_FILE = 'gdf_default.conf'  # N.B: Assumed to reside in code root directory

    ARG_DESCRIPTORS = {'refresh': {'short_flag': '-r',
                                   'long_flag': '--refresh',
                                   'default': False,
                                   'action': 'store_const',
                                   'const': True,
                                   'help': 'Flag to force refreshing of cached config'
                                   },
                       'cache_dir': {'short_flag': '-c',
                                     'long_flag': '--cache_dir',
                                     'default': None,
                                     'action': 'store',
                                     'const': None,
                                     'help': 'Cache directory for GDF operation'
                                     },
                       }
    MAX_UNITS_IN_MEMORY = 1000  # TODO: Do something better than this
    DECIMAL_PLACES = 6

    def _cache_object(self, cached_object, cache_filename):
        """
        Function to write an object to a cached pickle file
        """
        cache_file = open(os.path.join(self.cache_dir, cache_filename), 'wb')
        cPickle.dump(cached_object, cache_file, -1)
        cache_file.close()

    def _get_cached_object(self, cache_filename):
        """
        Function to retrieve an object from a cached pickle file
        Will raise a general exception if refresh is forced
        """
        if self.refresh:
            raise Exception('Refresh Forced')
        cache_file = open(os.path.join(self.cache_dir, cache_filename), 'r')
        cached_object = cPickle.load(cache_file)
        cache_file.close()
        return cached_object

    def _get_config(self, config_files_string=None):
        """
        Function to return a nested dict of config file entries
        Parameter:
            config_files_string - comma separated list of GDF config files
        Returns: dict {<db_ref>: {<param_name>: <param_value>,... },... }
        """
        config_dict = collections.OrderedDict()  # Need to preserve order of config files

        # Use default config file if none provided
        config_files_string = config_files_string or os.path.join(self._code_root, GDF.DEFAULT_CONFIG_FILE)

        # Set list of absolute config file paths from comma-delimited list
        self._config_files = [os.path.abspath(config_file) for config_file in config_files_string.split(',')]
        log_multiline(logger.debug, self._config_files, 'self._config_files', '\t')

        for config_file in self._config_files:
            config_file_object = ConfigFile(config_file)

            # Merge all configuration sections from individual config files to config dict, ignoring duplicate db_refs
            for db_ref in config_file_object.configuration.keys():
                if db_ref in config_dict.keys():
                    logger.warning('Duplicate db_ref "%s" in config file %s ignored' % (db_ref, config_file))
                else:
                    config_dict[db_ref] = config_file_object.configuration[db_ref]

        # Set variables from global config and remove global config from result dict
        for key, value in config_dict['gdf'].items():
            self.__setattr__(key, value)
            logger.debug('self.%s = %s', key, value)
        del config_dict['gdf']

        log_multiline(logger.debug, config_dict, 'config_dict', '\t')
        return config_dict

    def _get_dbs(self):
        """
        Function to return an ordered dict of database objects keyed by db_ref
        """
        database_dict = collections.OrderedDict()

        # Create a database connection for every valid configuration
        for db_ref in self._configuration.keys():
            db_dict = self._configuration[db_ref]
            try:
                host = db_dict['host']
                port = db_dict['port']
                dbname = db_dict['dbname']
                user = db_dict['user']
                password = db_dict['password']

                database = Database(db_ref=db_ref,
                                    host=host,
                                    port=port,
                                    dbname=dbname,
                                    user=user,
                                    password=password,
                                    keep_connection=False,  # Assume we don't want connections hanging around
                                    autocommit=True)

                database.submit_query('select 1 as test_field')  # Test DB connection

                database_dict[db_ref] = database
            except Exception as e:
                logger.warning('Unable to connect to database for %s: %s', db_ref, e.message)

        log_multiline(logger.debug, database_dict, 'database_dict', '\t')
        return database_dict

    def __init__(self):
        """Constructor for class GDF"""

        self._config_files = []  # List of config files read

        self._code_root = os.path.abspath(os.path.dirname(__file__))  # Directory containing module code

        # Create master configuration dict containing both command line and config_file parameters
        self._command_line_params = _get_command_line_params(GDF.ARG_DESCRIPTORS)

        self._debug = False
        self.debug = self._command_line_params['debug']

        # Create master configuration dict containing both command line and config_file parameters
        self._configuration = self._get_config(self._command_line_params['config_files'])

        self.cache_dir = self._command_line_params['cache_dir'] or self.cache_dir
        if not directory_writable(self.cache_dir):
            new_cache_dir = os.path.join(os.path.expanduser("~"), 'gdf', 'cache')
            logger.warning('Unable to access cache directory %s. Using %s instead.', self.cache_dir, new_cache_dir)
            self.cache_dir = new_cache_dir
            if not directory_writable(self.cache_dir):
                raise Exception('Unable to write to cache directory %s', self.cache_dir)

        # Convert self.refresh to Boolean
        self.refresh = self.debug or strtobool(self.refresh)

        # Force refresh if config has changed
        try:
            cached_config = self._get_cached_object('configuration.pkl')
            self.refresh = self.refresh or (self._configuration != cached_config)
        except:
            self.refresh = True

        if self.refresh:
            self._cache_object(self._configuration, 'configuration.pkl')
            logger.info('Forcing refresh of all cached data')

        # Create master database dict with Database objects keyed by db_ref
        try:
            self._databases = self._get_cached_object('databases.pkl')
            logger.info('Loaded cached database configuration %s', self._databases.keys())
        except:
            self._databases = self._get_dbs()
            self._cache_object(self._databases, 'databases.pkl')
            logger.info('Connected to databases %s', self._databases.keys())

        # Read storage configuration from cache or databases
        try:
            self._storage_config = self._get_cached_object('storage_config.pkl')
            logger.info('Loaded cached storage configuration %s', self._storage_config.keys())
        except:
            self._storage_config = self._get_storage_config()
            self._cache_object(self._storage_config, 'storage_config.pkl')
            logger.info('Read storage configuration from databases %s', self._storage_config.keys())

        log_multiline(logger.debug, self.__dict__, 'GDF.__dict__', '\t')

    def _do_storage_type_query(self, storage_types, args):
        """
        Generic function to execute a function across multiple databases, each function in its own thread
        Returns a dict which must be updated by db_function in a thread-safe manner

        Parameters:
            storage_types: List of storage_types to process (None for all storage types)
            args: list containing db_function to be multi-threaded and its arguments.
                NB: Last two arguments of db_function must be database and result_dict
        """

        def check_thread_exception():
            """"Check for exception raised by previous thread and raise it if found.
            Note that any other threads already underway will be allowed to finish normally.
            """
            global thread_exception
            logger.debug('thread_exception: %s', thread_exception)
            # Check for exception raised by previous thread and raise it if found
            if thread_exception:
                logger.error('Thread error: ' + thread_exception.message)
                raise thread_exception  # Raise the exception in the main thread

        def thread_execute(storage_type_function, *args, **kwargs):
            """Helper function to capture exception within the thread and set a global
            variable to be checked in the main thread
            N.B: THIS FUNCTION RUNS WITHIN THE SPAWNED THREAD
            """
            global thread_exception
            try:
                storage_type_function(*args, **kwargs)
            except Exception as e:
                thread_exception = e
                log_multiline(logger.error, traceback.format_exc(),
                              'Error in thread %s: %s' % (storage_type, e.message), '\t')
                raise thread_exception  # Re-raise the exception within the thread
            finally:
                logger.debug('Thread finished')

        storage_types = storage_types or self._storage_config.keys()

        result_dict = {}  # Nested dict to contain query results - must be updated in a thread-safe manner

        thread_list = []
        for storage_type in storage_types:
            check_thread_exception()

            process_thread = threading.Thread(target=thread_execute,
                                              args=args + [storage_type, result_dict],
                                              name=storage_type
                                              )
            thread_list.append(process_thread)
            process_thread.setDaemon(False)
            process_thread.start()
            logger.debug('Started thread %s', storage_type)

        # Wait for all threads to finish
        for process_thread in thread_list:
            check_thread_exception()
            process_thread.join()

        check_thread_exception()
        logger.debug('All threads finished')

        log_multiline(logger.debug, result_dict, 'result_dict', '\t')
        return result_dict

    def _get_storage_config(self):
        """
        Function to return a dict with details of all storage unit types managed in databases keyed as follows:

        Returns: Dict keyed as follows:

            <storage_type_tag>: {
                'db_ref':
                <db_ref>,
                'measurement_types': {
                    <measurement_type_tag>: {
                        <measurement_type_attribute_name>: <measurement_type_attribute_value>,
                        ...
                        }
                    ...
                    }
                'domains': {
                    <domain_name>
                        'dimensions':
                            <dimension_tag>,
                            ...
                            }
                    ...
                    }
                'dimensions'
                    <dimension_tag>
                    ...
                    }
                ...
                }
            ...
            }
        """

        def get_db_storage_config(database, result_dict):
            """
            Function to return a dict with details of all storage types managed in a single database

            Parameters:
                database: gdf.database object against which to run the query
                result_dict: dict to contain the result

            This is currently a bit ugly because it retrieves the de-normalised data in a single query and then has to
            build the tree from the flat result set. It could be done in a prettier (but slower) way with multiple
            queries
            """
            db_storage_config_dict = collections.OrderedDict()

            try:
                storage_type_filter_list = self._configuration[database.db_ref]['storage_types'].split(',')
            except:
                storage_type_filter_list = None
            logger.debug('storage_type_filter_list = %s', storage_type_filter_list)

            sql = '''-- Query to return all storage_type configuration info for database %s
select distinct
    storage_type_tag,
    storage_type_id,
    storage_type_name,
    storage_type_location,
    measurement_type_tag,
    measurement_metatype_id,
    measurement_type_id,
    measurement_type_index,
    measurement_metatype_name,
    measurement_type_name,
    nodata_value,
    datatype_name,
    numpy_datatype_name,
    gdal_datatype_name,
    netcdf_datatype_name,
    domain_tag,
    domain_id,
    domain_name,
    reference_system.reference_system_id,
    reference_system.reference_system_name,
    reference_system.reference_system_definition,
    reference_system.reference_system_unit,
    dimension_tag,
    dimension_name,
    dimension_id,
    dimension_order,
    dimension_extent,
    dimension_elements,
    dimension_cache,
    dimension_origin,
    dimension_extent::double precision / dimension_elements::double precision as dimension_element_size,
    indexing_type_name as indexing_type,
    reverse_index,
    index_reference_system.reference_system_id as index_reference_system_id,
    index_reference_system.reference_system_name as index_reference_system_name,
    index_reference_system.reference_system_definition as index_reference_system_definition,
    index_reference_system.reference_system_unit as index_reference_system_unit  ,
    property_name,
    attribute_string
from storage_type
join storage_type_measurement_type using(storage_type_id)
join measurement_type using(measurement_metatype_id, measurement_type_id)
join measurement_metatype using(measurement_metatype_id)
join datatype using(datatype_id)
join storage_type_dimension using(storage_type_id)
join dimension_domain using(dimension_id, domain_id)
join domain using(domain_id)
join dimension using(dimension_id)
join indexing_type using(indexing_type_id)
join reference_system using (reference_system_id)
left join reference_system index_reference_system on index_reference_system.reference_system_id = storage_type_dimension.index_reference_system_id
left join storage_type_dimension_property using(storage_type_id, domain_id, dimension_id)
left join property using(property_id)
''' % database.db_ref

            # Apply storage_type filter if configured
            if storage_type_filter_list:
                sql += "where storage_type_tag in ('" + "', '".join(storage_type_filter_list) + "')"

            sql += '''order by storage_type_tag, measurement_type_index, dimension_order;
'''

            storage_config_results = database.submit_query(sql)

            for record in storage_config_results.record_generator():
                log_multiline(logger.debug, record, 'record', '\t')

                storage_type_dict = db_storage_config_dict.get(record['storage_type_tag'])
                if storage_type_dict is None:
                    storage_type_dict = {'db_ref': database.db_ref,
                                         'storage_type_tag': record['storage_type_tag'],
                                         'storage_type_id': record['storage_type_id'],
                                         'storage_type_name': record['storage_type_name'],
                                         'storage_type_location': record['storage_type_location'],
                                         'measurement_types': collections.OrderedDict(),
                                         'domains': {},
                                         'dimensions': collections.OrderedDict()
                                         }

                db_storage_config_dict[record['storage_type_tag']] = storage_type_dict

                measurement_type_dict = storage_type_dict['measurement_types'].get(record['measurement_type_tag'])
                if measurement_type_dict is None:
                    measurement_type_dict = {'measurement_type_tag': record['measurement_type_tag'],
                                             'measurement_metatype_id': record['measurement_metatype_id'],
                                             'measurement_type_id': record['measurement_type_id'],
                                             'measurement_type_index': record['measurement_type_index'],
                                             'measurement_metatype_name': record['measurement_metatype_name'],
                                             'measurement_type_name': record['measurement_type_name'],
                                             'nodata_value': record['nodata_value'],
                                             'datatype_name': record['datatype_name'],
                                             'numpy_datatype_name': record['numpy_datatype_name'],
                                             'gdal_datatype_name': record['gdal_datatype_name'],
                                             'netcdf_datatype_name': record['netcdf_datatype_name']
                                             }

                    storage_type_dict['measurement_types'][record['measurement_type_tag']] = measurement_type_dict

                domain_dict = storage_type_dict['domains'].get(record['domain_tag'])
                if domain_dict is None:
                    domain_dict = {'domain_tag': record['domain_tag'],
                                   'domain_id': record['domain_id'],
                                   'domain_name': record['domain_name'],
                                   'reference_system_id': record['reference_system_id'],
                                   'reference_system_name': record['reference_system_name'],
                                   'reference_system_definition': record['reference_system_definition'],
                                   'reference_system_unit': record['reference_system_unit'],
                                   'dimensions': []
                                   }

                    storage_type_dict['domains'][record['domain_tag']] = domain_dict

                dimension_dict = storage_type_dict['dimensions'].get(record['dimension_tag'])
                if dimension_dict is None:
                    dimension_dict = {'dimension_tag': record['dimension_tag'],
                                      'dimension_name': record['dimension_name'],
                                      'dimension_id': record['dimension_id'],
                                      'dimension_order': record['dimension_order'],
                                      'dimension_extent': record['dimension_extent'],
                                      'dimension_elements': record['dimension_elements'],
                                      'dimension_cache': record['dimension_cache'],
                                      'dimension_origin': record['dimension_origin'],
                                      'dimension_element_size': record['dimension_element_size'],
                                      'indexing_type': record['indexing_type'],
                                      'reverse_index': record['reverse_index'],
                                      'domain_tag': record['domain_tag'],
                                      'domain_id': record['domain_id'],
                                      'domain_name': record['domain_name'],
                                      'reference_system_id': record['reference_system_id'],
                                      'reference_system_name': record['reference_system_name'],
                                      'reference_system_definition': record['reference_system_definition'],
                                      'reference_system_unit': record['reference_system_unit'],
                                      'index_reference_system_id': record['index_reference_system_id'],
                                      'index_reference_system_name': record['index_reference_system_name'],
                                      'index_reference_system_definition': record['index_reference_system_definition'],
                                      'index_reference_system_unit': record['index_reference_system_unit'],
                                      'properties': {}
                                      }

                    storage_type_dict['dimensions'][record['dimension_tag']] = dimension_dict
                    domain_dict['dimensions'].append(record['dimension_tag'])

                if dimension_dict['properties'].get(record['property_name']) is None:
                    dimension_dict['properties'][record['property_name']] = record['attribute_string']

            del storage_config_results

            sql = '''-- Find maxima and minima for all storage types and dimensions
select
    storage_type_tag,
    dimension_order,
    dimension_tag,
    min(storage_dimension_index) as min_index,
    max(storage_dimension_index) as max_index,
    min(storage_dimension_min) as min_value,
    max(storage_dimension_max) as max_value
from storage_type
join storage_type_dimension using(storage_type_id)
join storage using(storage_type_id)
join storage_dimension using(storage_type_id, storage_id, storage_version, domain_id, dimension_id)
join dimension using(dimension_id)
'''
            # Apply storage_type filter if configured
            if storage_type_filter_list:
                sql += "where storage_type_tag in ('" + "', '".join(storage_type_filter_list) + "')"

            sql += '''
group by 1,2,3
order by 1,2
'''
            min_max_results = database.submit_query(sql)
            for record in min_max_results.record_generator():
                logger.debug('record = %s', record)
                storage_type_dict = db_storage_config_dict.get(record['storage_type_tag'])
                logger.debug('storage_type_dict = %s', storage_type_dict)
                if storage_type_dict:
                    dimension_dict = storage_type_dict['dimensions'].get(record['dimension_tag'])
                    if True:  # dimension_dict:
                        dimension_dict['min_index'] = record['min_index']
                        dimension_dict['max_index'] = record['max_index']
                        dimension_dict['min_value'] = record['min_value']
                        dimension_dict['max_value'] = record['max_value']

                        #            log_multiline(logger.info, db_dict, 'db_dict', '\t')
            result_dict[database.db_ref] = db_storage_config_dict
            # End of per-DB function

        storage_config_dict = _do_db_query(self.databases, [get_db_storage_config])

        # Filter out duplicate storage unit types. Only keep first definition
        filtered_storage_config_dict = {}
        for db_ref in self._configuration.keys():
            db_storage_config_dict = storage_config_dict.get(db_ref)
            if not db_storage_config_dict:
                continue
            for storage_unit_type in db_storage_config_dict.keys():
                if storage_unit_type in filtered_storage_config_dict.keys():
                    logger.warning('Ignored duplicate storage unit type "%s" in DB "%s"' % (storage_unit_type, db_ref))
                else:
                    filtered_storage_config_dict[storage_unit_type] = storage_config_dict[db_ref][storage_unit_type]
        return filtered_storage_config_dict

    @property
    def code_root(self):
        return self._code_root

    @property
    def config_files(self):
        return self._config_files

    @property
    def command_line_params(self):
        return self._command_line_params

    @property
    def configuration(self):
        return self._configuration

    @property
    def databases(self):
        return self._databases

    @property
    def storage_config(self):
        return self._storage_config

    @property
    def debug(self):
        return self._debug

    @debug.setter
    def debug(self, debug_value):
        if self._debug != debug_value:
            self._debug = debug_value

            if self._debug:
                logger.setLevel(logging.DEBUG)
            else:
                logger.setLevel(logging.INFO)

    def get_descriptor(self, query_parameter=None):
        """
query_parameter = \
{
'storage_types':
    ['LS5TM', 'LS7ETM', 'LS8OLITIRS'],
'dimensions': {
     'x': {
           'range': (140, 142),
           'crs': 'EPSG:4326'
           },
     'y': {
           'range': (-36, -35),
           'crs': 'EPSG:4326'
           },
     't': {
           'range': (1293840000, 1325376000),
           'crs': 'SSE', # Seconds since epoch
           'grouping_function': GDF.solar_days_since_epoch
           }
     },
'polygon': '<some kind of text representation of a polygon for PostGIS to sort out>'
            # We won't be doing this in the pilot
}
"""

        # =======================================================================
        # # Create a dummy array of days since 1/1/1970 between min and max timestamps
        # min_date_ordinal = date.fromtimestamp(1293840000).toordinal()
        # max_date_ordinal = date.fromtimestamp(1325376000).toordinal()
        # date_array = np.array(range(min_date_ordinal - GDF.EPOCH_DATE_ORDINAL,
        #                       max_date_ordinal - GDF.EPOCH_DATE_ORDINAL), dtype=np.int16)
        #
        # descriptor = {
        #     'LS5TM': { # storage_type identifier
        #          'dimensions': ['x', 'y', 't'],
        #          'variables': { # These will be the variables which can be accessed as arrays
        #                'B10': {
        #                     'datatype': 'int16',
        #                     'nodata_value': -999
        #                     },
        #                'B20': {
        #                     'datatype': 'int16',
        #                     'nodata_value': -999
        #                     },
        #                'B30': {
        #                     'datatype': 'int16',
        #                     'nodata_value': -999
        #                     },
        #                'B40': {
        #                     'datatype': 'int16',
        #                     'nodata_value': -999
        #                     },
        #                'B50': {
        #                     'datatype': 'int16',
        #                     'nodata_value': -999
        #                     },
        #                'B70': {
        #                     'datatype': 'int16',
        #                     'nodata_value': -999
        #                     },
        #                'PQ': { # There is no reason why we can't put PQ in with NBAR if we want to
        #                     'datatype': 'int16'
        #                     }
        #                },
        #          'result_min': (140, -36, 1293840000),
        #          'result_max': (141, -35, 1325376000),
        #          'overlap': (0, 0, 0), # We won't be doing this in the pilot
        #          'buffer_size': (128, 128, 128), # Chunk size to use
        #          'result_shape': (8000, 8000, 40), # Overall size of result set
        #          'irregular_indices': { # Regularly indexed dimensions (e.g. x & y) won't need to be specified,
        #                                 # but we could also do that here if we wanted to
        #                't': date_array # Array of days since 1/1/1970
        #                },
        #          'storage_units': { # Should wind up with 8 for the 2x2x2 query above
        #                (140, -36, 2010): { # Storage unit indices
        #                     'storage_min': (140, -36, 1293840000),
        #                     'storage_max': (141, -35, 1293800400),
        #                     'storage_shape': (4000, 4000, 24)
        #                     },
        #                (140, -36, 2011): { # Storage unit indices
        #                     'storage_min': (140, -36, 1293800400),
        #                     'storage_max': (141, -35, 1325376000),
        #                     'storage_shape': (4000, 4000, 23)
        #                     },
        #                (140, -35, 2011): { # Storage unit indices
        #                     'storage_min': (140, -36, 1293840000),
        #                     'storage_max': (141, -35, 1293800400),
        #                     'storage_shape': (4000, 4000, 20)
        #                     }
        #     #          ...
        #     #          <more storage_unit sub-descriptors>
        #     #          ...
        #                }
        #     #    ...
        #     #    <more storage unit type sub-descriptors>
        #     #    ...
        #          }
        #     }
        # =======================================================================
        def get_db_descriptors(dimension_range_dict,
                               slice_dimension,
                               slice_grouping_function,
                               storage_types,
                               exclusive,
                               database,
                               result_dict):
            """
            Function to return descriptors for all storage_units which fall in the specified dimensional ranges

            Parameters:
                dimension_range_dict: dict defined as {<dimension_tag>: (<min_value>, <max_value>),
                                                       <dimension_tag>: (<min_value>, <max_value>)...}
                slice_dimension: Dimension along which to group results
                slice_locality: Range (in slice_dimension units) in which to group slices
                storage_types: list of storage_type_tags to include in query
                exclusive: Boolean flag to indicate whether query should exclude storage_units with lower dimensionality
                           than the specified range
                database: gdf.database object against which to run the query
                result_dict: dict to contain the result

            Return Value:
                {db_ref: <Descriptor as defined above>}
            """

            def update_storage_units_descriptor(storage_index_tuple,
                                                storage_type_dimensions,
                                                regular_storage_type_dimensions,
                                                storage_min_dict,
                                                overall_min_dict,
                                                storage_max_dict,
                                                overall_max_dict,
                                                storage_shape_dict,
                                                storage_slice_group_set,
                                                overall_slice_group_set,
                                                storage_units_descriptor
                                                ):
                """
                This function is a bit ugly, but it needs to run in two places so it's better not to have it inline
                """
                logger.debug('update_storage_units_descriptor() called')
                if storage_index_tuple is not None:  # We have values to write
                    for dimension in regular_storage_type_dimensions:
                        # Enforce query range on min/max values
                        storage_min_dict[dimension] = max(storage_min_dict[dimension],
                                                          dimension_minmax_dict[dimension][0])
                        storage_max_dict[dimension] = min(storage_max_dict[dimension],
                                                          dimension_minmax_dict[dimension][1])

                        storage_shape_dict[dimension] = round(
                            (storage_max_dict[dimension] - storage_min_dict[dimension]) /
                            self._storage_config[storage_type]['dimensions'][dimension]['dimension_element_size'],
                            GDF.DECIMAL_PLACES)

                    for dimension in irregular_storage_type_dimensions:  # Should be just 'T' for the EO trial
                        # TODO: Make the query and processig more general for multiple irregular dimensions
                        storage_min_dict[dimension] = min(storage_slice_group_set)
                        storage_max_dict[dimension] = max(storage_slice_group_set)

                        storage_shape_dict[dimension] = len(storage_slice_group_set)

                        # Merge slice group values into overall set
                    overall_slice_group_set |= storage_slice_group_set

                    # Update all overall max/mins
                    for dimension in storage_type_dimensions:
                        overall_min_dict[dimension] = min(overall_min_dict[dimension], storage_min_dict[dimension])
                        overall_max_dict[dimension] = max(overall_max_dict[dimension], storage_max_dict[dimension])

                    storage_unit_descriptor = {
                        'storage_min': tuple([storage_min_dict[dimension] for dimension in storage_type_dimensions]),
                        'storage_max': tuple([storage_max_dict[dimension] for dimension in storage_type_dimensions]),
                        'storage_shape': tuple([storage_shape_dict[dimension] for dimension in storage_type_dimensions])
                    }

                    log_multiline(logger.debug, storage_unit_descriptor, 'storage_unit_descriptor', '\t')
                    storage_units_descriptor[storage_index_tuple] = storage_unit_descriptor
                    # End of update_storage_units_descriptor() definition

            # Start of get_db_descriptor() definition
            logger.debug('update_storage_units_descriptor() called')
            logger.debug('storage_types = %s', storage_types)

            for storage_config in self._storage_config.values():
                # Disregard all storage types not in this DB
                if storage_config['db_ref'] != database.db_ref:
                    continue

                storage_type = storage_config['storage_type_tag']

                # Skip any storage_types if they are not in the specified list
                if storage_types and (storage_type not in storage_types):
                    continue

                logger.debug('storage_type = %s', storage_type)

                # list of dimensions for storage_type sorted by creation order
                storage_type_dimensions = [dimension['dimension_tag'] for dimension in
                                           sorted(storage_config['dimensions'].values(),
                                                  key=lambda dimension: dimension['dimension_order'])]
                logger.debug('storage_type_dimensions = %s', storage_type_dimensions)

                # list of dimensions for range query sorted by creation order (do all if
                if dimension_range_dict:
                    range_dimensions = [dimension_tag for dimension_tag in storage_type_dimensions if
                                        dimension_tag in dimension_range_dict.keys()]
                else:
                    range_dimensions = []
                logger.debug('range_dimensions = %s', range_dimensions)

                # Skip this storage_type if exclusive flag set and dimensionality is less
                # than query range dimnsionality
                if exclusive and (set(storage_type_dimensions) < set(range_dimensions)):
                    continue

                # Create a sub-descriptor for each storage_type
                storage_type_descriptor = {}

                sql = '''-- Find all slices in storage_units which fall in range for storage type %s
select distinct''' % storage_type
                for dimension_tag in storage_type_dimensions:
                    sql += '''
%s.storage_dimension_index as %s_index,
%s.storage_dimension_min as %s_min,
%s.storage_dimension_max as %s_max,'''.replace('%s', dimension_tag)
                sql += '''
slice_index_value
from storage
'''
                for dimension_tag in storage_type_dimensions:
                    sql += '''join (
select *
from storage_dimension
join dimension using(dimension_id)
where storage_type_id = %d
and storage_version = 0
and dimension.dimension_tag = '%s'
''' % (storage_config['storage_type_id'],
       dimension_tag
       )
                    # Apply range filters
                    if dimension_tag in range_dimensions:
                        sql += '''and (storage_dimension_min < %f
    and storage_dimension_max > %f)
''' % (dimension_range_dict[dimension_tag][1],  # Max
       dimension_range_dict[dimension_tag][0]  # Min
       )

                    sql += ''') %s using(storage_type_id, storage_id, storage_version)
''' % dimension_tag

                sql += '''
    join storage_dataset using (storage_type_id, storage_id, storage_version)
    join (
      select dataset_type_id,
        dataset_id,
        coalesce(indexing_value, (min_value+max_value)/2) as slice_index_value,
        dimension_tag,
        min_value,
        max_value,
        indexing_value
      from dataset
      join dataset_dimension using(dataset_type_id, dataset_id)
      join dimension using(dimension_id)
      where dimension_tag = '%s'
    ) dataset_index using(dataset_type_id, dataset_id)
''' % slice_dimension

                # Restrict slices to those within range if required
                if slice_dimension in range_dimensions:
                    sql += '''where slice_index_value between %f and %f
''' % (dimension_range_dict[slice_dimension][0],  # Min
       dimension_range_dict[slice_dimension][1])  # Max

                sql += '''
order by ''' + '_index, '.join(storage_type_dimensions) + '''_index, slice_index_value;
'''
                log_multiline(logger.debug, sql, 'SQL', '\t')

                slice_result_set = database.submit_query(sql)

                storage_units_descriptor = {}  # Dict to hold all storage unit descriptors for this storage type

                regular_storage_type_dimensions = [dimension for dimension in storage_type_dimensions if
                                                   self._storage_config[storage_type]['dimensions'][dimension][
                                                       'indexing_type'] == 'regular']
                irregular_storage_type_dimensions = [dimension for dimension in storage_type_dimensions if
                                                     self._storage_config[storage_type]['dimensions'][dimension][
                                                         'indexing_type'] == 'irregular']
                fixed_storage_type_dimensions = [dimension for dimension in storage_type_dimensions if
                                                 self._storage_config[storage_type]['dimensions'][dimension][
                                                     'indexing_type'] == 'fixed']

                # Define initial max/min/shape values
                dimension_minmax_dict = {
                    dimension: (dimension_range_dict.get(dimension) or (-sys.maxsize - 1, sys.maxsize)) for dimension in
                    storage_type_dimensions}
                storage_min_dict = {dimension: sys.maxsize for dimension in storage_type_dimensions}
                storage_max_dict = {dimension: -sys.maxsize - 1 for dimension in storage_type_dimensions}
                storage_shape_dict = {dimension: 0 for dimension in storage_type_dimensions}
                overall_min_dict = {dimension: sys.maxsize for dimension in storage_type_dimensions}
                overall_max_dict = {dimension: -sys.maxsize - 1 for dimension in storage_type_dimensions}
                overall_shape_dict = {dimension: 0 for dimension in storage_type_dimensions}

                storage_slice_group_set = set()
                overall_slice_group_set = set()

                storage_index_tuple = None
                # Iterate through all records once only
                for record_dict in slice_result_set.record_generator({'slice_group_value': slice_grouping_function}):
                    logger.debug('record_dict = %s', record_dict)
                    new_storage_index_tuple = tuple([record_dict[dimension_tag.lower() + '_index']
                                                     for dimension_tag in storage_type_dimensions])

                    if new_storage_index_tuple != storage_index_tuple:  # Change in storage unit
                        update_storage_units_descriptor(storage_index_tuple,
                                                        storage_type_dimensions,
                                                        regular_storage_type_dimensions,
                                                        storage_min_dict,
                                                        overall_min_dict,
                                                        storage_max_dict,
                                                        overall_max_dict,
                                                        storage_shape_dict,
                                                        storage_slice_group_set,
                                                        overall_slice_group_set,
                                                        storage_units_descriptor
                                                        )

                        # Re-initialise max & min dicts for new storage unit
                        storage_min_dict = {dimension: sys.maxsize for dimension in storage_type_dimensions}
                        storage_max_dict = {dimension: -sys.maxsize - 1 for dimension in storage_type_dimensions}
                        storage_slice_group_set = set()
                        storage_index_tuple = new_storage_index_tuple

                    # Do the following for every record
                    storage_slice_group_set.add(record_dict['slice_group_value'])
                    # Update min & max values
                    for dimension in regular_storage_type_dimensions:
                        storage_min_dict[dimension] = min(storage_min_dict[dimension],
                                                          record_dict['%s_min' % dimension.lower()])
                        storage_max_dict[dimension] = max(storage_max_dict[dimension],
                                                          record_dict['%s_max' % dimension.lower()])

                # All records processed - write last descriptor
                update_storage_units_descriptor(storage_index_tuple,
                                                storage_type_dimensions,
                                                regular_storage_type_dimensions,
                                                storage_min_dict,
                                                overall_min_dict,
                                                storage_max_dict,
                                                overall_max_dict,
                                                storage_shape_dict,
                                                storage_slice_group_set,
                                                overall_slice_group_set,
                                                storage_units_descriptor
                                                )

                if storage_units_descriptor:  # If any storage units were found
                    storage_type_descriptor['storage_units'] = storage_units_descriptor

                    # Determine overall max/min/shape values
                    for dimension in regular_storage_type_dimensions:
                        overall_shape_dict[dimension] = round(
                            (overall_max_dict[dimension] - overall_min_dict[dimension]) /
                            self._storage_config[storage_type]['dimensions'][dimension]['dimension_element_size'],
                            GDF.DECIMAL_PLACES)

                    for dimension in irregular_storage_type_dimensions:  # Should be just 'T' for the EO trial
                        # Show all unique group values in order
                        storage_type_descriptor['irregular_indices'] = {
                            dimension: np.array(sorted(list(overall_slice_group_set)))}  # , dtype = np.int16)}

                        overall_shape_dict[dimension] = len(overall_slice_group_set)

                    storage_type_descriptor['dimensions'] = storage_type_dimensions

                    storage_type_descriptor['result_min'] = tuple(
                        [overall_min_dict[dimension] for dimension in storage_type_dimensions])
                    storage_type_descriptor['result_max'] = tuple(
                        [overall_max_dict[dimension] for dimension in storage_type_dimensions])
                    storage_type_descriptor['result_shape'] = tuple(
                        [overall_shape_dict[dimension] for dimension in storage_type_dimensions])

                    # Show all unique group values in order
                    # TODO: Don't make this hard-coded for T slices
                    storage_type_descriptor['irregular_indices'] = {
                        'T': np.array(sorted(list(overall_slice_group_set)), dtype=np.int32)}

                    storage_type_descriptor['variables'] = dict(
                        self._storage_config[storage_type]['measurement_types'])

                    result_dict[storage_type] = storage_type_descriptor
                    # End of per-DB function

        # Start of cross-DB get_descriptor function
        query_parameter = query_parameter or {}  # Handle None as a parameter

        try:
            dimension_range_dict = {dimension_tag.upper(): query_parameter['dimensions'][dimension_tag].get('range') for
                                    dimension_tag in query_parameter['dimensions'].keys()}
        except KeyError:
            dimension_range_dict = {}

        try:
            storage_types = [storage_type.upper() for storage_type in query_parameter['storage_types'] if
                             storage_type in self._storage_config.keys()]
        except KeyError:
            try:
                storage_types = [query_parameter['storage_type'].upper()]  # Check for single storage type
            except KeyError:
                storage_types = self._storage_config.keys()

        # Make self.solar_days_since_epoch the default grouping function for T
        # TODO: Make this more general for all irregular dimensions
        try:  # Check for both upper & lower case dimension tag
            slice_grouping_function = query_parameter['dimensions']['T'][
                                          'grouping_function'] or solar_days_since_epoch
        except KeyError:
            try:
                slice_grouping_function = query_parameter['dimensions']['t'][
                                              'grouping_function'] or solar_days_since_epoch
            except KeyError:
                slice_grouping_function = solar_days_since_epoch

        return _do_db_query({db_ref: self.databases[db_ref] for db_ref in sorted(
            set([self._storage_config[storage_type]['db_ref'] for storage_type in storage_types]))},
                            [get_db_descriptors,
                             dimension_range_dict,
                             'T',
                             slice_grouping_function,
                             storage_types,
                             False
                             ]
                            )

    @staticmethod
    def get_storage_filename(storage_type, storage_indices):
        """
        Function to return the filename for a storage unit file with the specified storage_type & storage_indices
        """
        return storage_type + '_' + '_'.join([str(index) for index in storage_indices]) + '.nc'

    def get_storage_dir(self, storage_type):
        """
        Function to return the filename for a storage unit file with the specified storage_type & storage_indices
        """
        return os.path.join(self._storage_config[storage_type]['storage_type_location'], storage_type)

    def get_storage_path(self, storage_type, storage_indices):
        """
        Function to return the full path to a storage unit file with the specified storage_type & storage_indices
        """
        return os.path.join(self.get_storage_dir(storage_type),
                            self.get_storage_filename(storage_type, storage_indices))

    def ordinate2index(self, storage_type, dimension, ordinate):
        """
        Return the storage unit index from the reference system ordinate for the specified storage type, ordinate value
        and dimension tag
        """
        if dimension == 'T':
            # TODO: Make this more general - need to cater for other reference systems besides seconds since epoch
            index_reference_system_name = self.storage_config[storage_type]['dimensions']['T'][
                'index_reference_system_name'].lower()
            logger.debug('index_reference_system_name = %s', index_reference_system_name)
            datetime_value = secs2dt(ordinate)
            if index_reference_system_name == 'decade':
                return datetime_value.year // 10
            if index_reference_system_name == 'year':
                return datetime_value.year
            elif index_reference_system_name == 'month':
                return datetime_value.year * 12 + datetime_value.month - 1
        else:
            return int(
                floor((ordinate - self.storage_config[storage_type]['dimensions'][dimension]['dimension_origin']) /
                      self.storage_config[storage_type]['dimensions'][dimension]['dimension_extent']))

    def index2ordinate(self, storage_type, dimension, index):
        """
        Return the reference system ordinate from the storage unit index for the specified storage type, index value and
         dimension tag
        """
        if dimension == 'T':
            # TODO: Make this more general - need to cater for other reference systems besides seconds since epoch
            index_reference_system_name = self.storage_config[storage_type]['dimensions']['T'][
                'index_reference_system_name'].lower()
            logger.debug('index_reference_system_name = %s', index_reference_system_name)
            if index_reference_system_name == 'decade':
                return dt2secs(datetime(index * 10, 1, 1))
            if index_reference_system_name == 'year':
                return dt2secs(datetime(index, 1, 1))
            elif index_reference_system_name == 'month':
                return dt2secs(datetime(index // 12, index % 12 + 1, 1))
        else:  # Not time
            return ((index * self.storage_config[storage_type]['dimensions'][dimension]['dimension_extent']) +
                    self.storage_config[storage_type]['dimensions'][dimension]['dimension_origin'])

    def get_data(self, data_request_descriptor=None, destination_filename=None):
        """
        Function to return composite in-memory arrays

        data_request = \
        {
        'storage_type': 'LS5TM',
        'variables': ('B30', 'B40','PQ'), # Note that we won't necessarily have PQ in the same storage unit
        'dimensions': {
             'x': {
                   'range': (140, 142),
                   'array_range': (0, 127)
                   'crs': 'EPSG:4326'
                   },
             'y': {
                   'range': (-36, -35),
                   'array_range': (0, 127)
                   'crs': 'EPSG:4326'
                   },
             't': {
                   'range': (1293840000, 1325376000),
                   'array_range': (0, 127)
                   'crs': 'SSE', # Seconds since epoch
                   'grouping_function': '<e.g. gdf.solar_day>'
                   }
             },
        'polygon': '<some kind of text representation of a polygon for PostGIS to sort out>'
                    # We won't be doing this in the pilot
        }



        data_response = \
        {
        'dimensions': ['x', 'y', 't'],
        'arrays': { # All of these will have the same shape
             'B30': '<Numpy array>',
             'B40': '<Numpy array>',
             'PQ': '<Numpy array>'
             },
        'indices': [ # These will be the actual x, y & t (long, lat & time) values for each array index
            '<numpy array of x indices>',
            '<numpy array of y indices>',
            '<numpy array of t indices>'
            ]
        'element_sizes': [ # These will be the element sizes for each dimension
            '< x element size>',
            '< y element size>',
            '< t element size>'
            ]
        'coordinate_reference_systems': [ # These will be the coordinate_reference_systems for each dimension
            '< x CRS>',
            '< y CRS>',
            '< t CRS>'
            ]
        }
        """
        if not data_request_descriptor:
            data_request_descriptor = {}
        storage_type = data_request_descriptor['storage_type']

        # Convert dimension tags to upper case
        range_dict = {dimension.upper(): dimension_spec['range']
                      for dimension, dimension_spec in data_request_descriptor['dimensions'].items()}
        # Create dict of array slices if specified
        slice_dict = {dimension.upper(): slice(*dimension_spec['array_range'])
                      for dimension, dimension_spec in data_request_descriptor['dimensions'].items() if
                      dimension_spec.get('array_range')}

        # Create dict of grouping functions if specified
        grouping_function_dict = {dimension.upper(): dimension_spec['grouping_function']
                                  for dimension, dimension_spec in data_request_descriptor['dimensions'].items() if
                                  dimension_spec.get('grouping_function')}

        # Default grouping function for time is self.solar_days_since_epoch
        grouping_function_dict['T'] = grouping_function_dict.get('T') or solar_days_since_epoch

        storage_config = self._storage_config[storage_type]
        dimension_config = storage_config['dimensions']
        dimensions = dimension_config.keys()  # All dimensions in order
        # irregular_dimensions = [dimension for dimension in dimensions if dimension_config[dimension]['indexing_type']
        # == 'irregular']
        range_dimensions = [dimension for dimension in dimensions if
                            dimension in range_dict.keys()]  # Range dimensions in order
        dimension_element_sizes = {dimension: dimension_config[dimension]['dimension_element_size'] for dimension in
                                   dimensions}

        # Default to all variables if none specified
        variable_names = data_request_descriptor.get('variables') or storage_config['measurement_types'].keys()

        # Create complete range dict with minmax tuples for every dimension, either calculated from supplied ranges or
        # looked up from config if not supplied
        # TODO: Do something a bit nicer than the "- 0.000001" on the upper bound
        # get the correct indices on storage unit boundaries
        index_range_dict = {dimensions[dimension_index]: (
            (self.ordinate2index(storage_type, dimensions[dimension_index], range_dict[dimensions[dimension_index]][0]),
             self.ordinate2index(storage_type, dimensions[dimension_index],
                                 range_dict[dimensions[dimension_index]][1] - pow(0.1, GDF.DECIMAL_PLACES)))
            if dimensions[dimension_index] in range_dimensions
            else (dimension_config[dimensions[dimension_index]]['min_index'],
                  dimension_config[dimensions[dimension_index]]['max_index']))
                            for dimension_index in range(len(dimensions))}
        logger.debug('index_range_dict = %s', index_range_dict)

        # Find all existing storage units in range and retrieve the indices in ranges for each dimension
        subset_dict = collections.OrderedDict()
        dimension_index_dict = {dimension: set() for dimension in
                                dimensions}  # Dict containing set (converted to list) of unique indices
        # for each dimension
        # Iterate through all possible storage unit index combinations
        # TODO: Make this more targeted and efficient - OK for continuous ranges, but probably could do better
        for indices in itertools.product(
                *[range(index_range_dict[dimension][0], index_range_dict[dimension][1] + 1) for dimension in
                  dimensions]):
            logger.debug('indices = %s', indices)
            storage_path = self.get_storage_path(storage_type, indices)
            logger.debug('Opening storage unit %s', storage_path)
            if os.path.exists(storage_path):
                gdfnetcdf = GDFNetCDF(storage_config, netcdf_filename=storage_path, decimal_places=GDF.DECIMAL_PLACES)
                subset_indices = gdfnetcdf.get_subset_indices(range_dict)
                if not subset_indices:
                    raise Exception('Invalid subset range %s for storage unit %s' % (
                        range_dict, storage_path))  # This should never happen

                # Keep track of all indices for each dimension
                for dimension in dimensions:
                    dimension_indices = np.around(subset_indices[dimension], GDF.DECIMAL_PLACES)
                    # TODO: Find a vectorised way of doing this instead of using sets
                    dimension_index_dict[dimension] |= set(dimension_indices.tolist())

                subset_dict[indices] = (gdfnetcdf, subset_indices)
        logger.debug('%d storage units found', len(subset_dict))
        logger.debug('subset_dict = %s', subset_dict)

        # TODO: Do this check more thoroughly
        assert destination_filename or len(
            subset_dict) <= GDF.MAX_UNITS_IN_MEMORY, 'Too many storage units for an in-memory query'

        for dimension in dimensions:
            # Expect to find indices in all dimensions
            if not dimension_index_dict[dimension]:
                logger.warning('No data found')
                return

            # Convert index set to sorted list
            dimension_index_dict[dimension] = sorted(dimension_index_dict[dimension])

        # Create lookup arrays (grouped_value_dict) and sorted sets of of unique
        # values (result_grouped_value_dict) for grouped indices
        ungrouped_value_dict = {}
        grouped_value_dict = {}
        result_grouped_value_dict = {}
        for dimension in dimensions:
            grouping_function = grouping_function_dict.get(dimension)
            if grouping_function:
                # Create list of ungrouped values
                ungrouped_value_dict[dimension] = dimension_index_dict[dimension]
                # Create list of grouped values
                # TODO: Replace this awful code which creates a "fake" record dict for the grouping function
                grouped_value_dict[dimension] = [grouping_function(
                    {'slice_index_value': t_index, 'x_min': dimension_index_dict['X'][0],
                     'x_max': dimension_index_dict['X'][-1]}) for t_index in dimension_index_dict['T']]
                # Create sorted array of unique values
                result_grouped_value_dict[dimension] = np.array(sorted(set(grouped_value_dict[dimension])))
                # Convert grouped_value_dict[dimension] from list to array
                grouped_value_dict[dimension] = np.array(grouped_value_dict[dimension])

        logger.debug('dimension_index_dict = %s', dimension_index_dict)
        logger.debug('grouped_value_dict = %s', grouped_value_dict)
        logger.debug('result_grouped_value_dict = %s', result_grouped_value_dict)

        # Create composite result array indices either from
        # result_grouped_value_dict if irregular & grouped or created as a range
        # if regular
        result_array_indices = {
            dimension: (np.array(result_grouped_value_dict[dimension]) if dimension in result_grouped_value_dict.keys()
                        else np.around(np.arange(dimension_index_dict[dimension][0],
                                                 dimension_index_dict[dimension][-1] + pow(0.1, GDF.DECIMAL_PLACES),
                                                 dimension_element_sizes[dimension]), GDF.DECIMAL_PLACES))
            for dimension in dimensions}

        # Reverse any indices with reverse_index flag set
        for dimension in dimensions:
            if dimension_config[dimension]['reverse_index']:
                result_array_indices[dimension] = result_array_indices[dimension][::-1]

        # Apply optional array ranges
        # TODO: TEST THIS!
        if slice_dict:
            logger.debug('Applying slices from slice_dict %s', slice_dict)
            result_array_indices = {dimension: (index_array[slice_dict[dimension]] if dimension in slice_dict.keys()
                                                else index_array)
                                    for dimension, index_array in result_array_indices.items()}
            # Revise index_range_dict to conform to reduced ranges
            restricted_range_dict = {dimension: ((max(range_dict[dimension][0], result_array_indices[dimension][0]),
                                                  min(range_dict[dimension][1], result_array_indices[dimension][
                                                      -1])) if dimension in range_dict.keys()
                                                 else (
                result_array_indices[dimension][0], result_array_indices[dimension][-1]))
                                     for dimension in dimensions}
        else:  # No array range supplied - use full range
            restricted_range_dict = range_dict

        logger.debug('result_array_indices = %s', result_array_indices)
        logger.debug('restricted_range_dict = %s', restricted_range_dict)

        # Create empty result_dict for returning result
        result_dict = {
            'dimensions': dimensions,
            'arrays': {},
            'indices': result_array_indices,
            'element_sizes': [dimension_config[dimension]['dimension_element_size'] for dimension in dimensions],
            'coordinate_reference_systems': [
                {'reference_system_name': dimension_config[dimension]['reference_system_name'],
                 'reference_system_definition': dimension_config[dimension]['reference_system_definition'],
                 'reference_system_unit': dimension_config[dimension]['reference_system_unit']
                 }
                for dimension in dimensions]
        }

        # Create empty composite result arrays
        array_shape = [len(result_array_indices[dimension]) for dimension in dimensions]
        logger.debug('array_shape = %s', array_shape)

        for variable_name in variable_names:
            dtype = storage_config['measurement_types'][variable_name]['numpy_datatype_name']
            logger.debug('%s dtype = %s', variable_name, dtype)

            # TODO: Do something better for variables with no no-data value specified (e.g. PQ)
            nodata_value = storage_config['measurement_types'][variable_name]['nodata_value'] or 0
            result_dict['arrays'][variable_name] = np.ones(shape=array_shape, dtype=dtype) * nodata_value

        # Iterate through all storage units with data
        # TODO: Implement merging of multiple group layers. Current implemntation
        # breaks when more than one layer per group
        for indices in subset_dict.keys():
            # Unpack tuple
            gdfnetcdf = subset_dict[indices][0]
            subset_indices = subset_dict[indices][1]

            selection = []
            for dimension in dimensions:
                dimension_indices = np.around(subset_indices[dimension], GDF.DECIMAL_PLACES)
                logger.debug('%s dimension_indices = %s', dimension, dimension_indices)

                logger.debug('result_array_indices[%s] = %s', dimension, result_array_indices[dimension])
                if dimension in grouping_function_dict.keys():
                    # logger.debug('Un-grouped %s min_index_value = %s, max_index_value = %s', dimension,
                    # min_index_value, max_index_value)
                    subset_group_values = grouped_value_dict[dimension][np.in1d(ungrouped_value_dict[dimension],
                                                                                dimension_indices)]  # Convert raw time
                    # values to group values
                    logger.debug('%s subset_group_values = %s', dimension, subset_group_values)
                    dimension_selection = np.in1d(result_array_indices[dimension],
                                                  subset_group_values)  # Boolean array mask for result array
                    logger.debug('%s dimension_selection = %s', dimension, dimension_selection)
                else:
                    dimension_selection = np.in1d(result_array_indices[dimension],
                                                  dimension_indices)  # Boolean array mask for result array
                    logger.debug('%s boolean dimension_selection = %s', dimension, dimension_selection)
                    dimension_selection = slice(np.where(dimension_selection)[0][0],
                                                np.where(dimension_selection)[0][-1] + 1)
                    logger.debug('%s slice dimension_selection = %s', dimension, dimension_selection)
                selection.append(dimension_selection)
            logger.debug('selection = %s', selection)

            for variable_name in variable_names:
                # Read data into array
                read_array = gdfnetcdf.read_subset(variable_name, restricted_range_dict)[0]
                logger.debug('read_array from %s = %s', gdfnetcdf.netcdf_filename, read_array)
                logger.debug('read_array.shape from %s = %s', gdfnetcdf.netcdf_filename, read_array.shape)

                logger.debug("result_dict['arrays'][variable_name][selection].shape = %s",
                             result_dict['arrays'][variable_name][selection].shape)
                result_dict['arrays'][variable_name][selection] = gdfnetcdf.read_subset(variable_name, range_dict)[0]

        log_multiline(logger.debug, result_dict, 'result_dict', '\t')
        logger.debug('Result size = %s', tuple(len(result_array_indices[dimension]) for dimension in dimensions))

        return result_dict
