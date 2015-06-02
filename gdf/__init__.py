import os
import sys
import threading
import traceback
import numpy as np
from datetime import datetime, date, timedelta
import pytz

from _database import Database, CachedResultSet
from _arguments import CommandLineArgs
from _config_file import ConfigFile
import logging

from EOtools.utils import log_multiline

# Set handler for root logger to standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
#console_handler.setLevel(logging.DEBUG)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)
logging.root.addHandler(console_handler)

logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG) # Logging level for this module

thread_exception = None

class GDF(object):
    '''
    Class definition for GDF (General Data Framework).
    Manages configuration and database connections.
    '''
    DEFAULT_CONFIG_FILE = 'gdf_default.conf' # N.B: Assumed to reside in code root directory
    
    def get_command_line_params(self):
        '''
        Function to return a dict of command line parameters
        '''
        command_line_args_object = CommandLineArgs()
        
        return command_line_args_object.arguments
        
    def get_config(self):
        '''
        Function to return a nested dict of config file entries
        
        Returns: dict {<db_ref>: {<param_name>: <param_value>,... },... }
        '''
        config_dict = {}
        
        # Use default config file if none provided
        config_files_string = self._command_line_params['config_files'] or os.path.join(self._code_root, GDF.DEFAULT_CONFIG_FILE)
        
        # Set list of absolute config file paths from comma-delimited list
        self._config_files = [os.path.abspath(config_file) for config_file in config_files_string.split(',')] 
        log_multiline(logger.debug, self._config_files, 'self._config_files', '\t')
           
        for config_file in self._config_files:
            config_file_object = ConfigFile(config_file)
        
            # Merge all configuration sections from individual config files to config dict
            config_dict.update(config_file_object.configuration)
        
        log_multiline(logger.debug, config_dict, 'config_dict', '\t')
        return config_dict
    
    def get_dbs(self):
        '''
        Function to return a dict of database objects keyed by db_ref
        '''
        database_dict = {}
        
        # Create a database connection for every valid configuration
        for section_name in sorted(self._configuration.keys()):
            section_dict = self._configuration[section_name]
            try:
                host = section_dict['host']
                port = section_dict['port']
                dbname = section_dict['dbname']
                user = section_dict['user']
                password = section_dict['password']
                
                database = Database(db_ref=section_name,
                                    host=host, 
                                    port=port, 
                                    dbname=dbname, 
                                    user=user, 
                                    password=password, 
                                    keep_connection=False, # Assume we don't want connections hanging around
                                    autocommit=True)
                
                database.submit_query('select 1 as test_field') # Test DB connection
                
                database_dict[section_name] = database
            except Exception, e:
                logger.warning('Unable to connect to database for %s: %s', section_name, e.message)

        log_multiline(logger.debug, database_dict, 'database_dict', '\t')
        return database_dict
        

    def __init__(self):
        '''Constructor for class GDF
        '''
        self._config_files = [] # List of config files read
        
        self._code_root = os.path.abspath(os.path.dirname(__file__)) # Directory containing module code
        
        # Create master configuration dict containing both command line and config_file parameters
        self._command_line_params = self.get_command_line_params()
        
        if self._command_line_params['debug']:
            logger.setLevel(logging.DEBUG)
        #=======================================================================
        # else:
        #     logger.setLevel(logging.INFO)
        #=======================================================================
                
        # Create master configuration dict containing both command line and config_file parameters
        self._configuration = self.get_config()
                
        # Create master database dict
        self._databases = self.get_dbs()
        
        # Read configuration from databases
        self._storage_config = self.get_storage_config()
        
        log_multiline(logger.debug, self.__dict__, 'GDF.__dict__', '\t')

        
    def do_db_query(self, databases, args):
        '''
        Generic function to execute a function across multiple databases, each function in its own thread
        Returns a dict which must be updated by db_function in a thread-safe manner 
        
        Parameters:
            databases: dict of database objects keyed by db_ref
            args: list containing db_function to be multi-threaded and its arguments. 
                NB: Last two arguments of db_function must be database and result_dict 
        '''        
        def check_thread_exception():
            """"Check for exception raised by previous thread and raise it if found.
            Note that any other threads already underway will be allowed to finish normally.
            """
            global thread_exception
            logger.debug('thread_exception: %s', thread_exception)
            # Check for exception raised by previous thread and raise it if found
            if thread_exception:
                logger.error('Thread error: ' + thread_exception.message)
                raise thread_exception # Raise the exception in the main thread
    
        def thread_execute(db_function, *args, **kwargs):
            """Helper function to capture exception within the thread and set a global
            variable to be checked in the main thread
            N.B: THIS FUNCTION RUNS WITHIN THE SPAWNED THREAD
            """
            global thread_exception
            try:
                db_function(*args, **kwargs)
            except Exception, e:
                thread_exception = e
                log_multiline(logger.error, traceback.format_exc(), 'Error in thread: ' + e.message, '\t')
                raise thread_exception # Re-raise the exception within the thread
            finally:
                logger.debug('Thread finished')

        result_dict = {} # Nested dict to contain query results - must be updated in a thread-safe manner

        thread_list = []
        for db_ref in sorted(databases.keys()):
            check_thread_exception()
            
            database = databases[db_ref]
            process_thread = threading.Thread(target=thread_execute,                    
                                              args=args+[database, result_dict]
                                              )
            thread_list.append(process_thread)
            process_thread.setDaemon(False)
            process_thread.start()
            logger.debug('Started thread for get_config_data(%s, %s)', database, args)

        # Wait for all threads to finish
        for process_thread in thread_list:
            check_thread_exception()
            process_thread.join()

        check_thread_exception()
        logger.debug('All threads finished')

        log_multiline(logger.debug, result_dict, 'result_dict', '\t')
        return result_dict

    def get_storage_config(self):
        '''
        Function to return a dict with details of all dimensions managed in databases keyed as follows:
          
        Returns: Dict keyed as follows:
          
            <db_ref>
                'storage_types'
                    <storage_type_tag>
                        'measurement_types'
                            <measurement_type_tag>
                            ...
                        'domains'
                            <domain_name>
                                'dimensions'
                                    <dimension_tag>
                                    ...
                            ...
                        'dimensions'
                            <dimension_tag>
                            ...
                    ...
            ...
         '''
        def get_db_storage_config(database, result_dict):
            '''
            Function to return a dict with details of all dimensions managed in a single database 
            
            Parameters:
                database: gdf.database object against which to run the query
                result_dict: dict to contain the result
                        
            This is currently a bit ugly because it retrieves the de-normalised data in a single query and then has to
            build the tree from the flat result set. It could be done in a prettier (but slower) way with multiple queries
            '''
            db_dict = {'storage_types': {}}
            
            try:
                storage_type_filter_list = self._configuration[database.db_ref]['storage_types'].split(',')
            except:
                storage_type_filter_list = None
            logger.debug('storage_type_filter_list = %s', storage_type_filter_list)
              
            SQL = '''-- Query to return all storage_type configuration info for database %s
select distinct
storage_type_tag,
storage_type_id,
storage_type_name,
measurement_type_tag,
measurement_metatype_id,
measurement_type_id,
measurement_type_index, 
measurement_metatype_name,
measurement_type_name,
domain_tag,
domain_id,
domain_name,
reference_system.reference_system_id,
reference_system.reference_system_name,
reference_system.reference_system_definition,
reference_system.reference_system_unit,
dimension_tag,
dimension_id,
dimension_order,
dimension_extent,
dimension_elements,
dimension_cache,
dimension_origin,
index_reference_system.reference_system_id as index_reference_system_id,
index_reference_system.reference_system_name as index_reference_system_name,
index_reference_system.reference_system_definition as index_reference_system_definition,
index_reference_system.reference_system_unit as index_reference_system_unit  
from storage_type 
join storage_type_measurement_type using(storage_type_id)
join measurement_type using(measurement_metatype_id, measurement_type_id)
join measurement_metatype using(measurement_metatype_id)
join storage_type_dimension using(storage_type_id)
join dimension_domain using(dimension_id, domain_id)
join domain using(domain_id)
join dimension using(dimension_id)
join indexing_type using(indexing_type_id)
join reference_system using (reference_system_id)
left join reference_system index_reference_system on index_reference_system.reference_system_id = storage_type_dimension.index_reference_system_id
''' % database.db_ref

            # Apply storage_type filter if configured
            if storage_type_filter_list:
                SQL += "where storage_type_tag in ('" + "', '".join(storage_type_filter_list) + "')"
                
            SQL += '''order by storage_type_tag, measurement_type_index, dimension_order;
'''

            storage_types = database.submit_query(SQL)
            
            for record_dict in storage_types.record_generator():
                log_multiline(logger.debug, record_dict, 'record_dict', '\t')
                  
                storage_type_dict = db_dict['storage_types'].get(record_dict['storage_type_tag'])
                if storage_type_dict is None:
                    storage_type_dict = {'storage_type_tag': record_dict['storage_type_tag'],
                                           'storage_type_id': record_dict['storage_type_id'],
                                           'storage_type_name': record_dict['storage_type_name'],
                                           'measurement_types': {},
                                           'domains': {},
                                           'dimensions': {}
                                           }
    
                db_dict['storage_types'][record_dict['storage_type_tag']] = storage_type_dict
                      
                measurement_type_dict = storage_type_dict['measurement_types'].get(record_dict['measurement_type_tag'])
                if measurement_type_dict is None:
                    measurement_type_dict = {'measurement_type_tag': record_dict['measurement_type_tag'],
                                               'measurement_metatype_id': record_dict['measurement_metatype_id'],
                                               'measurement_type_id': record_dict['measurement_type_id'],
                                               'measurement_type_index': record_dict['measurement_type_index'],
                                               'measurement_metatype_name': record_dict['measurement_metatype_name'],
                                               'measurement_type_name': record_dict['measurement_type_name']
                                               }
    
                    storage_type_dict['measurement_types'][record_dict['measurement_type_tag']] = measurement_type_dict
                      
                domain_dict = storage_type_dict['domains'].get(record_dict['domain_tag'])
                if domain_dict is None:
                    domain_dict = {'domain_tag': record_dict['domain_tag'],
                                     'domain_id': record_dict['domain_id'],
                                     'domain_name': record_dict['domain_name'],
                                     'reference_system_id': record_dict['reference_system_id'],
                                     'reference_system_name': record_dict['reference_system_name'],
                                     'reference_system_definition': record_dict['reference_system_definition'],
                                     'reference_system_unit': record_dict['reference_system_unit'], 
                                     'dimensions': {}
                                     }
    
                    storage_type_dict['domains'][record_dict['domain_tag']] = domain_dict
                      
                dimension_dict = domain_dict['dimensions'].get(record_dict['dimension_tag'])
                if dimension_dict is None:
                    dimension_dict = {'dimension_tag': record_dict['dimension_tag'],
                                        'dimension_id': record_dict['dimension_id'],
                                        'dimension_order': record_dict['dimension_order'],
                                        'dimension_extent': record_dict['dimension_extent'],
                                        'dimension_elements': record_dict['dimension_elements'],
                                        'dimension_cache': record_dict['dimension_cache'],
                                        'dimension_origin': record_dict['dimension_origin'],
                                        'index_reference_system_id': record_dict['index_reference_system_id'],
                                        'index_reference_system_name': record_dict['index_reference_system_name'],
                                        'index_reference_system_definition': record_dict['index_reference_system_definition'],
                                        'index_reference_system_unit': record_dict['index_reference_system_unit']
                                        }
    
                    # Store a reference both under domains and storage_type
                    domain_dict['dimensions'][record_dict['dimension_tag']] = dimension_dict
                    storage_type_dict['dimensions'][record_dict['dimension_tag']] = dimension_dict
                      
                      
    #            log_multiline(logger.info, db_dict, 'db_dict', '\t')
            result_dict[database.db_ref] = db_dict
            # End of per-DB function

        return self.do_db_query(self.databases, [get_db_storage_config])
    
    def get_storage_units(self, dimension_range_dict, storage_type_tags=[], exclusive=False):
        '''
        Function to return all storage_units which fall in the specified dimensional ranges
        
        Parameter:
            dimension_range_dict: dict defined as {<dimension_tag>: (<min_value>, <max_value>), 
                                                   <dimension_tag>: (<min_value>, <max_value>)...}
            storage_type_tags: list of storage_type_tags to include in query
            exclusive: Boolean flag to indicate whether query should exclude storage_units with lower dimensionality than the specified range
                                                   
        Return Value:
            {<db_ref>: {<storage_type_tag>: {(<index1>, <index2>...<indexn>): <storage_info_dict>}}}
        '''
        def get_db_storage_units(dimension_range_dict, storage_type_tags, exclusive, database, result_dict):
            '''
            Function to return all storage_units which fall in the specified dimensional ranges
            
            Parameters:
                dimension_range_dict: dict defined as {<dimension_tag>: (<min_value>, <max_value>), 
                                                       <dimension_tag>: (<min_value>, <max_value>)...}
                storage_type_tags: list of storage_type_tags to include in query
                exclusive: Boolean flag to indicate whether query should exclude storage_units with lower dimensionality than the specified range
                database: gdf.database object against which to run the query
                result_dict: dict to contain the result
                                                                              
            Return Value:
                {<storage_type_tag>: {(<index1>, <index2>...<indexn>): <storage_info_dict>}}
                
                Sample <storage_info_dict> is as follows:
                    {'md5_checksum': None,
                     'storage_bytes': None,
                     'storage_id': 1409962010L,
                     'storage_location': '/storage_units/MODIS-Terra/MOD09/MODIS-Terra_MOD09_14_-4_2010.nc',
                     'storage_type_id': 100L,
                     'storage_version': 0,
                     't_index': 2010,
                     't_max': 1293840000.0,
                     't_min': 1262304000.0,
                     'x_index': 14,
                     'x_max': 150.0,
                     'x_min': 140.0,
                     'y_index': -4,
                     'y_max': -30.0,
                     'y_min': -40.0
                     }
            '''
            db_storage_dict = {}
            storage_type_dict = self._storage_config[database.db_ref]['storage_types']
            
            for storage_type in storage_type_dict.values():
                
                storage_type_tag = storage_type['storage_type_tag']
                logger.debug('storage_type_tag = %s', storage_type_tag)
                
                # Skip any storage_types if they are not in a specified list
                if storage_type_tags and (storage_type_tag not in storage_type_tags):
                    continue
                
                # list of dimension_tags for storage_type sorted by creation order
                storage_type_dimension_tags = [dimension['dimension_tag'] for dimension in sorted(storage_type['dimensions'].values(), key=lambda dimension: dimension['dimension_order'])]
                logger.debug('storage_type_dimension_tags = %s', storage_type_dimension_tags)
                # list of dimension_tags for range query sorted by creation order
                range_dimension_tags = [dimension_tag for dimension_tag in storage_type_dimension_tags if dimension_tag in dimension_range_dict.keys()]
                logger.debug('range_dimension_tags = %s', range_dimension_tags)
                
                # Skip this storage_type if exclusive flag set and dimensionality is less than query range dimnsionality
                if exclusive and (set(storage_type_dimension_tags) < set(range_dimension_tags)):
                    continue
                
                # Create a dict of storage_units keyed by indices for each storage_type
                storage_dict = {}
                
                SQL = '''-- Find storage_units which fall in range
select distinct'''
                for dimension_tag in storage_type_dimension_tags:
                    SQL +='''
%s.storage_dimension_index as %s_index,
%s.storage_dimension_min as %s_min,
%s.storage_dimension_max as %s_max,'''.replace('%s', dimension_tag)
                SQL +='''
storage.*
from storage
'''                    
                for dimension_tag in storage_type_dimension_tags:
                    SQL += '''join (
select *
from dimension 
join dimension_domain using(dimension_id)
join storage_dimension using(dimension_id, domain_id)
where storage_type_id = %d
and storage_version = 0
and dimension.dimension_tag = '%s'
''' % (storage_type['storage_type_id'], 
   dimension_tag
   )
                    # Apply range filters
                    if dimension_tag in range_dimension_tags:
                        SQL += '''and (storage_dimension_min < %f 
    and storage_dimension_max > %f)
''' % (dimension_range_dict[dimension_tag][1], # Max
   dimension_range_dict[dimension_tag][0] # Min
   )

                    SQL += ''') %s using(storage_type_id, storage_id, storage_version)
''' % (dimension_tag)

                SQL +='''
order by ''' + '_index, '.join(storage_type_dimension_tags) + '''_index;
'''
            
                log_multiline(logger.debug, SQL , 'SQL', '\t')
    
                storage_units = database.submit_query(SQL)
                
                for record_dict in storage_units.record_generator():
                    log_multiline(logger.debug, record_dict, 'record_dict', '\t')
                    storage_indices = tuple([record_dict[dimension_tag.lower() + '_index'] for dimension_tag in storage_type_dimension_tags])
    
                    storage_dict[storage_indices] = record_dict
                    
                if storage_dict:
                    db_storage_dict[storage_type_tag] = storage_dict
                                
                #log_multiline(logger.info, db_dict, 'db_dict', '\t')
            result_dict[database.db_ref] = db_storage_dict
            # End of per-DB function
        return self.do_db_query(self.databases, [get_db_storage_units, dimension_range_dict, storage_type_tags, exclusive])
    
    
    def solar_date(self, record_dict):
        '''
        Function which takes a record_dict containing all values from a query in the get_db_slices function 
        and returns a tuple containing the solar date of the observation and the storage_type_tag value
        '''
        # Assumes slice_index_value is time in seconds since epoch and x values are in degrees
        #TODO: Make more general (if possible)
        # Note: Solar time offset = average X ordinate in degrees converted to solar time offset in seconds 
        return (datetime.fromtimestamp(record_dict['slice_index_value'] + (record_dict['x_min'] + record_dict['x_max']) * 120).date(),
                record_dict['storage_type_tag'])
            
            
    def solar_year_month(self, record_dict):
        '''
        Function which takes a record_dict containing all values from a query in the get_db_slices function 
        and returns a (year, month, storage_type_tag) tuple from the solar date of the observation
        '''
        # Assumes slice_index_value is time in seconds since epoch and x values are in degrees
        #TODO: Make more general (if possible)
        # Note: Solar time offset = average X ordinate in degrees converted to solar time offset in seconds 
        solar_date_tuple = self.solar_date(record_dict)
        return (solar_date_tuple[0].year, 
                solar_date_tuple[0].month,
                solar_date_tuple[1])
    
            
    def solar_year(self, record_dict):
        '''
        Function which takes a record_dict containing all values from a query in the get_db_slices function 
        and returns a tuple containing the solar year of the observation and the storage_type_tag value
        '''
        # Assumes slice_index_value is time in seconds since epoch and x values are in degrees
        #TODO: Make more general (if possible)
        # Note: Solar time offset = average X ordinate in degrees converted to solar time offset in seconds 
        solar_date_tuple = self.solar_date(record_dict)
        return (solar_date_tuple[0].year,
                solar_date_tuple[1])
            
            
    def solar_month(self, record_dict):
        '''
        Function which takes a record_dict containing all values from a query in the get_db_slices function 
        and returns a tuple containing the solar month of the observation and the storage_type_tag value
        '''
        # Assumes slice_index_value is time in seconds since epoch and x values are in degrees
        #TODO: Make more general (if possible)
        # Note: Solar time offset = average X ordinate in degrees converted to solar time offset in seconds 
        solar_date_tuple = self.solar_date(record_dict)
        return (solar_date_tuple[0].month,
                solar_date_tuple[1])
            
            
    def get_slices(self, 
                   dimension_range_dict, 
                   storage_type_tags=[], 
                   exclusive=False,
                   slice_dimension='T',
                   slice_grouping_function=None):
        '''
        Function to return all slices which fall in the specified dimensional ranges
        
        Parameter:
            dimension_range_dict: dict defined as {<dimension_tag>: (<min_value>, <max_value>), 
                                                   <dimension_tag>: (<min_value>, <max_value>)...}
            slice_dimension: Dimension along which to group results
            slice_locality: Range (in slice_dimension units) in which to group slices
            storage_type_tags: list of storage_type_tags to include in query
            exclusive: Boolean flag to indicate whether query should exclude storage_units with lower dimensionality than the specified range
                                                   
        Return Value:
            TODO: Make this correct
            {<db_ref>: {<storage_type_tag>: {(<index1>, <index2>...<indexn>): <storage_info_dict>}}}
        '''
        
        def get_db_slices(dimension_range_dict, 
                          slice_dimension,
                          slice_grouping_function, 
                          storage_type_tags, 
                          exclusive, 
                          database, 
                          result_dict):
            '''
            Function to return all slices in storage_units which fall in the specified dimensional ranges
            
            Parameters:
                dimension_range_dict: dict defined as {<dimension_tag>: (<min_value>, <max_value>), 
                                                       <dimension_tag>: (<min_value>, <max_value>)...}
                slice_dimension: Dimension along which to group results
                slice_locality: Range (in slice_dimension units) in which to group slices
                storage_type_tags: list of storage_type_tags to include in query
                exclusive: Boolean flag to indicate whether query should exclude storage_units with lower dimensionality than the specified range
                database: gdf.database object against which to run the query
                result_dict: dict to contain the result
                                                                              
            Return Value:
                {<storage_type_tag>: {(<index1>, <index2>...<indexn>): <storage_info_dict>}}
                
                Sample <storage_info_dict> is as follows:
                    {'md5_checksum': None,
                     'storage_bytes': None,
                     'storage_id': 1409962010L,
                     'storage_location': '/storage_units/MODIS-Terra/MOD09/MODIS-Terra_MOD09_14_-4_2010.nc',
                     'storage_type_id': 100L,
                     'storage_version': 0,
                     't_index': 2010,
                     't_max': 1293840000.0,
                     't_min': 1262304000.0,
                     'x_index': 14,
                     'x_max': 150.0,
                     'x_min': 140.0,
                     'y_index': -4,
                     'y_max': -30.0,
                     'y_min': -40.0
                     }
            '''
            db_slice_dict = {}
            storage_type_dict = self._storage_config[database.db_ref]['storage_types']
            
            for storage_type in storage_type_dict.values():
                
                storage_type_tag = storage_type['storage_type_tag']
                logger.debug('storage_type_tag = %s', storage_type_tag)
            
                
                # Skip any storage_types if they are not in a specified list
                if storage_type_tags and (storage_type_tag not in storage_type_tags):
                    continue
                
                # list of dimension_tags for storage_type sorted by creation order
                storage_type_dimension_tags = [dimension['dimension_tag'] for dimension in sorted(storage_type['dimensions'].values(), key=lambda dimension: dimension['dimension_order'])]
                logger.debug('storage_type_dimension_tags = %s', storage_type_dimension_tags)
                # list of dimension_tags for range query sorted by creation order
                range_dimension_tags = [dimension_tag for dimension_tag in storage_type_dimension_tags if dimension_tag in dimension_range_dict.keys()]
                logger.debug('range_dimension_tags = %s', range_dimension_tags)
                
                # Skip this storage_type if exclusive flag set and dimensionality is less than query range dimnsionality
                if exclusive and (set(storage_type_dimension_tags) < set(range_dimension_tags)):
                    continue
                
                # Create a dict of storage units keyed by indices for each storage_type
                storage_dict = {}
                
                SQL = '''-- Find storage_units which fall in range
select distinct'''
                for dimension_tag in storage_type_dimension_tags:
                    SQL +='''
%s.storage_dimension_index as %s_index,
%s.storage_dimension_min as %s_min,
%s.storage_dimension_max as %s_max,'''.replace('%s', dimension_tag)
                SQL +='''
storage.*,
dataset_index.*
from storage
'''                    
                for dimension_tag in storage_type_dimension_tags:
                    SQL += '''join (
select *
from dimension 
join dimension_domain using(dimension_id)
join storage_dimension using(dimension_id, domain_id)
where storage_type_id = %d
and storage_version = 0
and dimension.dimension_tag = '%s'
''' % (storage_type['storage_type_id'], 
   dimension_tag
   )
                    # Apply range filters
                    if dimension_tag in range_dimension_tags:
                        SQL += '''and (storage_dimension_min < %f 
    and storage_dimension_max > %f)
''' % (dimension_range_dict[dimension_tag][1], # Max
   dimension_range_dict[dimension_tag][0] # Min
   )

                    SQL += ''') %s using(storage_type_id, storage_id, storage_version)
''' % (dimension_tag)

                SQL +='''
    join storage_dataset using (storage_type_id, storage_id, storage_version)
    join (
      select coalesce(indexing_value, (min_value+max_value)/2) as slice_index_value,
      *
      from dataset 
      join dataset_dimension using(dataset_type_id, dataset_id)
      join dimension using(dimension_id)
      where dimension_tag = '%s'
    ) dataset_index using(dataset_type_id, dataset_id)
''' % (slice_dimension)

                SQL +='''
order by ''' + '_index, '.join(storage_type_dimension_tags) + '''_index, slice_index_value;
'''            
                log_multiline(logger.debug, SQL , 'SQL', '\t')
    
                storage_units = database.submit_query(SQL)
                
                last_storage_id = -1 # Initial impossible value
                storage_slice_index = 0
                for record_dict in storage_units.record_generator():
                    
                    # Determine position of slice in storage
                    if record_dict['storage_id'] == last_storage_id:
                        storage_slice_index += 1
                    else:
                        storage_slice_index = 0
                        last_storage_id = record_dict['storage_id']
                        
                    # Don't add this slice to the result dict if a range is set for the slicing dimension and it's outside that range
                    if (slice_dimension in dimension_range_dict.keys() and
                        (record_dict['slice_index_value'] < dimension_range_dict[slice_dimension][0] or
                         record_dict['slice_index_value'] > dimension_range_dict[slice_dimension][1]
                         )
                        ):
                        continue
                    
                    record_dict.update({'storage_type_tag': storage_type_tag})
                    record_dict.update({'slice_index': storage_slice_index})
                    record_dict.update({'slice_group': slice_grouping_function(record_dict)})
                                        
#                    log_multiline(logger.debug, record_dict, 'record_dict', '\t')
                    storage_indices = tuple([record_dict[dimension_tag.lower() + '_index'] for dimension_tag in storage_type_dimension_tags])
                    storage_dict[(tuple(storage_indices), storage_slice_index)] = record_dict
                    
                if storage_dict:
                    db_slice_dict[storage_type_tag] = storage_dict
                                
                #log_multiline(logger.info, db_dict, 'db_dict', '\t')
            result_dict[database.db_ref] = db_slice_dict
            # End of per-DB function
            
        slice_grouping_function = slice_grouping_function or self.solar_date
        
        return self.do_db_query(self.databases, [get_db_slices, 
                                                         dimension_range_dict, 
                                                         slice_dimension, 
                                                         slice_grouping_function, 
                                                         storage_type_tags, 
                                                         exclusive
                                                         ]
                                        )
        
    def get_grouped_slices_from_dict(self, interim_dict):
        '''
        Function to take output dict from get_slices function and re-arrange it into a dict keyed by slice_group
        '''
        result_dict = {}
        for db_ref in interim_dict.keys():
            for storage_type_tag in interim_dict[db_ref].keys():
                for slice_ref in interim_dict[db_ref][storage_type_tag].keys():
                    slice_dict = interim_dict[db_ref][storage_type_tag][slice_ref]
                    
                    # Add extra information 
                    slice_dict['db_ref'] = db_ref
                    slice_dict['storage_type_tag'] = storage_type_tag
                    
                    slice_group = slice_dict['slice_group']
                    slice_list = result_dict.get(slice_group)
                    if not slice_list:
                        slice_list = []
                        result_dict[slice_group] = slice_list
                    slice_list.append(slice_dict)
                    
        return result_dict
    
    def get_grouped_slices(self, 
                   dimension_range_dict, 
                   storage_type_tags=[], 
                   exclusive=False,
                   slice_dimension='T',
                   slice_grouping_function=None):
        '''
        Function to return a dict keyed by slice_group containing lists of slice info dicts
        '''
        
        interim_dict = self.get_slices(dimension_range_dict, 
                   storage_type_tags, 
                   exclusive,
                   slice_dimension,
                   slice_grouping_function)
        
        return self.get_grouped_slices_from_dict(interim_dict)
        
    
    # Define properties for GDF class
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
    
    def get_descriptor(self, query_parameter):
        '''
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
                   'grouping_function': '<e.g. gdf.solar_day>'
                   }
             },
        'polygon': '<some kind of text representation of a polygon for PostGIS to sort out>' # We won't be doing this in the pilot
        }
        '''
        
        # Create a dummy array of days since 1/1/1970 between min and max timestamps
        min_date_ordinal = date.fromtimestamp(1293840000).toordinal()
        max_date_ordinal = date.fromtimestamp(1325376000).toordinal()
        epoch_date_ordinal = date(1970, 1, 1).toordinal()
        date_array = np.array(range(min_date_ordinal - epoch_date_ordinal, max_date_ordinal - epoch_date_ordinal), dtype=np.int16)
        
        descriptor = {
            'LS5TM': { # storage_type identifier
                 'dimensions': ['x', 'y', 't'],
                 'variables': { # These will be the variables which can be accessed as arrays
                       'B10': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B20': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B30': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B40': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B50': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'B70': {
                            'datatype': 'int16',
                            'nodata_value': -999
                            },
                       'PQ': { # There is no reason why we can't put PQ in with NBAR if we want to
                            'datatype': 'int16'
                            }
                       },
                 'result_min': (140, -36, 1293840000),
                 'result_max': (141, -35, 1325376000),
                 'overlap': (0, 0, 0), # We won't be doing this in the pilot
                 'buffer_size': (128, 128, 128), # Chunk size to use
                 'result_shape': (8000, 8000, 40), # Overall size of result set
                 'irregular_indices': { # Regularly indexed dimensions (e.g. x & y) won't need to be specified, but we could also do that here if we wanted to
                       't': date_array # Array of days since 1/1/1970
                       },
                 'storage_units': { # Should wind up with 8 for the 2x2x2 query above
                       (140, -36, 2010): { # Storage unit indices
                            'storage_min': (140, -36, 1293840000),
                            'storage_max': (141, -35, 1293800400),
                            'storage_shape': (4000, 4000, 24)
                            },
                       (140, -36, 2011): { # Storage unit indices
                            'storage_min': (140, -36, 1293800400),
                            'storage_max': (141, -35, 1325376000),
                            'storage_shape': (4000, 4000, 23)
                            },
                       (140, -35, 2011): { # Storage unit indices
                            'storage_min': (140, -36, 1293840000),
                            'storage_max': (141, -35, 1293800400),
                            'storage_shape': (4000, 4000, 20)
                            }
            #          ...
            #          <more storage_unit sub-descriptors>
            #          ...
                       }
            #    ...
            #    <more storage unit type sub-descriptors>
            #    ...
                 }
            }

        return descriptor