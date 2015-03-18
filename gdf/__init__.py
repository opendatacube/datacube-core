import os
import sys

from _database import Database, CachedResultSet
from _arguments import CommandLineArgs
from _config_file import ConfigFile
import logging

from EOtools.utils import log_multiline

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)

class GDF(object):
    '''
    Class definition for GDF (General Data Framework).
    Manages configuration and database connections.
    '''
    DEFAULT_CONFIG_FILE = 'gdf_default.conf' # N.B: Assumed to reside in code root directory
    
    def get_config(self):
        command_line_args_object = CommandLineArgs()
        
        # Copy command line values to config dict
        config_dict = {'command_line': command_line_args_object.arguments,
                       'configurations' : {}
                       }
        
        # Use default config file if none provided
        config_files_string = command_line_args_object.arguments['config_files'] or os.path.join(self._code_root, GDF.DEFAULT_CONFIG_FILE)
        
        # Set list of absolute config file paths from comma-delimited list
        self._config_files = [os.path.abspath(config_file) for config_file in config_files_string.split(',')] 
        log_multiline(logger.debug, self._config_files, 'self._config_files', '\t')
           
        for config_file in self._config_files:
            config_file_object = ConfigFile(config_file)
        
            # Merge all configuration sections from individual config files to config dict
            config_dict['configurations'].update(config_file_object.configuration)
        
        log_multiline(logger.debug, config_dict, 'config_dict', '\t')
        return config_dict
    
    def get_dbs(self):
        config_dict = self._configuration['configurations']
        
        database_dict = {}
        
        # Create a database connection for every valid configuration
        for section_name in sorted(config_dict.keys()):
            section_dict = config_dict[section_name]
            try:
                host = section_dict['host']
                port = section_dict['port']
                dbname = section_dict['dbname']
                user = section_dict['user']
                password = section_dict['password']
                
                database = Database(host=host, 
                                    port=port, 
                                    dbname=dbname, 
                                    user=user, 
                                    password=password, 
                                    keep_connection=False, # Assume we don't want connections hanging around
                                    autocommit=True)
                
                database.submit_query('select 1 as test_field') # Test DB connection
                
                database_dict[section_name] = database
            except Exception, e:
                logger.info('Unable to connect to database for %s: %s', section_name, e.message)

        log_multiline(logger.debug, database_dict, 'database_dict', '\t')
        return database_dict
        

    def __init__(self):
        '''Constructor for class GDF
        '''
        self._config_files = [] # List of config files read
        
        self._code_root = os.path.abspath(os.path.dirname(__file__)) # Directory containing module code
        
        # Create master configuration dict containing both command line and config_file parameters
        self._configuration = self.get_config()
                
        # Create master database dict
        self._databases = self.get_dbs()
        
        log_multiline(logger.debug, self.__dict__, 'GDF.__dict__', '\t')
        
        
    def get_ndarray_types(self, databases={}):
        '''Function to return a dict with details of all dimensions managed in databases keyed as follows:
        
           <db_name>
               'ndarray_types'
                    <ndarray_type_tag>
                        'measurement_types'
                           <measurement_type_tag>
                        'domains'
                            <domain_name>
                                'dimensions'
                                    <dimension_tag>
                   
        '''
        
        def get_db_data(db_name, databases):
            db_dict = {'ndarray_types': {}}
            database = databases[db_name]
            db_config_dict = self._configuration['configurations'][db_name]
            
            ndarray_types = database.submit_query('''-- Query to return all ndarray_type configuration info
select * from ndarray_type 
join ndarray_type_measurement_type using(ndarray_type_id)
join measurement_type using(measurement_metatype_id, measurement_type_id)
join measurement_metatype using(measurement_metatype_id)
join ndarray_type_dimension using(ndarray_type_id)
join dimension_domain using(dimension_id, domain_id)
join domain using(domain_id)
join dimension using(dimension_id)
join indexing_type using(indexing_type_id)
join reference_system using (reference_system_id)
order by ndarray_type_tag, measurement_type_index, creation_order;
''')
            for record_dict in ndarray_types.record_generator():
                ndarray_type_dict = db_dict['ndarray_types'].get(record_dict['ndarray_type_tag'])
                if ndarray_type_dict is None:
                    ndarray_type_dict = {'ndarray_type_name': record_dict['ndarray_type_name'],
                                         'measurement_types': {},
                                         'domains': {}
                                         }

                    db_dict['ndarray_types'][record_dict['ndarray_type_tag']] = ndarray_type_dict
                    
                measurement_type_dict = ndarray_type_dict['measurement_types'].get(record_dict['measurement_type_tag'])
                if measurement_type_dict is None:
                    measurement_type_dict = {'measurement_metatype': record_dict['measurement_metatype_name'],
                                             'measurement_type_name': record_dict['measurement_type_name']
                                             }

                    ndarray_type_dict['measurement_types'][record_dict['measurement_type_tag']] = measurement_type_dict
                    
                #TODO: Should have a domain_tag field
                domain_dict = ndarray_type_dict['domains'].get(record_dict['domain_name'])
                if domain_dict is None:
                    domain_dict = {'reference_system_name': record_dict['reference_system_name'],
                                   'reference_system_definition': record_dict['reference_system_definition'],
                                   'reference_system_unit': record_dict['reference_system_unit'], 
                                   'dimensions': {}
                                   }

                    ndarray_type_dict['domains'][record_dict['domain_name']] = domain_dict
                    
                dimension_dict = domain_dict['dimensions'].get(record_dict['dimension_tag'])
                if dimension_dict is None:
                    dimension_dict = {'creation_order': record_dict['creation_order'],
                                      'dimension_extent': record_dict['dimension_extent'],
                                      'dimension_elements': record_dict['dimension_elements'],
                                      'dimension_cache': record_dict['dimension_cache'],
                                      'dimension_origin': record_dict['dimension_origin'],
                                      'dimension_extent_unit': record_dict['dimension_extent_unit'] or record_dict['reference_system_unit']
                                      }

                    domain_dict['dimensions'][record_dict['dimension_tag']] = dimension_dict
                    
            return db_dict
        
        databases = databases or self._databases
        
        result_dict = {} # Nested dict containing ndarray_type details for all databases

        #TODO: Multi-thread this section
        for db_name in sorted(databases.keys()):
            result_dict.update(get_db_data(db_name, databases))
            
        return result_dict
                    
                
    
    def get_dimensions(self, databases={}):
        '''Function to return a dict with details of all dimensions managed in databases
        '''
        databases = databases or self._databases
        
        for db_name in sorted(databases.keys()):
            database = databases[db_name]
            db_config_dict = self._configuration['configurations'][db_name]

            
        
    
    # Define properties for GDF class
    @property
    def code_root(self):
        return self._code_root
    
    @property
    def config_files(self):
        return self._config_files
    
    @property
    def configuration(self):
        return self._configuration
    
    @property
    def databases(self):
        return self._databases
        
