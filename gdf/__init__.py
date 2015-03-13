import os
import sys

from _database import Database, CachedResultSet
from _arguments import CommandLineArgs
from _config_file import ConfigFile
import logging

from EOtools.utils import log_multiline

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
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
        config_dict = {'command_line': command_line_args_object.arguments}
        
        try:
            self._config_file = command_line_args_object.arguments['config_file']
        except:
            self._config_file = os.path.join(self._code_root, GDF.DEFAULT_CONFIG_FILE)

            
        config_file_object = ConfigFile(self._config_file)
        
        # Copy all configuration sections from config file to config dict
        config_dict['config_file'] = config_file_object.configuration
        
        return config_dict
    
    def get_dbs(self):
        config_dict = self._configuration['config_file']
        
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
        
    def __init__(self):
        '''Constructor for class GDF
        '''
        self._code_root = os.path.dirname(__file__)
        
        # Create master configuration dict
        self._configuration = self.get_config()
                
        # Create master database dict
        self._databases = self.get_dbs()
        
    
    # Define properties for GDF class
    @property
    def code_root(self):
        return self._code_root
    
    @property
    def config_file(self):
        return self._config_file
    
    @property
    def configuration(self):
        return self._configuration
    
    @property
    def databases(self):
        return self._databases
        