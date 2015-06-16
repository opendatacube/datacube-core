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
Utility for converting AGDC
Created on Jun 5, 2015

@author: Alex Ip
'''

import os
import sys
import threading
import traceback
import numpy as np
from datetime import datetime, date, timedelta
from math import floor
import pytz
import calendar
import collections
import numexpr
import logging
import errno
import shutil
from osgeo import gdal

import gdf
from gdf import Database
from gdf import CachedResultSet
from gdf import CommandLineArgs
from gdf import ConfigFile
from gdf import GDF
from gdf import GDFNetCDF
from gdf import dt2secs
from gdf import make_dir

from EOtools.utils import log_multiline

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Logging level for this module

class AGDC2GDF(GDF):
    DEFAULT_CONFIG_FILE = 'agdc2gdf_default.conf' # N.B: Assumed to reside in code root directory
    ARG_DESCRIPTORS = {'xmin': {'short_flag': '-x1', 
                                        'long_flag': '--xmin', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'Minimum X inclusive t (longitude) of spatial range to process'
                                        },
                                'xmax': {'short_flag': '-x2', 
                                        'long_flag': '--xmax', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'Maximum X inclusive t (longitude) of spatial range to process'
                                        },
                                'ymin': {'short_flag': '-y1', 
                                        'long_flag': '--ymin', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'Minimum Y inclusive t (latitude) of spatial range to process'
                                        },
                                'ymax': {'short_flag': '-y2', 
                                        'long_flag': '--ymax', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'Maximum Y inclusive t (latitude) of spatial range to process'
                                        },
                                'tmin': {'short_flag': '-t1', 
                                        'long_flag': '--tmin', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'Minimum t inclusive t (years) of spatial range to process'
                                        },
                                'tmax': {'short_flag': '-t2', 
                                        'long_flag': '--tmax', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'Maximum inclusive t (years) of spatial range to process'
                                        },
                                'storage_type': {'short_flag': '-st', 
                                        'long_flag': '--storage', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'GDF storage type to populate'
                                        },
                                'satellite': {'short_flag': '-sa', 
                                        'long_flag': '--satellite', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'AGDC satellite to process'
                                        },
                                'sensor': {'short_flag': '-se', 
                                        'long_flag': '--sensor', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'AGDC sensor to process'
                                        },
                                'level': {'short_flag': '-l', 
                                        'long_flag': '--level', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'AGDC processing level to process'
                                        },
                                'storage_root': {'short_flag': '-r', 
                                        'long_flag': '--root', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'Root directory of GDF filestore'
                                        },
                                'temp_dir': {'short_flag': '-t', 
                                        'long_flag': '--temp', 
                                        'default': None, 
                                        'action': 'store',
                                        'const': None, 
                                        'help': 'Temporary directory for AGDC2GDF operation'
                                        },
                                'force': {'short_flag': '-f', 
                                        'long_flag': '--force', 
                                        'default': False, 
                                        'action': 'store_const', 
                                        'const': True,
                                        'help': 'Flag to force replacement of existing files'
                                        },
                                }
    
    def __init__(self):
        '''Constructor for class AGDC2GDF
        '''
        self._code_root = os.path.abspath(os.path.dirname(__file__)) # Directory containing module code
        self._gdf_root = os.path.abspath(os.path.dirname(gdf.__file__)) # Directory containing module code
        
        # Create master configuration dict containing both command line and config_file parameters
        self._command_line_params = self.get_command_line_params(AGDC2GDF.ARG_DESCRIPTORS)
        
        if self._command_line_params['debug']:
            self.debug = True                

        agdc2gdf_config_file = self._command_line_params['config_files'] or os.path.join(self._code_root, AGDC2GDF.DEFAULT_CONFIG_FILE)
        
        agdc2gdf_config_file_object = ConfigFile(agdc2gdf_config_file)
        
        # Comma separated list of GDF config files specified in master config file
        gdf_config_files_string = agdc2gdf_config_file_object.configuration['gdf'].get('config_files') or os.path.join(self._gdf_root, GDF.DEFAULT_CONFIG_FILE)
        
        # Create master GDF configuration dict containing both command line and config_file parameters
        self._configuration = self.get_config(gdf_config_files_string)
                
        # Create master GDF database dict
        self._databases = self.get_dbs()
        
        self.force = self._command_line_params.get('force') or agdc2gdf_config_file_object.configuration['agdc2gdf'].get('force')
        
        self.storage_type = self._command_line_params.get('storage_type') or agdc2gdf_config_file_object.configuration['gdf']['storage_type']

        self.agdc_satellite = self._command_line_params.get('satellite') or agdc2gdf_config_file_object.configuration['agdc']['satellite']
        self.agdc_sensor = self._command_line_params.get('sensor') or agdc2gdf_config_file_object.configuration['agdc']['sensor']
        self.agdc_level = self._command_line_params.get('level') or agdc2gdf_config_file_object.configuration['agdc']['level']
        
        #=======================================================================
        # # Read GDF storage configuration from databases but only keep the one for the specified storage type - not interested in anything else
        # self._storage_config = dict([storage_tuple for storage_tuple in self.get_storage_config().items() if storage_tuple[0] == self.storage_type])
        #=======================================================================
        
        # Read GDF storage configuration from databases
        self._storage_config = self.get_storage_config()
        self.storage_type_config = self._storage_config[self.storage_type]
        self.database = self._databases[self.storage_type_config['db_ref']]
        
        self.storage_root = self._command_line_params.get('storage_root') or self.storage_type_config['storage_root']
        
        self.dimensions = self.storage_type_config['dimensions'] # This is used a lot
                
        
        # Set up AGDC stuff now
        agdc_config_dict = gdf_config_files_string = agdc2gdf_config_file_object.configuration['agdc']
        try:
            db_ref = agdc_config_dict['db_ref']
            host = agdc_config_dict['host']
            port = agdc_config_dict['port']
            dbname = agdc_config_dict['dbname']
            user = agdc_config_dict['user']
            password = agdc_config_dict['password']
            
            self.agdc_db = Database(db_ref=db_ref,
                                host=host, 
                                port=port, 
                                dbname=dbname, 
                                user=user, 
                                password=password, 
                                keep_connection=False, # Assume we don't want connections hanging around
                                autocommit=True)
            
            self.agdc_db.submit_query('select 1 as test_field') # Test DB connection
        except Exception, e:
            logger.error('Unable to connect to database for %s: %s', db_ref, e.message)
            raise e
        
       
        # Set self.range_dict from either command line or config file values
        self.range_dict = {}
        for dimension in self.storage_type_config['dimensions']:
            min_value = int(self._command_line_params['%smin' % dimension.lower()] or agdc2gdf_config_file_object.configuration['agdc2gdf']['%smin' % dimension.lower()])
            max_value = int(self._command_line_params['%smax' % dimension.lower()] or agdc2gdf_config_file_object.configuration['agdc2gdf']['%smax' % dimension.lower()])
            self.range_dict[dimension] = (min_value, max_value)

        
        log_multiline(logger.debug, self.__dict__, 'AGDC2GDF.__dict__', '\t')        

    def read_agdc(self, storage_indices):        
        
        SQL = '''-- Query to select all tiles in range with required dataset info
select *
from tile
join dataset using(dataset_id)
join acquisition using(acquisition_id)
join satellite using(satellite_id)
join sensor using(satellite_id, sensor_id)
join processing_level using(level_id)
where tile_class_id in (1,4) --Non-overlapped & mosaics
and x_index = %(x_index)s
and y_index = %(y_index)s
and end_datetime between %(start_datetime)s and %(end_datetime)s
and satellite_tag = %(satellite)s
and sensor_name = %(sensor)s
and level_name = %(level)s
order by end_datetime
'''     
        #TODO: Make this more general
        index_reference_system_name = self.storage_type_config['dimensions']['T']['index_reference_system_name'].lower()
        t_index = storage_indices[self.dimensions.keys().index('T')]
        if index_reference_system_name == 'decade':
            start_datetime = datetime(t_index*10, 1, 1)
            end_datetime = datetime(t_index*10 + 10, 1, 1) - timedelta(microseconds=1)
        if index_reference_system_name == 'year':
            start_datetime = datetime(t_index, 1, 1)
            end_datetime = datetime(t_index + 1, 1, 1) - timedelta(microseconds=1)
        elif index_reference_system_name == 'month':
            start_datetime = datetime(t_index // 12, t_index % 12, 1)
            end_datetime = datetime(t_index // 12, t_index % 12, 1) - timedelta(microseconds=1)
        
        params={'x_index': storage_indices[self.dimensions.keys().index('X')],
                'y_index': storage_indices[self.dimensions.keys().index('Y')],
                'start_datetime': start_datetime,
                'end_datetime': end_datetime,
                'satellite': self.agdc_satellite,
                'sensor': self.agdc_sensor, 
                'level': self.agdc_level,
                }
        
        log_multiline(logger.debug, SQL, 'SQL', '\t')
        log_multiline(logger.debug, params, 'params', '\t')
        
        tile_result_set = self.agdc_db.submit_query(SQL, params)

        # Return descriptor - this shouldn't be too big for one storage unit
        return [record for record in tile_result_set.record_generator()]
    
    def create_netcdf(self, storage_indices, data_descriptor):
        '''
        Function to create netCDF-CF file for specified storage indices
        '''
        temp_storage_path = self.get_temp_storage_path(self.storage_type, storage_indices)
        storage_path = self.get_storage_path(self.storage_type, storage_indices)
        
        if os.path.isfile(storage_path):
            if not self.force: 
                logger.warning('Skipping existing storage unit %s' % storage_path)
                return storage_path #TODO: Make this return nothing. 
        
        t_indices = np.array([dt2secs(record_dict['end_datetime']) for record_dict in data_descriptor])
        
        gdfnetcdf = GDFNetCDF(storage_config=self.storage_config[self.storage_type])
        
        logger.debug('Creating temporary storage unit %s', temp_storage_path)
        gdfnetcdf.create(netcdf_filename=temp_storage_path, 
                         index_tuple=storage_indices, 
                         dimension_index_dict={'T': t_indices}, netcdf_format=None)
        del t_indices
        
        # Set georeferencing from first tile
        gdfnetcdf.georeference_from_file(data_descriptor[0]['tile_pathname'])

        variable_dict = self.storage_config[self.storage_type]['measurement_types']
        variable_names = variable_dict.keys()
                
        slice_index = 0
        for record_dict in data_descriptor:
            tile_dataset = gdal.Open(record_dict['tile_pathname'])
            assert tile_dataset, 'Failed to open tile file %s' % record_dict['tile_pathname']
            
            logger.debug('Reading array data from tile file %s', record_dict['tile_pathname'])
            data_array = tile_dataset.ReadAsArray()
            
            for variable_index in range(len(variable_dict)):
                variable_name = variable_names[variable_index]
                logger.debug('Writing array to variable %s', variable_name)
                gdfnetcdf.write_slice(variable_name, data_array[variable_index], {'T': slice_index})

            slice_index += 1
                 
        del gdfnetcdf # Close the netCDF
        
        logger.debug('Moving temporary storage unit %s to %s', temp_storage_path, storage_path)
        if os.path.isfile(storage_path):
            logger.debug('Removing existing storage unit %s' % storage_path)
            os.remove(storage_path)
        shutil.move(temp_storage_path, storage_path)
        
        return storage_path
    
    def write_gdf_data(self, storage_indices, data_descriptor, storage_unit_path):
        '''
        Function to write records to database. Must occur in a single transaction
        '''

        def get_storage_id(record, storage_unit_path):
            '''
            Function to write storage unit record if required and return storage unit ID
            '''
            SQL ='''-- Attempt to insert a storage record and return storage_id
insert into storage(
    storage_type_id,
    storage_id,
    storage_version,
    storage_location,
    md5_checksum,
    storage_bytes,
    spatial_footprint_id
    )  
values(
    %(storage_type_id)s,
    nextval('storage_id_seq'::regclass),
    0, -- storage_version
    %(storage_location)s,
    NULL,
    NULL,
    NULL
    )
where not exists (
    select storage_id from storage 
    where storage_type_id =%(storage_type_id)s
    and storage_location = %(storage_location)s
    );
            
select storage_id from storage
where storage_type_id =%(storage_type_id)s
and storage_location = %(storage_location)s;
'''            
            params = {'storage_type_id': self.storage_type_config['storage_type_id'],
                      'storage_location': storage_unit_path
                      }
            
            log_multiline(logger.debug, self.database.default_cursor.mogrify(SQL, params), 'Mogrified SQL', '\t')
            
            storage_id_result = self.database.submit_query(SQL, params)
            assert storage_id_result.record_count == 1, '%d records retrieved for storage_id query'
            return storage_id_result['storage_id'][0]
            
        def get_observation_id(record):
            '''
            Function to write observation (acquisition) record if required and return observation ID
            '''
            SQL = '''Attempt to insert an observation record and return observation_id
insert into observation(
    observation_type_id,
    observation_id,
    observation_start_datetime,
    observation_end_datetime,
    instrument_type_id,
    instrument_id
    )
values (
    1, -- Optical Satellite
    nextval('observation_id_seq'::regclass),
    %(observation_start_datetime)s,
    %(observation_end_datetime)s,
    1, -- Passive Satellite-borne
    (select instrument_id from instrument where instrument_tag = %(instrument_tag)s
    )
where not exists (
    select observation_id from observation
    where observation_type_id = 1 -- Optical Satellite
    and instrument_type_id = 1 -- Passive Satellite-borne
    and instrument_id = (select instrument_id from instrument where instrument_tag = %(instrument_tag)s
    and observation_start_datetime = %(observation_start_datetime)s
    and observation_end_datetime = %(observation_end_datetime)s
    );

select observation_id from observation
where observation_type_id = 1 -- Optical Satellite
and instrument_type_id = 1 -- Passive Satellite-borne
and instrument_id = (select instrument_id from instrument where instrument_tag = %(instrument_tag)s
and observation_start_datetime = %(observation_start_datetime)s
and observation_end_datetime = %(observation_end_datetime)s;
'''
            params = {'instrument_tag': record['sensor_name'],
                      'observation_start_datetime': record['start_datetime'],
                      'observation_end_datetime': record['end_datetime']
                      }
            
            observation_id_result = self.database.submit_query(SQL, params)
            assert observation_id_result.record_count == 1, '%d records retrieved for observation_id query'
            return observation_id_result['observation_id'][0]
           
        
        def get_dataset_id(record, observation_id):
            '''
            Function to write observation (acquisition) record if required and return observation ID
            '''
            SQL = '''Attempt to insert a dataset record and return dataset_id
insert into dataset(
    dataset_type_id,
    dataset_id,
    observation_type_id,
    observation_id,
    dataset_location
    )
values (
    (select dataset_type_id from dataset_type where dataset_type_tag = %(dataset_type_tag)s),
    nextval('dataset_id_seq'::regclass),
    1, -- Optical Satellite
    %(observation_id)s,
    %(dataset_location)s
    )
where not exists (
    select dataset_id from dataset
    where dataset_type_id = (select dataset_type_id from dataset_type where dataset_type_tag = %(dataset_type_tag)s)
    and dataset_location = %(dataset_location)s
    );

select dataset_id from dataset
where dataset_type_id = (select dataset_type_id from dataset_type where dataset_type_tag = %(dataset_type_tag)s)
and dataset_location = %(dataset_location)s
'''
            params = {'dataset_type_tag': record['level_name'],
                      'observation_id': observation_id,
                      'dataset_location': record['dataset_path']
                      }
            
            observation_id_result = self.database.submit_query(SQL, params)
            assert observation_id_result.record_count == 1, '%d records retrieved for observation_id query'
            return observation_id_result['observation_id'][0]
        
        
        # Start of write_gdf_data(self, storage_indices, data_descriptor, storage_unit_path) definition
        assert os.path.isfile(storage_unit_path), 'Storage unit file does not exist'
        
        self.database.keep_connection = True
        self.database.autocommit = False
        self.database.execSQL('begin;') # Begin transaction
        
        try:
            storage_type_id = self.storage_type_config['storage_type_id']
            for record in data_descriptor:
                storage_id = get_storage_id(record, storage_unit_path)
                observation_id = get_observation_id(record)
                dataset_id = get_dataset_id(record, observation_id)
                
                
                
                
                
            self.database.execSQL('commit;') # Commit transaction    
        except:
            try:
                self.database.execSQL('rollback;') # Rollback transaction
            except:
                pass 
        finally:
            self.database.autocommit = True
            self.database.keep_connection = False
            
    
    def ordinate2index(self, ordinate, dimension):
        '''
        Return the storage unit index from the reference system ordinate for the specified storage type, ordinate value and dimension tag
        '''
        return int((ordinate - self.storage_type_config['dimensions'][dimension]['dimension_origin']) / 
                   self.storage_type_config['dimensions'][dimension]['dimension_extent'])
        

    def index2ordinate(self, index, dimension):
        '''
        Return the reference system ordinate from the storage unit index for the specified storage type, index value and dimension tag
        '''
        if dimension == 'T':
            #TODO: Make this more general - need to cater for other reference systems besides seconds since epoch
            index_reference_system_name = self.storage_type_config['dimensions']['T']['index_reference_system_name'].lower()
            if index_reference_system_name == 'decade':
                return gdf.dt2secs(datetime(index*10, 1, 1))
            if index_reference_system_name == 'year':
                return gdf.dt2secs(datetime(index, 1, 1))
            elif index_reference_system_name == 'month':
                return gdf.dt2secs(datetime(index // 12, index % 12, 1))
        else: # Not time   
            return ((index * self.storage_type_config['dimensions'][dimension]['dimension_extent']) + 
                    self.storage_type_config['dimensions'][dimension]['dimension_origin'])
        


def main(): 
    agdc2gdf = AGDC2GDF()
#    storage_config = agdc2gdf.storage_config[agdc2gdf.storage_type]
    
    # Create list of storage unit indices from CRS ranges
    #TODO - Find some way of not hard coding the dimensions
    storage_indices_list = [(t, x, y) 
                            for t in range(agdc2gdf.ordinate2index(agdc2gdf.range_dict['T'][0], 'T'), agdc2gdf.ordinate2index(agdc2gdf.range_dict['T'][1], 'T') + 1)
                            for x in range(agdc2gdf.ordinate2index(agdc2gdf.range_dict['X'][0], 'X'), agdc2gdf.ordinate2index(agdc2gdf.range_dict['X'][1], 'X') + 1)
                            for y in range(agdc2gdf.ordinate2index(agdc2gdf.range_dict['Y'][0], 'Y'), agdc2gdf.ordinate2index(agdc2gdf.range_dict['Y'][1], 'Y') + 1)
                            ]
    logger.debug('storage_indices_list = %s', storage_indices_list)
    
    # Do migration in storage unit batches
    for storage_indices in storage_indices_list:
        data_descriptor = agdc2gdf.read_agdc(storage_indices) 
#        log_multiline(logger.debug, data_descriptor, 'data_descriptor', '\t')
                
        storage_unit_path = agdc2gdf.create_netcdf(storage_indices, data_descriptor)
        if not storage_unit_path: continue
        logger.debug('storage_unit_path = %s', storage_unit_path)
        
        agdc2gdf.write_gdf_data(storage_indices, data_descriptor, storage_unit_path)
        
         

if __name__ == '__main__':
    main()
