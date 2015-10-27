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
'''

import sys
import logging
import psycopg2

from _gdfutils import log_multiline

logger = logging.getLogger(__name__)


class CachedResultSet(object):
    '''Class CachedResultSet to manage an in-memory cache of query results
    '''
    def __init__(self, cursor): 
        '''Constructor for class CachedResultSet
        
        Parameter:
            cursor: psycopg2 cursor object through which the result set from a previously executed query will be retrieved 
            Stores values in lists to avoid duplicating field names in dicts
        '''
        if cursor.description is None: # No fields returned            
            self._field_names = []
            self._result_dict = {}
            self._record_count = 0
        else:
            self._field_names = [field_descriptor[0] for field_descriptor in cursor.description]
            
            self._result_dict = {field_name: [] for field_name in self._field_names}
            
            for record in cursor:
                for field_index in range(len(record)):
                    self._result_dict[self._field_names[field_index]].append(record[field_index])
                    
            self._record_count = len(self._result_dict[self._field_names[0]])
     
    def record_generator(self, calculated_field_dict=None): 
        '''
        Generator function to return a complete dict for each record
        
        Optional parameter: calculated_field_dict = {
            <calculated_field_name>: <function_on_record_dict>,
            <calculated_field_name>: <function_on_record_dict>,
            ...
            }
            
            where <function_on_record_dict> is a function which takes a record_dict and returns a single value
        '''
        calculated_field_dict = calculated_field_dict or {}
        for record_index in range(self._record_count):
            record_fields = {field_name: self._result_dict[field_name][record_index] for field_name in self._field_names}
            calculated_fields = {field_name: calculated_field_dict[field_name](record_fields) for field_name in calculated_field_dict.keys()}
            record_fields.update(calculated_fields)   
            yield record_fields
            
    def add_values(self, value_dict): 
        '''
        Function to add new calculated values to the result set.
        Parameter: value_dict = {
            <field_name_string>: <field_value_list>,
            <field_name_string>: <field_value_list>,
            ...
            }
            
        N.B: Overwriting of existing field values IS permitted
        '''
        new_field_count = 0
        
        for field_name in [field_name.lower() for field_name in value_dict.keys()]:
            assert len(value_dict[field_name]) == self._record_count, 'Mismatched record count. Result set has $d but %s has %d.' % (self._record_count,
                                                                                                               field_name,
                                                                                                               len(value_dict[field_name])
                                                                                                               )
            if field_name not in self._field_names:
                self._field_names.append(field_name)
                new_field_count += 1
                
            self._result_dict[field_name] = value_dict[field_name]
        
        return new_field_count
    

    @property
    def field_names(self):
        return self._field_names

    @property
    def field_count(self):
        return len(self._field_names)

    @property
    def record_count(self):
        return self._record_count

    @property
    def field_values(self):
        return self._result_dict


class Database(object):
    '''
    Class Database
    '''

    def create_connection(self, autocommit=None):
        db_connection = psycopg2.connect(host=self._host, 
                                              port=self._port, 
                                              dbname=self._dbname, 
                                              user=self._user, 
                                              password=self._password)
        
        if autocommit is None:
            autocommit = self._autocommit
        
        if autocommit:
            db_connection.autocommit = True
            db_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        else:
            db_connection.autocommit = False
            db_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

        return db_connection
    
    def execSQL(self, SQL, params=None, cursor=None):
        '''
        Function to return cursor with query results for specified SQL and parameters
        
        Parameters:
            SQL: Query text
            params: Dict containing query parameters (optional)
            cursor: cursor to return results. Defaults to self._default_cursor
        '''
        cursor = cursor or self._default_cursor or self.create_connection().cursor()
        
        log_multiline(logger.debug, cursor.mogrify(SQL, params), 'SQL', '\t')
        cursor.execute(SQL, params)
        
        return cursor
    
    def submit_query(self, SQL, params=None, connection=None):
        '''
        Function to return CachedResultSet object to manage an in-memory cache of query results for specified SQL and parameters
        
        Parameters:
            SQL: Query text
            params: Dict containing query parameters (optional)
            connection: DB connection to query. Defaults to self._default_connection
        '''
        connection = connection or self._default_connection or self.create_connection()
        
        log_multiline(logger.debug, SQL, 'SQL', '\t')
        log_multiline(logger.debug, params, 'params', '\t')
        
        # Use local cursor to return results
        return CachedResultSet(self.execSQL(SQL, params, cursor=connection.cursor()))
        
    
    def _close_default_connection(self):
        '''Function to close default connection if required
        '''
        if self._default_connection:
            self._default_connection.close()
            
        self._default_connection = None
        self._default_cursor = None

    def _setup_default_cursor(self):
        '''Function to setup default connection and cursor if required
        '''
        self._close_default_connection()
        
        if self._keep_connection:
            self._default_connection = self.create_connection()
            self._default_cursor = self._default_connection.cursor()
    
    def __init__(self, db_ref, host, port, dbname, user, password, keep_connection=True, autocommit=True):
        '''
        Constructor for class Database.
        
        Parameters:
            host: PostgreSQL database host
            port: PostgreSQL database port
            dbname: PostgreSQL database database name
            user: PostgreSQL database user
            password: PostgreSQL database password for user
        '''
        self._db_ref = db_ref
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._password = password
        self._keep_connection = keep_connection
        self._autocommit = autocommit
        self._default_connection = None
        self._default_cursor = None
        
        self._setup_default_cursor()
        
        log_multiline(logger.debug, self.__dict__, 'Database.__dict__', '\t')
    
    
    def commit(self): 
        '''
        Commit transaction if autocommit not enabled
        '''
        if self._autocommit:
            logger.warning('Autocommit enabled. Ignoring transaction commit request.')
        elif self._default_connection.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
            self._default_connection.commit()
            logger.debug('Transaction on default connection committed')
        else:
            logger.warning('Default connection not in transaction. Ignoring transaction commit request.')
        
    def rollback(self): 
        '''
        Rollback transaction if autocommit not enabled
        '''
        if self._autocommit:
            logger.warning('Autocommit enabled. Ignoring transaction rollback request.')
        elif self._default_connection.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
            self._default_connection.rollback()
            logger.debug('Transaction on default connection rolled back')
        else:
            logger.warning('Default connection not in transaction. Ignoring transaction rollback request.')
        
    # Define class properties
    @property
    def db_ref(self):
        return self._db_ref

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, host):
        if self._host != host:
            self._host = host
            self._setup_default_cursor()
        
    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, port):
        if self._port != port:
            self._port = port
            self._setup_default_cursor()
        
    @property
    def dbname(self):
        return self._dbname

    @dbname.setter
    def dbname(self, dbname):
        if self._dbname != dbname:
            self._dbname = dbname
            self._setup_default_cursor()
        
    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, user):
        if self._user != user:
            self._user = user
            self._setup_default_cursor()
        
    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, password):
        if self._password != password:
            self._password = password
            self._setup_default_cursor()
            
    @property
    def keep_connection(self):
        return self._keep_connection

    @keep_connection.setter
    def keep_connection(self, keep_connection):
        if self._keep_connection != keep_connection:
            self._keep_connection = keep_connection            
            self._setup_default_cursor()
            
    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, autocommit):
        if self._autocommit != autocommit:
            self._autocommit = autocommit            
            self._setup_default_cursor()
            
    @property
    def default_connection(self):
        return self._default_connection

    @property
    def default_cursor(self):
        return self._default_cursor
