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

class CachedResultSet(object):
    '''Class CachedResultSet to manage an in-memory cache of query results
    '''
    def __init__(self, cursor): 
        '''Constructor for class CachedResultSet
        
        Parameter:
            cursor: psycopg2 cursor object through which the result setfrom a previously executed query will be retrieved 
        '''
        if cursor.description is None: # No fields returned            
            self._field_names = []
            self._result_dict = {}
        else:
            self._field_names = [field_descriptor[0] for field_descriptor in cursor.description]
            
            self._result_dict = {field_name: [] for field_name in self._field_names}
            
            for record in cursor:
                for field_index in range(len(record)):
                    self._result_dict[self._field_names[field_index]].append(record[field_index])
     
    def record_generator(self): 
        '''Generator function to return a complete dict for each record
        '''
        for record_index in range(self.record_count):
            yield {field_name: self._result_dict[field_name][record_index] for field_name in self._field_names}           

    @property
    def field_names(self):
        return list(self._field_names)

    @property
    def record_count(self):
        return len(self._result_dict[self._field_names[0]])

    @property
    def field_values(self):
        return dict(self._result_dict)


class Database(object):
    '''
    Class Database
    '''

    def create_connection(self, autocommit=True):
        db_connection = psycopg2.connect(host=self._host, 
                                              port=self._port, 
                                              dbname=self._dbname, 
                                              user=self._user, 
                                              password=self._password)
        if autocommit:
            db_connection.autocommit = True
            db_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        return db_connection
    
    def execSQL(self, SQL, params=None, cursor=None):
        '''
        Function to return cursor with query results for specified SQL and parameters
        
        Parameters:
            SQL: Query text
            params: Dict containing query parameters (optional)
            cursor: cursor to return results. Defaults to self._default_cursor
        '''
        cursor = cursor or self._default_cursor
        
        log_multiline(logger.debug, cursor.mogrify(SQL, params), 'SQL', '\t')
        cursor.execute(SQL, params)
        
        return cursor
    
    def cache_result_set(self, SQL, params=None, connection=None):
        '''
        Function to return CachedResultSet object to manage an in-memory cache of query results for specified SQL and parameters
        
        Parameters:
            SQL: Query text
            params: Dict containing query parameters (optional)
            connection: DB connection to query. Defaults to self._default_connection
        '''
        connection = connection or self._default_connection
        
        # Use local cursor to return results
        return CachedResultSet(self.execSQL(SQL, params=None, cursor=connection.cursor()))
        
    
    def __init__(self, host, port, dbname, user, password):
        '''
        Constructor for class Database.
        
        Parameters:
            host: PostgreSQL database host
            port: PostgreSQL database port
            dbname: PostgreSQL database database name
            user: PostgreSQL database user
            password: PostgreSQL database password for user
        '''
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._password = password
        
        self._default_connection = self.create_connection()
        self._default_cursor = self._default_connection.cursor()
        
        log_multiline(logger.debug, self.__dict__, 'Database.__dict__', '\t')
        
        
    # Define class properties
    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, host):
        if self._host != host:
            self._default_connection.close()
            self._host = host
            self._default_connection = self.create_connection()
            self._default_cursor = self._default_connection.cursor()
        
    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, port):
        if self._port != port:
            self._default_connection.close()
            self._port = port
            self._default_connection = self.create_connection()
            self._default_cursor = self._default_connection.cursor()
        
    @property
    def dbname(self):
        return self._dbname

    @dbname.setter
    def dbname(self, dbname):
        if self._dbname != dbname:
            self._default_connection.close()
            self._dbname = dbname
            self._default_connection = self.create_connection()
            self._default_cursor = self._default_connection.cursor()
        
    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, user):
        if self._user != user:
            self._default_connection.close()
            self._user = user
            self._default_connection = self.create_connection()
            self._default_cursor = self._default_connection.cursor()
        
    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, password):
        if self._password != password:
            self._default_connection.close()
            self._password = password
            self._default_connection = self.create_connection()
            self._default_cursor = self._default_connection.cursor()
            
        
