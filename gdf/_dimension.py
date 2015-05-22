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
Created on 31/03/2015

@author: Alex Ip
'''
from _sqlalchemy_db import SQLAlchemyDB

def get_dimension(dimension_tag, db_dict):
    '''
    Method to either return an existing instance of dimension for the supplied tag or a new instance if it doesn't already exist
    Parameters:
        dimension_tag: string containing candidate key for dimension
        db_dict: dict keyed by dbref
    '''
    dimension = Dimension._dimension_dict.get(dimension_tag)
    
    if dimension is None:
        dimension = Dimension(dimension_tag, db_dict)
            
    return dimension

def get_all_dimensions(db_dict):
    '''
    Function to return a dict keyed by dimension_tag containing all dimensions across all databases in db_dict
    '''
    for db in db_dict.values():
        for dimension_tag in db.dimensions.keys():
            if dimension_tag not in Dimension._dimension_dict.keys():
                get_dimension(dimension_tag, db_dict)
     
    return Dimension._dimension_dict  

class Dimension(object):
    '''
    Class Dimension to manage the same dimension across multiple databases
    '''
    # Class variable to track all Dimension instances
    _dimension_dict = {}
    
    def __init__(self, dimension_tag, db_dict):
        '''
        Constructor for class Dimension
        Parameters:
            dimension_tag: string containing candidate key for dimension
            db_dict: dict keyed by dbref
        '''
        assert dimension_tag not in Dimension._dimension_dict.keys(), 'Dimension object already created for dimension %s' % dimension_tag
        
        self._dimension_tag = dimension_tag
        self._dbdimension_dict = {}
        
        for db in db_dict.values():
            # Should only have one reference to each dimension per database
            self._dbdimension_dict.update({db.dbref: dbdimension for dbdimension in db.dimensions.values() if dbdimension.dimension_tag == self._dimension_tag})
            
        Dimension._dimension_dict[dimension_tag] = self

    @property
    def dimension_tag(self):
        return self._dimension_tag
                
    @property
    def dbdimension_dict(self):
        return self._dbdimension_dict
                
        