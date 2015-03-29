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

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import \
    ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
    DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, \
    INTERVAL, MACADDR, NUMERIC, REAL, SMALLINT, TEXT, \
    TIME, TIMESTAMP, UUID, VARCHAR, INT4RANGE, INT8RANGE, NUMRANGE, \
    DATERANGE, TSRANGE, TSTZRANGE
#    JSON, JSONB, OID, TSVECTOR
    
    
Base = declarative_base()

class SQLAlchemyDB(object):
    '''
    classdocs
    '''

    def get_ndarray_types(self):
        '''Return a dict containing all defined (<ndarray_type_tag>: <ndarray_type>) pairs
        '''
        return {ndarray_type.ndarray_type_tag: ndarray_type for ndarray_type in self.session.query(NDarrayType)}
        
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
        
        self.engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (self._user,
                                                                     self._password,
                                                                     self._host,
                                                                     self._port,
                                                                     self._dbname
                                                                     )
                                    )
        
        Session = sessionmaker(bind=self.engine)
        
        self.session = Session()
        
        self.ndarray_types = self.get_ndarray_types()
        
class _DimensionDomain(Base):
    __tablename__ = 'dimension_domain'

    dimension_id = Column(BIGINT, ForeignKey('dimension.dimension_id'), primary_key=True)
    domain_id = Column(BIGINT, ForeignKey('domain.domain_id'), primary_key=True)
    
    dimension = relationship("Dimension", 
                             foreign_keys=[dimension_id], 
                             uselist=False, 
                             backref='dimension_domain', 
                             innerjoin=True
                             )    
    domain = relationship("Domain", 
                          foreign_keys=[domain_id], 
                          uselist=False, 
                          backref='dimension_domain', 
                          innerjoin=True
                          )

    def __repr__(self):
        return "<_DimensionDomain(domain_tag='%s', dimension_tag='%s')>" % (
           self.domain.domain_tag, self.dimension.dimension_tag)   

       
class Dimension(Base):
    __tablename__ = 'dimension'

    dimension_id = Column(BIGINT, primary_key=True)
    dimension_name = Column(String(50))
    dimension_tag = Column(String(8))
    
    def __repr__(self):
        return "<Dimension(dimension_id='%d', dimension_name='%s', dimension_tag='%s')>" % (
                            self.dimension_id, self.dimension_name, self.dimension_tag)   
       
       
class Domain(Base):
    __tablename__ = 'domain'

    domain_id = Column(BIGINT, primary_key=True)
    domain_name = Column(String(16))
    domain_tag = Column(String(16))

    def __repr__(self):
        return "<Domain(domain_id='%d', domain_name='%s', domain_tag='%s')>" % (
                            self.domain_id, self.domain_name, self.domain_tag)
       
       
class _NDarrayTypeDimension(Base):
    __tablename__ = 'ndarray_type_dimension'

    ndarray_type_id = Column(BIGINT, ForeignKey('ndarray_type.ndarray_type_id'), primary_key=True)
    domain_id = Column(BIGINT, ForeignKey('dimension_domain.domain_id'), primary_key=True)
    dimension_id = Column(BIGINT, ForeignKey('dimension_domain.dimension_id'), primary_key=True)
    creation_order = Column(SMALLINT)
    dimension_extent = Column(NUMERIC)
    dimension_elements = Column(BIGINT)
    dimension_cache = Column(BIGINT)
    dimension_origin = Column(NUMERIC)
    indexing_type_id = Column(SMALLINT, ForeignKey('indexing_type.indexing_type_id'))
    reference_system_id = Column(BIGINT, ForeignKey('reference_system.reference_system_id'))
    dimension_extent_unit = Column(String(32))
    
    _dimension_domain = relationship('_DimensionDomain', 
                                    foreign_keys=[domain_id, dimension_id], 
                                    uselist=False, 
                                    backref='ndarray_type_dimension', 
                                    innerjoin=True,
                                    primaryjoin="and_(_DimensionDomain.domain_id==_NDarrayTypeDimension.domain_id, "
                                                "_DimensionDomain.dimension_id==_NDarrayTypeDimension.dimension_id)"
                                    )
    def __repr__(self):
        return "<_NDarrayTypeDimension(ndarray_type_id='%d', dimension_id='%d', domain_id='%d')>" % (
                            self.ndarray_type_id, self.dimension_id, self.domain_id)
       
       
class NDarrayType(Base):
    __tablename__ = 'ndarray_type'

    ndarray_type_id = Column(BIGINT, primary_key=True)
    ndarray_type_name = Column(String(254))
    ndarray_type_tag = Column(String(16))
    
    _ndarray_type_dimensions = relationship('_NDarrayTypeDimension', 
                                foreign_keys=[ndarray_type_id],
                                uselist=True, 
                                backref='ndarray_type',
                                innerjoin=True,
                                primaryjoin="_NDarrayTypeDimension.ndarray_type_id==NDarrayType.ndarray_type_id",
                                order_by=[_NDarrayTypeDimension.creation_order]
                                )
    
    def __repr__(self):
        return "<NDarrayType(ndarray_type_tag='%s', ndarray_type_name='%s', ndarray_type_tag='%s')>" % (
                            self.ndarray_type_tag, self.ndarray_type_name, self.ndarray_type_tag)
    
    def _dimensions(self):
        '''Return list of dimension objects sorted by creation order
        '''
        return [ndarray_type_dimension._dimension_domain.dimension for ndarray_type_dimension in self._ndarray_type_dimensions]

    def _domains(self):
        '''Return set of domain objects
        '''
        return set([ndarray_type_dimension.dimension_domain.domain for ndarray_type_dimension in self.ndarray_type_dimensions])

    @property
    def dimensions(self):
        return self._dimensions()

    @property
    def domains(self):
        return self._domains()
    
    
