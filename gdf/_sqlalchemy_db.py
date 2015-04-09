#!/usr/bin/env python
from EOtools.utils import log_multiline

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
from datetime import datetime
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
import logging

from EOtools.utils import log_multiline

    
logger = logging.getLogger(__name__)
    
Base = declarative_base()

class SQLAlchemyDB(object):
    '''
    classdocs
    '''

    def get_ndarray_types(self):
        '''
        Return a dict containing all defined (<ndarray_type_tag>: <ndarray_type>) pairs in DB
        '''
        return {ndarray_type.ndarray_type_tag: ndarray_type for ndarray_type in self.session.query(NDarrayType)}
        
    def get_dimensions(self):
        '''
        Return a dict containing all defined (<dimension_tag>: <dimension>) pairs in DB
        Requires self.ndarray_types
        '''
        dimension_set = set([])
        for ndarray_type in self.ndarray_types.values():
            dimension_set |= set(ndarray_type.dimensions)
            
        return {dimension.dimension_tag: dimension for dimension in dimension_set}
            
    def get_domains(self):
        '''
        Return a dict containing all defined (<domain_tag>: <domain>) pairs in DB
        Requires self.ndarray_types
        '''
        domain_set = set([])
        for ndarray_type in self.ndarray_types.values():
            domain_set |= set(ndarray_type.domains)
            
        return {domain.domain_tag: domain for domain in domain_set}
            
        

    def __init__(self, dbref, host, port, dbname, user, password):
        '''
        Constructor for class Database.
        
        Parameters:
            host: PostgreSQL database host
            port: PostgreSQL database port
            dbname: PostgreSQL database database name
            user: PostgreSQL database user
            password: PostgreSQL database password for user
        '''
        self._dbref = dbref
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
        
        # Create dicts containing relevant configuration objects
        self._ndarray_types = self.get_ndarray_types()
        self._dimensions = self.get_dimensions()
        self._domains = self.get_domains()

    def get_ndarrays(self, dimension_range_dict): 
        '''
        Function to return all ndarrays which fall in the specified dimensional ranges
        
        Parameter:
            dimension_range_dict: dict defined as {<dimension_tag>: (<min_value>, <max_value>), 
                                                   <dimension_tag>: (<min_value>, <max_value>)...}
        '''
        ndarray_dict = {}
        
        for ndarray_type in self._ndarray_types.values():
            logger.debug('ndarray_type = %s', ndarray_type)
            
            # Skip ndarray_type if  dimensionality of query is greater than dimensionality of ndarray_type - may do this differently
            if set(dimension_range_dict.keys()) > set([dimension.dimension_tag for dimension in ndarray_type.dimensions]):
                continue
            
            # Create a dict of lists containing ndarrays for each ndarray_type
            ndarray_dict[ndarray_type] = {}
            
            # Obtain list of dimension tags ordered by creation order
            dimension_tag_list = [dimension.dimension_tag for dimension in ndarray_type.dimensions if dimension.dimension_tag in dimension_range_dict.keys()]
            logger.debug('dimension_tag_list = %s', dimension_tag_list)
            #===============================================================
            # for dimension_tag in dimension_tag_list:
            #     min_index, min_ordinate = ndarray_type.get_index_and_ordinate(dimension_tag, dimension_range_dict[dimension_tag][0])
            #     max_index, max_ordinate = ndarray_type.get_index_and_ordinate(dimension_tag, dimension_range_dict[dimension_tag][1])
            #     
            #===============================================================
            SQL = '''-- Find ndarrays which fall in range
select distinct'''
            for dimension_tag in dimension_tag_list:
                SQL +='''
%s.ndarray_dimension_index as %s_index,
%s.ndarray_dimension_min as %s_min,
%s.ndarray_dimension_max as %s_max,''' % (dimension_tag, dimension_tag, dimension_tag, dimension_tag, dimension_tag, dimension_tag)
            SQL +='''
ndarray.*
from ndarray
'''                    
            for dimension_tag in dimension_tag_list:
                SQL +='''join (
select *
from dimension 
    join dimension_domain using(dimension_id)
    join ndarray_dimension using(dimension_id, domain_id)
    where ndarray_type_id = %d
    and ndarray_version = 0
    and dimension.dimension_tag = '%s'
    and (ndarray_dimension_min between %f and %f 
        or ndarray_dimension_max between %f and %f)
    ) %s using(ndarray_type_id, ndarray_id, ndarray_version)
''' % (ndarray_type.ndarray_type_id, 
   dimension_tag, 
   dimension_range_dict[dimension_tag][0],
   dimension_range_dict[dimension_tag][1],
   dimension_range_dict[dimension_tag][0],
   dimension_range_dict[dimension_tag][1],
   dimension_tag
   )
            SQL +='''
order by ''' + '_index, '.join(dimension_tag_list) + '''_index;
'''
            
            #TODO: Evaluate the SQL query to obtain a list of ndarray objects
            log_multiline(logger.debug, SQL , 'SQL', '\t')
            print SQL
                
                
    @property
    def dbref(self):
        return self._dbref

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port
        
    @property
    def dbname(self):
        return self._dbname

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password
        
    @property
    def ndarray_types(self):
        return self._ndarray_types

    @property
    def dimensions(self):
        return self._dimensions

    @property
    def domains(self):
        return self._domains
        
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
       
class ReferenceSystem(Base):
    __tablename__ = 'reference_system'

    reference_system_id = Column(BIGINT, primary_key=True)
    reference_system_name = Column(String(32))
    reference_system_unit = Column(String(32))
    reference_system_definition = Column(String(254))
    reference_system_tag = Column(String(32))

    def __repr__(self):
        return "<ReferenceSystem(reference_system_id='%d', reference_system_name='%s', reference_system_tag='%s')>" % (
                            self.reference_system_id, self.reference_system_name, self.reference_system_tag)
       
       
class IndexingType(Base):
    __tablename__ = 'indexing_type'

    indexing_type_id = Column(SMALLINT, primary_key=True)
    indexing_type_name = Column(String(128))

    def __repr__(self):
        return "<IndexingType(indexing_type_id='%d', indexing_type_name='%s')>" % (
                            self.indexing_type_id, self.indexing_type_name)
       
       
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
    index_reference_system_id = Column(BIGINT, ForeignKey('reference_system.reference_system_id'))
    
    _dimension_domain = relationship('_DimensionDomain', 
                                    foreign_keys=[domain_id, dimension_id], 
                                    uselist=False, 
                                    backref='ndarray_type_dimension', 
                                    innerjoin=True,
                                    primaryjoin="and_(_DimensionDomain.domain_id==_NDarrayTypeDimension.domain_id, "
                                                "_DimensionDomain.dimension_id==_NDarrayTypeDimension.dimension_id)"
                                    )
    
    indexing_type = relationship('IndexingType', 
                                    foreign_keys=[indexing_type_id], 
                                    uselist=False, 
                                    backref='ndarray_type_dimension', 
                                    innerjoin=True,
                                    primaryjoin="IndexingType.indexing_type_id==_NDarrayTypeDimension.indexing_type_id"
                                    )
    
    reference_system = relationship('ReferenceSystem', 
                                    foreign_keys=[reference_system_id], 
                                    uselist=False, 
                                    backref='ndarray_type_dimension', 
                                    innerjoin=True,
                                    primaryjoin="ReferenceSystem.reference_system_id==_NDarrayTypeDimension.reference_system_id"
                                    )
    
    index_reference_system = relationship('ReferenceSystem', 
                                    foreign_keys=[index_reference_system_id], 
                                    uselist=False, 
                                    backref='index_ndarray_type_dimension', 
                                    innerjoin=True,
                                    primaryjoin="ReferenceSystem.reference_system_id==_NDarrayTypeDimension.index_reference_system_id",
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
        return set([ndarray_type_dimension._dimension_domain.domain for ndarray_type_dimension in self._ndarray_type_dimensions])
    
    def get_index_and_ordinate(self, dimension_tag, dimension_value):
        '''
        Returns a index value and offset within ndarray for given dimension_tag and dimension_value
        '''
        dimension = [dimension for dimension in self.dimensions if dimension.dimension_tag == dimension_tag][0]
        
        ndarray_type_dimension = [ndarray_type_dimension for ndarray_type_dimension in self._ndarray_type_dimensions if ndarray_type_dimension.dimension_id == dimension.dimension_id][0]
        
        domain = [domain for domain in self.domains if domain.domain_id == ndarray_type_dimension.domain_id][0]
        
        #TODO: Re-examine conditions for exceptional indexing - not sure if this is the best way
        if (ndarray_type_dimension.indexing_type.indexing_type_name == 'regular' and 
            ndarray_type_dimension.index_reference_system_id == ndarray_type_dimension.reference_system_id):
            # Regular index calculated from origin and extent values
            ndarray_index = (dimension_value - ndarray_type_dimension.dimension_origin) // ndarray_type_dimension.dimension_extent
            ndarray_ordinate = ((dimension_value - ndarray_type_dimension.dimension_origin) % ndarray_type_dimension.dimension_extent) * ndarray_type_dimension.dimension_elements
        else:
            # Special case for year-indexed time (irregular intervals)
            if (dimension.dimension_tag == 'T' and
                ndarray_type_dimension.index_reference_system.reference_system_tag == 'YEAR' and 
                ndarray_type_dimension.reference_system.reference_system_tag == 'SSE'):
                #Set year index value form seconds-since-epoch value
                ndarray_index = datetime.fromtimestamp(dimension_value).year
                # Keep seconds-since-epoch value?
                ndarray_ordinate = dimension_value
            else:
                raise Exception('Unhandled indexing type')    
            
        return ndarray_index, ndarray_ordinate

    @property
    def dimensions(self):
        return self._dimensions()

    @property
    def domains(self):
        return self._domains()
    

   
