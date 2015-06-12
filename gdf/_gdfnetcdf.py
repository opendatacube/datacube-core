#!/usr/bin/env python

# Some code derived from hnetcdf_builder.py by Matt Paget & Edward King of CSIRO
# https://stash.csiro.au/projects/CMAR_RS/repos/netcdf-tools/browse/create/netcdf_builder.py

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
Created on Jun 9, 2015

@author: Alex Ip (based on code by Matt Paget & Edward King)
'''
import netCDF4
import numpy as np
import os
import re
from collections import OrderedDict
import logging

from EOtools.utils import log_multiline

# Only needed for testing
from pprint import pprint

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Logging level for this module

try:
    import netcdf_builder
except ImportError:
    logger.error('Requires netcdf_builder.py (https://stash.csiro.au/projects/CMAR_RS/repos/netcdf-tools/browse/create/netcdf_builder.py)')
    raise

class GDFNetCDF(object):
    '''
    Class GDFNetCDF - Class to manage GDF netCDF storage units
    '''
    def __init__(self, storage_config, netcdf_filename=None, netcdf_mode=None, netcdf_format=None):
        '''
        Constructor for class GDFNetCDF
        Parameters:
            storage_config: nested dict containing configuration for storage type (defined in class GDF)
            netcdf_filename: Filename of netCDF file to be opened
            netcdf_mode: Mode for netCDF file open
            netcdf_format: Format for netCDF file open
        '''
        self.storage_config = storage_config
        self.netcdf_filename = netcdf_filename
        self.netcdf_mode = netcdf_mode or 'r' # Default to 'r' for reading
        self.netcdf_format = netcdf_format or 'NETCDF4_CLASSIC'
        
        if netcdf_filename is None:
            self.netcdf_object = None
        else:
            self.open(netcdf_filename)
            
    def __del__(self):
        '''
        Destructor for class GDFNetCDF
        '''
        try:
            self.netcdf_object.close()
        except:
            pass

        
    def open(self, netcdf_filename, netcdf_mode=None, netcdf_format=None):
        '''
        Constructor for class GDFNetCDF
        Parameters:
            storage_config: nested dict containing configuration for storage type (defined in class GDF)
            netcdf_filename: Filename of netCDF file to be opened
            netcdf_mode: Mode for netCDF file open
            netcdf_format: Format for netCDF file open
        '''
        # Default to existing instance values
        self.netcdf_filename = netcdf_filename
        self.netcdf_mode = netcdf_mode or self.netcdf_mode
        self.netcdf_format = netcdf_format or self.netcdf_format

        if netcdf_mode == 'w':
            self.netcdf_object = netCDF4.Dataset(self.netcdf_filename, mode=self.netcdf_mode, format=self.netcdf_format)
        else:
            # Format will be deduced by the netCDF modules
            self.netcdf_object = netCDF4.Dataset(netcdf_filename, mode=self.netcdf_mode)
            self.netcdf_format = self.netcdf_object.file_format

        
    def create(self, netcdf_filename, index_tuple, dimension_index_dict={}, netcdf_format=None):
        '''
        Create new NetCDF File in 'w' mode with required dimensions
        Parameters:
            index_tuple = tuple of storage unit indices
            dimension_index_dict: dict of iterables or 1D numpy arrays keyed by dimension_tag. Required for irregular dimensions (e.g. time)
        '''
        def set_dimension(dimension, dimension_config, index, dimension_index_vector=None):
            '''
            Parameters:
                dimension: Dimension tag (e.g. X, Y, T, etc.)
                dimension_config: Nested dict containing storage configuration from GDF.storage_config['<storage_type>']
                index: index for storage unit
                dimension_index_vector: Numpy array of index values for irregular dimension (e.g. time) or None for unlimited irregular dimension
            '''
            if dimension_config['indexing_type'] == 'regular' and not dimension_index_vector:
                dimension_min = index * dimension_config['dimension_extent'] + dimension_config['dimension_origin']
#                dimension_max = dimension_min + dimension_config['dimension_extent']
                
                dimension_index_vector = (np.arange(dimension_config['dimension_elements']) * dimension_config['dimension_element_size'] + # Pixel size
                                            dimension_min + # Storage unit positional offset
                                            (dimension_config['dimension_element_size'] / 2.0)) # Half pixel to account for netCDF centre of pixel reference
                
            #TODO: Implement fixed indexing type
                
            log_multiline(logger.debug, dimension_index_vector, 'dimension_index_vector for %s' % dimension, '\t')
            
            if dimension_index_vector is not None:
                dimension_index_shape = dimension_index_vector.shape
                assert len(dimension_index_shape) == 1, 'Invalid dimension_index_vector shape. Must be 1D'
                assert dimension_index_shape[0] <= dimension_config['dimension_elements'], 'dimension_index_vector must have %d elements or fewer' % dimension_config['dimension_elements']
                dimension_size = len(dimension_index_vector)
                #TODO: Do range checks to ensure indices are within storage unit boundaries
            else:
                dimension_size = 0 # Unlimited dimension
                
            dimension_name = dimension_config['dimension_name']
            
            # Dimensions can be renamed with the 'renameDimension' method of the file
            self.netcdf_object.createDimension(dimension_name, dimension_size)
        
            variable = self.netcdf_object.createVariable(dimension_name,'f8',(dimension_name,))
            for property_name, property_value in dimension_config['properties'].items():
                logger.debug('property_name = %s, property_value = %s', property_name, property_value)
                variable.__setattr__(property_name, property_value)
            
            variable[:] = dimension_index_vector    
            self.netcdf_object.sync()
            #===================================================================
            # if dimension_name == 'time':
            #     variable[:] = netCDF4.date2num(dimension_index_list,
            #                                  units=variable.units,
            #                                  calendar=variable.calendar)
            # else:
            #     variable[:] = dimension_index_vector
            #===================================================================
                
        #=======================================================================
        #     # Open the existing dataset using GDAL
        #     samples  = template_dataset.RasterXSize
        #     lines    = template_dataset.RasterYSize
        #     bands    = len(variables)
        #     geoT     = template_dataset.GetGeoTransform()
        #     prj      = template_dataset.GetProjection()
        #     metadata = template_dataset.GetMetadata_Dict()
        # 
        # 
        #     # Is this specifying centre of pixel or upper left???
        #     # UPDATE!!! netCDF should be centre
        #     latvec, lonvec = calcLatLonVectors(geoT, samples, lines)
        # 
        #     netcdf_object = netcdf_builder.ncopen(netcdf_path, permission='w')
        #     netcdf_builder.set_timelatlon(netcdf_object, 0, lines, samples, timeunit='seconds since 1970-01-01 00:00:00')
        #     netcdf_builder.add_data(netcdf_object, 'latitude', latvec)
        #     netcdf_builder.add_data(netcdf_object, 'longitude', lonvec)
        # 
        #     # Set coordinate reference system metadata variable
        #     ref = osr.SpatialReference()
        #     ref.ImportFromWkt(prj)
        #     crs_metadata = {'crs:name': ref.GetAttrValue('geogcs'),
        #                 'crs:longitude_of_prime_meridian': 0.0, # This needs to be fixed!!! An OSR object should have this, but maybe only for specific OSR references??
        #                 'crs:inverse_flattening': ref.GetInvFlattening(),
        #                 'crs:semi_major_axis': ref.GetSemiMajor(),
        #                 'crs:semi_minor_axis': ref.GetSemiMinor(),
        #                 }
        #     netcdf_builder.set_variable(netcdf_object, 'crs', 'i4')
        #     netcdf_builder.set_attributes(netcdf_object, crs_metadata)
        # 
        #     creation_date = datetime.utcnow().strftime("%Y%m%d")
        #     netcdf_object.history = 'Reformatted to NetCDF %s.' %(creation_date)
        #     netcdf_object.license = 'TEST DATA for SPEDDEXES.'
        #     netcdf_object.spatial_coverage = "1 degree grid."
        #     netcdf_object.featureType = 'grid'
        # 
        #     extents = getMinMaxExtents(samples, lines, geoT)
        #     #pdb.set_trace()
        #     netcdf_object.geospatial_lat_min = extents[0]
        #     netcdf_object.geospatial_lat_max = extents[1]
        #     netcdf_object.geospatial_lat_units = 'degrees_north'
        #     netcdf_object.geospatial_lat_resolution = geoT[5]
        #     netcdf_object.geospatial_lon_min = extents[2]
        #     netcdf_object.geospatial_lon_max = extents[3]
        #     netcdf_object.geospatial_lon_units = 'degrees_east'
        #     netcdf_object.geospatial_lon_resolution = geoT[1]
        # 
        #     # Create individual variables for different output values
        #     for variable_name in sorted(variables):
        #         netcdf_builder.set_variable(netcdf_object, variable_name, dtype=dtypeMapping(variables[variable_name]['gdal_dtype']), fill=variables[variable_name]['no_data_value'])
        # 
        #         # A method of handling variable metadata
        #         metadata_dict = {variable_name + ':' + 'coordinates': 'lat lon',
        #                          variable_name + ':' + 'grid_mapping': 'crs'
        #                          }
        # 
        #         netcdf_builder.set_attributes(netcdf_object, metadata_dict)
        #=======================================================================
            
        def set_variable(variable_name, variable_config):
            dimensions = self.storage_config['dimensions'].keys()
            dimension_names = tuple([self.storage_config['dimensions'][dimension]['dimension_name']
                                      for dimension in dimensions])

            nc_shape_dict = {dimensions[index]: len(self.netcdf_object.dimensions[dimension_names[index]]) for index in range(len(dimensions))}

            chunksizes = tuple([min(self.storage_config['dimensions'][dimension]['dimension_cache'], nc_shape_dict[dimension])
                                      for dimension in dimensions])
            logger.debug('Creating variable %s with dimensions %s and chunk sizes %s', variable_name, dimensions, chunksizes)
            
            variable = self.netcdf_object.createVariable(variable_name, variable_config['netcdf_datatype_name'], dimensions=dimension_names,
                   chunksizes=chunksizes, fill_value=variable_config['nodata_value'], zlib=False)
            logger.debug('variable = %s' % variable)
            self.netcdf_object.sync()
            
        # Start of create function
        # Default to existing instance value
        self.netcdf_mode = 'w' 
        self.netcdf_format = netcdf_format or self.netcdf_format
        
        self.open(netcdf_filename=netcdf_filename)
        
        for dimension, dimension_config in self.storage_config['dimensions'].items():
            set_dimension(dimension, dimension_config, index_tuple[self.storage_config['dimensions'].keys().index(dimension)], dimension_index_dict.get(dimension))
            
        for variable, variable_config in self.storage_config['measurement_types'].items():
            set_variable(variable, variable_config)
            
        logger.debug('self.netcdf_object.variables = %s' % self.netcdf_object.variables)
        
    def write_array(self, variable_name, data_array, indices_dict):
        '''
        '''        
        dimension_config = self.storage_config['dimensions']
        dimensions = dimension_config.keys()
        index_dimensions = indices_dict.keys()
        dimension_names = [dimension_config[dimension]['dimension_name'] for dimension in dimensions]
        # Dict of dimensions and sizes read from netCDF
        nc_shape_dict = {dimensions[index]: len(self.netcdf_object.dimensions[dimension_names[index]]) for index in range(len(dimensions))}

        logger.debug('variable_name = %s', variable_name)
        logger.debug('nc_shape_dict = %s', nc_shape_dict)
        
        assert len(data_array.shape) + len(indices_dict) == len(dimensions), 'Indices must be provided for all dimensions not covered by the data array'
        
        slice_shape = tuple(nc_shape_dict[dimension] for dimension in dimensions if dimension not in indices_dict)
        assert data_array.shape == slice_shape, 'Shape of data array %s does not match storage unit slice shape %s' % (data_array.shape, slice_shape)
        
        # Create slices for accessing netcdf array
        slices = [slice(indices_dict[dimension], indices_dict[dimension] + 1) if dimension in index_dimensions 
                  else slice(0, nc_shape_dict[dimension]) for dimension in dimensions]
        logger.debug('slices = %s', slices)

        logger.debug('self.netcdf_object.variables = %s' % self.netcdf_object.variables)
        variable = self.netcdf_object.variables[variable_name]
        logger.debug('variable = %s' % variable)

        logger.debug('slice = %s', variable[indices_dict['T']])
        logger.debug('data_array = %s', data_array)

        self.netcdf_object.variables[variable_name][slices] = data_array
        self.netcdf_object.sync()
    
    def get_attributes(self, verbose=None, normalise=True):
        """
        Copy the global and variable attributes from a netCDF object to an
        OrderedDict.  This is a little like 'ncdump -h' (without the formatting).
        Global attributes are keyed in the OrderedDict by the attribute name.
        Variable attributes are keyed in the OrderedDict by the variable name and
        attribute name separated by a colon, i.e. variable:attribute.
    
        Normalise means that some NumPy types returned from the netCDF module are
        converted to equivalent regular types.
    
        Notes from the netCDF module:
          The ncattrs method of a Dataset or Variable instance can be used to
          retrieve the names of all the netCDF attributes.
    
          The __dict__ attribute of a Dataset or Variable instance provides all
          the netCDF attribute name/value pairs in an OrderedDict.
    
          self.netcdf_object.dimensions.iteritems()
          self.netcdf_object.variables
          self.netcdf_object.ncattrs()
          self.netcdf_object.__dict__
        """
        return netcdf_builder.get_attributes(self.netcdf_object, verbose, normalise)
    
    def set_attributes(self, ncdict, delval='DELETE'):
        """
        Copy attribute names and values from a dict (or OrderedDict) to a netCDF
        object.
        Global attributes are keyed in the OrderedDict by the attribute name.
        Variable attributes are keyed in the OrderedDict by the variable name and
        attribute name separated by a colon, i.e. variable:attribute.
    
        If any value is equal to delval then, if the corresponding attribute exists
        in the netCDF object, the corresponding attribute is removed from the
        netCDF object.  The default value of delval is 'DELETE'. For example,
          nc3_set_attributes(self.netcdf_object, {'temperature:missing_value':'DELETE'})
        will delete the missing_value attribute from the temperature variable.
    
        A ValueError exception is raised if a key refers to a variable name that
        is not defined in the netCDF object.
        """
        netcdf_builder.set_attributes(self.netcdf_object, ncdict, delval)
               
    def show_dimensions(self):
        """
        Print the dimension names, lengths and whether they are unlimited.
        """
        netcdf_builder.show_dimensions(self.netcdf_object)


    def set_variable(self, varname, dtype='f4', dims=None, chunksize=None, fill=None, zlib=False, **kwargs):
        """
        Define (create) a variable in a netCDF object.  No data is written to the
        variable yet.  Give the variable's dimensions as a tuple of dimension names.
        Dimensions must have been previously created with self.netcdf_object.createDimension
        (e.g. see set_timelatlon()).
    
        Recommended ordering of dimensions is:
          time, height or depth (Z), latitude (Y), longitude (X).
        Any other dimensions should be defined before (placed to the left of) the
        spatio-temporal coordinates.
    
        To create a scalar variable, use an empty tuple for the dimensions.
        Variables can be renamed with the 'renameVariable' method of the netCDF
        object.
    
        Specify compression with zlib=True (default = False).
    
        Specify the chunksize with a sequence (tuple, list) of the same length
        as dims (i.e., the number of dimensions) where each element of chunksize
        corresponds to the size of the chunk along the corresponding dimension.
        There are some tips and tricks associated with chunking - see
        http://data.auscover.org.au/node/73 for an overview.
    
        The default behaviour is to create a floating-point (f4) variable
        with dimensions ('time','latitude','longitude'), with no chunking and
        no compression.
        """
        netcdf_builder.set_variable(self.netcdf_object, varname, dtype=dtype, dims=dims, chunksize=chunksize, fill=fill, zlib=zlib, **kwargs)



    def add_bounds(self, dimension_tag, bounds):
        """Add a bounds array of data to the netCDF object.
        Bounds array can be a list, tuple or NumPy array.
    
        A bounds array gives the values of the vertices corresponding to a dimension
        variable (see the CF documentation for more information). The dimension
        variable requires an attribute called 'bounds', which references a variable
        that contains the bounds array. The bounds array has the same shape as the
        corresponding dimension with an extra size for the number of vertices.
    
        This function:
            - Adds a 'bounds' attribute to the dimension variable if required.
              If a bounds attribute exits then its value will be used for the bounds
              variable (bndname). Otherwise if a bndname is given then this will be
              used. Otherwise the default bndname will be '_bounds' appended to the
              dimension name.
            - If the bounds variable exists then a ValueError will be raised if its
              shape does not match the bounds array.
            - If the bounds variable does not exist then it will be created. If so
              an exra dimension is required for the number of vertices. Any existing
              dimension of the right size will be used. Otherwise a new dimension
              will be created. The new dimension's name will be 'nv' (number of
              vertices), unless this dimension name is already used in which case
              '_nv' appended to the dimension name will be used instead.
            - Lastly, the bounds array is written to the bounds variable. If the
              corresponding dimension is time (name = 'time' or dim.axis = 't') then
              the bounds array will be written as date2num data.
        """
        dimension_tag = dimension_tag.upper()
        dimension_name=self.storage_config['dimensions'][dimension_tag]['dimension_name']
        bounds_name = dimension_name + '_bounds'
        
        netcdf_builder.add_bounds(self.netcdf_object, dimension_name, bounds, bounds_name)
