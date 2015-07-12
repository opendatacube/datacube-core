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
from osgeo import gdal, gdalconst, osr
from datetime import datetime

from eotools.utils import log_multiline

# Only needed for testing
from pprint import pprint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Logging level for this module

try:
    import netcdf_builder
except ImportError:
    logger.error('Requires netcdf_builder.py (https://stash.csiro.au/projects/CMAR_RS/repos/netcdf-tools/browse/create/netcdf_builder.py)')
    raise

class GDFNetCDF(object):
    '''
    Class GDFNetCDF - Class to manage GDF netCDF storage units
    '''
    def __init__(self, storage_config, netcdf_filename=None, netcdf_mode=None, netcdf_format=None, decimal_places=None):
        '''
        Constructor for class GDFNetCDF
        Parameters:
            storage_config: nested dict containing configuration for storage type (defined in class GDF)
            netcdf_filename: Filename of netCDF file to be opened
            netcdf_mode: Mode for netCDF file open
            netcdf_format: Format for netCDF file open
        '''
        self._isopen = False
        self.storage_config = storage_config
        self.netcdf_filename = netcdf_filename
        self.netcdf_mode = netcdf_mode or 'r' # Default to 'r' for reading
        self.netcdf_format = netcdf_format or 'NETCDF4_CLASSIC'
        self.decimal_places = decimal_places if decimal_places is not None else 6 # Default to 6 decimal places if no precision specified
        
        if netcdf_filename is None:
            self.netcdf_object = None
        else:
            self.open(netcdf_filename)
            
    def __del__(self):
        '''
        Destructor for class GDFNetCDF
        '''
        self.close()

    def close(self):
        '''
        Destructor for class GDFNetCDF
        '''
        self._isopen = False
        try:
            self.netcdf_object.close()
        except:
            pass

        
    def open(self, netcdf_filename=None, netcdf_mode=None, netcdf_format=None):
        '''
        Constructor for class GDFNetCDF
        Parameters:
            storage_config: nested dict containing configuration for storage type (defined in class GDF)
            netcdf_filename: Filename of netCDF file to be opened
            netcdf_mode: Mode for netCDF file open
            netcdf_format: Format for netCDF file open
        '''
        self._isopen = False

        # Default to existing instance values
        self.netcdf_filename = netcdf_filename or self.netcdf_filename
        assert self.netcdf_filename, 'NetCDF filename not provided'

        self.netcdf_mode = netcdf_mode or self.netcdf_mode
        self.netcdf_format = netcdf_format or self.netcdf_format

        if netcdf_mode == 'w':
            self.netcdf_object = netCDF4.Dataset(self.netcdf_filename, mode=self.netcdf_mode, format=self.netcdf_format)
        else:
            # Format will be deduced by the netCDF modules
            self.netcdf_object = netCDF4.Dataset(self.netcdf_filename, mode=self.netcdf_mode)
            self.netcdf_format = self.netcdf_object.file_format

        self._isopen = True
        
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

            logger.debug('dimension = %s', dimension)
            logger.debug('dimension_config = %s', dimension_config)
            logger.debug('index = %s', index)
            logger.debug('dimension_index_vector = %s', dimension_index_vector)

            if dimension_config['indexing_type'] == 'regular' and not dimension_index_vector:
                element_size = dimension_config['dimension_element_size']
                dimension_min = index * dimension_config['dimension_extent'] + dimension_config['dimension_origin'] + element_size / 2.0 # Half pixel to account for netCDF centre of pixel reference
                dimension_max = dimension_min + dimension_config['dimension_extent']
                
                dimension_index_vector = np.around(np.arange(dimension_min, dimension_max, element_size), GDFNetCDF.DECIMAL_PLACES)
                
                # Cater for reversed index (e.g. positive Y index tends Southwards when image origin is in UL/NW corner)
                if dimension_config['reverse_index']:
                    dimension_index_vector = dimension_index_vector[::-1]
                
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
            
        def set_variable(variable_name, variable_config):
            dimensions = self.storage_config['dimensions'].keys()
            dimension_names = tuple([self.storage_config['dimensions'][dimension]['dimension_name']
                                      for dimension in dimensions])

            nc_shape_dict = {dimensions[index]: len(self.netcdf_object.dimensions[dimension_names[index]]) for index in range(len(dimensions))}

            chunksizes = tuple([min(self.storage_config['dimensions'][dimension]['dimension_cache'], nc_shape_dict[dimension])
                                      for dimension in dimensions])
            logger.debug('Creating variable %s with dimensions %s and chunk sizes %s', variable_name, dimensions, chunksizes)
            
            variable = self.netcdf_object.createVariable(variable_name, variable_config['netcdf_datatype_name'], dimensions=dimension_names,
                   chunksizes=chunksizes, fill_value=variable_config['nodata_value'], zlib=True)
            logger.debug('variable = %s' % variable)
            
            # A method of handling variable metadata
            metadata_dict = {variable_name + ':' + 'coordinates': 'latitude longitude',
                             variable_name + ':' + 'grid_mapping': 'crs',
                             variable_name + ':' + 'name': variable_config['measurement_type_name']
                             }
     
            self.set_attributes(metadata_dict)            
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
        
        creation_date = datetime.utcnow().strftime("%Y%m%d")
        self.netcdf_object.history = 'NetCDF-CF file created %s.' %(creation_date)
        self.netcdf_object.license = 'Generalised Data Framework NetCDF-CF Test File'
        self.netcdf_object.spatial_coverage = '%f %s grid' % (self.storage_config['dimensions']['X']['dimension_extent'],
                                                              self.storage_config['dimensions']['X']['reference_system_unit'])
        self.netcdf_object.featureType = 'grid'
     
        #     samples  = template_dataset.RasterXSize
        #     lines    = template_dataset.RasterYSize
    def write_slice(self, variable_name, slice_array, indices_dict):
        '''
        Function to set a specified slice in the specified netCDF variable
        Parameters:
            variable_name: Name of variable to which slice array will be written
            slice_array: Numpy array to be written to netCDF file
            indices_dict: Dict keyed by dimension tag indicating the dimension(s) & index/indices to which the slice should be written
        '''        
        if not self._isopen:
            self.open()

        dimension_config = self.storage_config['dimensions']
        dimensions = dimension_config.keys()
        index_dimensions = indices_dict.keys()
        dimension_names = [dimension_config[dimension]['dimension_name'] for dimension in dimensions]
        # Dict of dimensions and sizes read from netCDF
        nc_shape_dict = {dimensions[index]: len(self.netcdf_object.dimensions[dimension_names[index]]) for index in range(len(dimensions))}

        logger.debug('variable_name = %s', variable_name)
        logger.debug('slice_array.shape = %s', slice_array.shape)
        logger.debug('indices_dict = %s', indices_dict)
        logger.debug('nc_shape_dict = %s', nc_shape_dict)

        assert set(index_dimensions) <= set(dimensions), 'Invalid slice index dimension(s)'
        
        assert len(slice_array.shape) + len(indices_dict) == len(dimensions), 'Indices must be provided for all dimensions not covered by the data array'
        
        slice_shape = tuple(nc_shape_dict[dimension] for dimension in dimensions if dimension not in indices_dict)
        assert slice_array.shape == slice_shape, 'Shape of data array %s does not match storage unit slice shape %s' % (slice_array.shape, slice_shape)
        
        # Create slices for accessing netcdf array
        slicing = [slice(indices_dict[dimension], indices_dict[dimension] + 1) if dimension in index_dimensions 
                  else slice(0, nc_shape_dict[dimension]) for dimension in dimensions]
        logger.debug('slicing = %s', slicing)

        logger.debug('self.netcdf_object.variables = %s' % self.netcdf_object.variables)
        variable = self.netcdf_object.variables[variable_name]
#        logger.debug('variable = %s' % variable)

        logger.debug('slice_array = %s', slice_array)

        variable[slicing] = slice_array
        self.netcdf_object.sync()
        
    def read_slice(self, variable_name, indices_dict):
        '''
        Function to read a specified slice in the specified netCDF variable
        Parameters:
            variable_name: Name of variable from which the slice array will be read
            indices_dict: Dict keyed by dimension tag indicating the dimension(s) & index/indices from which the slice should be read
        Returns:
            slice_array: Numpy array read from netCDF file
        '''        
        if not self._isopen:
            self.open()

        dimension_config = self.storage_config['dimensions']
        dimensions = dimension_config.keys()
        index_dimensions = indices_dict.keys()
        dimension_names = [dimension_config[dimension]['dimension_name'] for dimension in dimensions]
        # Dict of dimensions and sizes read from netCDF
        nc_shape_dict = {dimensions[index]: len(self.netcdf_object.dimensions[dimension_names[index]]) for index in range(len(dimensions))}

        logger.debug('variable_name = %s', variable_name)
        logger.debug('indices_dict = %s', indices_dict)
        logger.debug('nc_shape_dict = %s', nc_shape_dict)
        
        assert set(index_dimensions) <= set(dimensions), 'Invalid slice index dimension(s)'
        
        # Create slices for accessing netcdf array
        slicing = [slice(indices_dict[dimension], indices_dict[dimension] + 1) if dimension in index_dimensions 
                  else slice(0, nc_shape_dict[dimension]) for dimension in dimensions]
        logger.debug('slicing = %s', slicing)

        logger.debug('self.netcdf_object.variables = %s' % self.netcdf_object.variables)
        variable = self.netcdf_object.variables[variable_name]
#        logger.debug('variable = %s' % variable)

        slice_array = variable[slicing]
        logger.debug('slice_array = %s', slice_array)
        return slice_array

    def get_subset_indices(self, range_dict):
        '''
        Function to read an array subset of the specified netCDF variable
        Parameters:
            variable_name: Name of variable from which the subset array will be read
            range_dict: Dict keyed by dimension tag containing the dimension(s) & range tuples from which the subset should be read
        Returns:
            dimension_indices_dict: Dict containing array indices for each dimension
        '''        
        if not self._isopen:
            self.open()

        dimension_config = self.storage_config['dimensions']
        dimensions = dimension_config.keys()
        range_dimensions = range_dict.keys()
        dimension_names = [dimension_config[dimension]['dimension_name'] for dimension in dimensions]
        
        # Dict of dimensions and sizes read from netCDF 
        nc_shape_dict = {dimensions[index]: len(self.netcdf_object.dimensions[dimension_names[index]]) for index in range(len(dimensions))}
        logger.debug('range_dict = %s', range_dict)
        logger.debug('nc_shape_dict = %s', nc_shape_dict)
        
        assert set(range_dimensions) <= set(dimensions), 'Invalid range dimension(s)'
        
        # Create slices for accessing netcdf array
        dimension_indices_dict = {} # Dict containing all indices for each dimension
        for dimension_index in range(len(dimensions)):
            dimension = dimensions[dimension_index]
            dimension_array = self.netcdf_object.variables[dimension_names[dimension_index]][:]
            if dimension in range_dimensions:
                logger.debug('dimension_array = %s', dimension_array)
                logger.debug('range = %s', range_dict[dimension])
                mask_array = ((dimension_array > range_dict[dimension][0]) * (dimension_array <= range_dict[dimension][1])) 
                index_array = np.where(mask_array)
                logger.debug('index_array = %s', index_array)
                dimension_indices_dict[dimension] = dimension_array[mask_array]

                if not index_array:
                    logger.warning('Invalid range %s for dimension %s', range_dict[dimension], dimension)
                    return None
            else: # Range not defined for this dimension - take the whole lot
                dimension_indices_dict[dimension] = dimension_array
            
        return dimension_indices_dict


    def read_subset(self, variable_name, range_dict):
        '''
        Function to read an array subset of the specified netCDF variable
        Parameters:
            variable_name: Name of variable from which the subset array will be read
            range_dict: Dict keyed by dimension tag containing the dimension(s) & range tuples from which the subset should be read
        Returns:
            subset_array: Numpy array read from netCDF file
            dimension_indices_dict: Dict containing array indices for each dimension
        '''        
        if not self._isopen:
            self.open()

        dimension_config = self.storage_config['dimensions']
        dimensions = dimension_config.keys()
        range_dimensions = range_dict.keys()
        dimension_names = [dimension_config[dimension]['dimension_name'] for dimension in dimensions]
        # Dict of dimensions and sizes read from netCDF
        nc_shape_dict = {dimensions[index]: len(self.netcdf_object.dimensions[dimension_names[index]]) for index in range(len(dimensions))}
        
        logger.debug('variable_name = %s', variable_name)
        logger.debug('range_dict = %s', range_dict)
        logger.debug('nc_shape_dict = %s', nc_shape_dict)
        
        assert set(range_dimensions) <= set(dimensions), 'Invalid range dimension(s)'
        
        # Create slices for accessing netcdf array
        dimension_indices_dict = {} # Dict containing all indices for each dimension
        slicing = []
        for dimension_index in range(len(dimensions)):
            dimension = dimensions[dimension_index]
            dimension_array = self.netcdf_object.variables[dimension_names[dimension_index]][:]
            if dimension in range_dimensions:
                logger.debug('dimension_array = %s', dimension_array)
                logger.debug('range = %s', range_dict[dimension])
                mask_array = ((dimension_array > range_dict[dimension][0]) * (dimension_array <= range_dict[dimension][1]))
                index_array = np.where(mask_array)
                logger.debug('index_array = %s', index_array)
                dimension_indices_dict[dimension] = dimension_array[mask_array]
                try:
                    dimension_slice = slice(index_array[0][0], index_array[0][-1] + 1)
                except IndexError:
                    logger.warning('Invalid range %s for dimension %s', range_dict[dimension], dimension)
                    return None
            else: # Range not defined for this dimension
                dimension_indices_dict[dimension] = dimension_array
                dimension_slice = slice(0, nc_shape_dict[dimension])
            slicing.append(dimension_slice)
            
        logger.debug('slicing = %s', slicing)

        variable = self.netcdf_object.variables[variable_name]
#        logger.debug('variable = %s' % variable)

        subset_array = variable[slicing]
        
        logger.debug('subset_array = %s', subset_array)
        return subset_array, dimension_indices_dict
        

    def get_datatype(self, variable_name, convention='numpy'):
        '''
        Returns NetCDF datatype of specified variable
        '''
        return self.storage_config['measurement_types'][variable_name].get(convention + '_datatype_name')
        

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
        
    def georeference_from_file(self, gdal_dataset_path):
        '''
        Function to set georeferencing from template GDAL dataset
        '''
        def getMinMaxExtents(samples, lines, geoTransform):
            """
            Calculates the min/max extents based on the input latitude and longitude vectors.
        
            :param samples:
                An integer representing the number of samples (columns) in an array.
        
            :param lines:
                An integer representing the number of lines (rows) in an array.
        
            :param geoTransform:
                A tuple containing the geotransform information returned by GDAL.
        
            :return:
                A tuple containing (min_lat, max_lat, min_lon, max_lat)
        
            :notes:
                Hasn't been tested for nothern or western hemispheres.
            """
            extents = []
            x_list  = [0,samples]
            y_list  = [0,lines]
        
            for px in x_list:
                for py in y_list:
                    x = geoTransform[0]+(px*geoTransform[1])+(py*geoTransform[2])
                    y = geoTransform[3]+(px*geoTransform[4])+(py*geoTransform[5])
                    extents.append([x,y])
        
            extents = np.array(extents)
            min_lat = np.min(extents[:,1])
            max_lat = np.max(extents[:,1])
            min_lon = np.min(extents[:,0])
            max_lon = np.max(extents[:,0])
        
            return (min_lat, max_lat, min_lon, max_lon)
        
        # Start of georeference_from_file(self, gdal_dataset_path) definition
        gdal_dataset = gdal.Open(gdal_dataset_path)
        assert gdal_dataset, 'Unable to open file %s' % gdal_dataset_path
        
        geotransform = gdal_dataset.GetGeoTransform()
        logger.debug('geotransform = %s', geotransform)
        projection = gdal_dataset.GetProjection()
        logger.debug('projection = %s', projection)
        
        # Set coordinate reference system metadata variable
        spatial_reference = osr.SpatialReference()
        spatial_reference.ImportFromWkt(projection)
        crs_metadata = {'crs:name': spatial_reference.GetAttrValue('geogcs'),
                    'crs:longitude_of_prime_meridian': 0.0, #TODO: This needs to be fixed!!! An OSR object should have this, but maybe only for specific OSR references??
                    'crs:inverse_flattening': spatial_reference.GetInvFlattening(),
                    'crs:semi_major_axis': spatial_reference.GetSemiMajor(),
                    'crs:semi_minor_axis': spatial_reference.GetSemiMinor(),
                    }
        self.set_variable('crs', dims=(), dtype='i4')
        self.set_attributes(crs_metadata)
        logger.debug('crs_metadata = %s', crs_metadata)
     
        extents = getMinMaxExtents(gdal_dataset.RasterXSize, gdal_dataset.RasterYSize, geotransform)
        #pdb.set_trace()
        self.netcdf_object.geospatial_lat_min = extents[0]
        self.netcdf_object.geospatial_lat_max = extents[1]
        self.netcdf_object.geospatial_lat_units = 'degrees_north'
        self.netcdf_object.geospatial_lat_resolution = geotransform[5]
        self.netcdf_object.geospatial_lon_min = extents[2]
        self.netcdf_object.geospatial_lon_max = extents[3]
        self.netcdf_object.geospatial_lon_units = 'degrees_east'
        self.netcdf_object.geospatial_lon_resolution = geotransform[1]

    @property
    def isopen(self):
        return self._isopen

        
