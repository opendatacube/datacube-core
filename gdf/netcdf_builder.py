# -------------------------------------------------------------------------------
# Name:         netcdf_builder
# Purpose:      Selection of functions to open, create and manage netCDF
#		objects and files. These routines have been developed and
#		tested with netCDF3 and netCDF4 file formats.
#
# Author:       Matt Paget, Edward King
#
# Created:      24 March 2011
# Copyright:    2011-2014 CSIRO (Commonwealth Science and Industry Research
#               Organisation, Australia).
# License:      This software is open source under the CSIRO BSD (3 clause)
#               License variant as provided in the accompanying LICENSE file or
#               available from
#   https://stash.csiro.au/projects/CMAR_RS/repos/netcdf-tools/browse/LICENSE.
#               By continuing, you acknowledge that you have read and you
#               accept and will abide by the terms of the License.
#
# Updates:
# 24 Mar 2011	Initial demonstration of the idea.
# 16 Nov 2011	Separated the code into logical components and made the netCDF
#		object the primary object to be passed between routines.
#		Generalised the nc3_add_data() routine to accept slices (or
#		slice-like elements) and thus give the user more control over
#		where data is placed in the NetCDF variable.
# 22 Nov 2011	Added optional timeunit argument to nc3_set_timelatlon().
#		Some minor changes to the comments.
# 10 Feb 2013	Renamed to netcdf_builder.py
#		Changed the 'nc3' prefix of all routines to 'nc'. Retained
#		'nc3*' function names for backward compatibility.
#		Refreshed routines to work with the netCDF4-python package.
#		Updated add variables commands to add a _FillValue by default.
#		Added mode keyword in call to Dataset for permission parameter
#		in nc_open.
#		Added zlib option to nc_set_var.
# 11 Apr 2013	Added chunksizes option to nc_set_var.
# 17 Apr 2013	Wrapped import of OrderedDict in a try statement.
#		Rearranged default order of dimension attributes.
#		Added a check for changing the _FillValue attribute of a
#		variable in nc_set_attributes.
# 12 Oct 2013	Added axis attribute to time, latitude and longitude dimensions
#		in nc_set_timelatlon (not strictly required but handy for
#		completeness).
#		Removed 'nc_' prefix from each function name but retained
#		previous function names as alias functions at the bottom of the
#		script.
#		Changed default format to NETCDF4_CLASSIC in ncopen().
# 19 Mar 2014	Added add_bounds() function for adding the CF bounds attribute
#		to a dimension and the associated bounds array to a new
#		variable.
#		Added warning comment to add_data() docstring.
# 15 Aug 2014	Added type normalisation to get_attributes - now as an
#		internal function called for all global and variable attributes
#		and switchable via a boolean parameter. Previously, variable
#		attributes were normalised.
# 12 May 2015	Added _ncversion() and _setattr() functions to provide a
# 		wrapper for the bug described at
# 		  https://code.google.com/p/netcdf4-python/issues/detail?id=110
# 		A warning message is printed about the bug if the netCDF C
# 		library version is < 4.2 and an AttributeError occured when
# 		setting the attribute value. The AttributeError is then raised.
# 		The library bug may not be cause of the error but if it is then
# 		the message should prove helpful.
# 13 May 2015	Implemented the work-around, in addition to printing the warning
# 		message.
# -------------------------------------------------------------------------------

# All functions, except ncopen(), operate on the netCDF object that is
# returned from ncopen().  The functions contain "standard" operations for
# creating a netCDF file, defining dimensions, adding data and adding/removing
# metadata.  If additional operations are required then the netCDF4 package
# routines can be used directly.  In which case, the functions here can be
# considered as examples of using the netCDF4 package routines.
#
# One limitation on returning the netCDF object from ncopen() is that the
# corresponding filename is not retained with the object.  If the filename was
# added as an object attribute it would become a global attribute in the
# resulting file.  Other possible work-arounds, such as managing a filename
# object attribute separately or creating a class that inherits from the
# netCDF object class, would create a non-standard netCDF implementation.
# So it is instead left to the user to retain and manage the filename.
#
# The netCDF4 package uses NumPy arrays and data types to manage data.  As
# such it is difficult to exclude NumPy entirely when working with the
# netCDF4 package.  If NumPy is not available on your system then you *may*
# be able to use this code as a guide and customise your own routines.
#
# Requires:
#  NumPy
#  OrderedDict
#  https://code.google.com/p/netcdf4-python/

from __future__ import absolute_import
from __future__ import print_function
import os
import re

import netCDF4
import numpy as np

from collections import OrderedDict


def ncopen(fname, permission='a', format='NETCDF4_CLASSIC'):
    """
    Return a netCDF object.
    Default permission is 'a' for appending.
    """
    if permission == 'w':
        ncobj = netCDF4.Dataset(fname, mode=permission, format=format)
    else:
        # Format will be deduced by the netCDF modules
        ncobj = netCDF4.Dataset(fname, mode=permission)
    return ncobj


def ncclose(ncobj):
    """
    Close a netCDF object.
    This is required to write the final state of the netCDF object to disk.
    """
    ncobj.close()


def _ncversion(v=None):
    """Return the netCDF C library version. If v is None the full version
    number string is returned. If v is a full version number string the
    [major].[minor] number is returned as float"""
    if v is None:
        return str(netCDF4.getlibversion()).split()[0]  # Full string version
    v = v.split('.')
    if len(v) == 1:
        v.append('0')
    return float('.'.join(v[0:2]))  # [major].[minor] number as a float


def _setattr(obj, name, val):
    """Local wrapper for the standard python function setattr() to handle the
    bug described at
      https://code.google.com/p/netcdf4-python/issues/detail?id=110
    which is fixed in netCDF C library >= 4.2"""
    vers = _ncversion()
    if _ncversion(vers) >= 4.2:
        setattr(obj, name, val)
        return
    try:
        setattr(obj, name, val)
    except AttributeError as e:
        print("WARNING: A bug in your netCDF C library version (" + vers + ") may mean that updating an attribute with a value that has a larger size than the current value may cause a library crash. See https://code.google.com/p/netcdf4-python/issues/detail?id=110 for details. We'll attempt to apply the work-around. Best option, however, is to upgrade your netCDF C library.")
        # Ok, we'll fix it, need a temporary variable name
        import uuid
        tmp = str(uuid.uuid4()).split('-')[0]
        cnt = 0
        while (not re.match('[a-z]', tmp)) or \
                (getattr(obj, tmp, None) is not None):
            tmp = str(uuid.uuid4()).split('-')[0]
            cnt += 1
            if cnt > 10:
                print('Failed to create a unique temporary attribute name')
                raise e
        # Apply the fix
        print('Applying set attribute work-around')
        setattr(obj, tmp, tmp)
        setattr(obj, name, val)
        delattr(obj, tmp)


def _normalise(d, verbose=None):
    """Normalise value types from numpy to regular types.
    """
    for k, v in d.iteritems():
        if verbose:
            print('Attribute type and value (' + k + '):', type(v), v)
        if not isinstance(v, str) and not isinstance(v, unicode):
            # Its probably a numpy dtype thanks to the netCDF* module.
            # Need to convert it to a standard python type for JSON.
            # May need to implement more type checks here.
            # Clues:
            #   isinstance(y,np.float32)
            #   np.issubdtype(y,float)
            #   val = str(val)
            try:
                if re.search('\'numpy\.', str(type(v))):
                    v = v.tolist()  # works for numpy arrays and scalars
                else:
                    v = str(v)
                if verbose:
                    print('  Converted to type:', type(v))
            except TypeError:
                print('Conversion error (' + k + '):', type(v), v)
                pass
            d[k] = v
    return d


def get_attributes(ncobj, verbose=None, normalise=True):
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

      ncobj.dimensions.iteritems()
      ncobj.variables
      ncobj.ncattrs()
      ncobj.__dict__
    """
    d = OrderedDict()

    # Get the global attributes
    d.update(ncobj.__dict__)

    # Iterate through each Dimension and Variable, pre-pending the dimension
    # or variable name to the name of each attribute
    for name, var in ncobj.variables.iteritems():
        for att, val in var.__dict__.iteritems():
            d.update({name + ':' + att: val})
    if normalise:
        d = _normalise(d, verbose)
    return d


def set_attributes(ncobj, ncdict, delval='DELETE'):
    """
    Copy attribute names and values from a dict (or OrderedDict) to a netCDF
    object.
    Global attributes are keyed in the OrderedDict by the attribute name.
    Variable attributes are keyed in the OrderedDict by the variable name and
    attribute name separated by a colon, i.e. variable:attribute.

    If any value is equal to delval then, if the corresponding attribute exists
    in the netCDF object, the corresponding attribute is removed from the
    netCDF object.  The default value of delval is 'DELETE'. For example,
      nc3_set_attributes(ncobj, {'temperature:missing_value':'DELETE'})
    will delete the missing_value attribute from the temperature variable.

    A ValueError exception is raised if a key refers to a variable name that
    is not defined in the netCDF object.
    """
    # Add metadata attributes
    for k in ncdict.keys():
        p = k.partition(':')
        if p[1] == "":
            # Key is a global attribute
            if ncdict[k] == delval:
                delattr(ncobj, p[0])
            else:
                _setattr(ncobj, p[0], ncdict[k])
        elif p[0] in ncobj.variables:
            # Key is a variable attribute
            if ncdict[k] == delval:
                delattr(ncobj.variables[p[0]], p[2])
            elif p[2] == "_FillValue":
                # Its ok to have _FillValue in the dict as long as it has
                # the same value as the variable's attribute
                if getattr(ncobj.variables[p[0]], p[2]) != ncdict[k]:
                    print("Warning: As of netcdf4-python version 0.9.2, _FillValue can only be set when the variable is created (see http://netcdf4-python.googlecode.com/svn/trunk/Changelog). The only way to change the _FillValue would be to copy the array and create a new variable.")
                    raise AttributeError("Can not change " + k)
            else:
                _setattr(ncobj.variables[p[0]], p[2], ncdict[k])
        else:
            raise ValueError("Variable name in dict does not match any variable names in the netcdf object:", p[0])
            # print "Updated attributes in netcdf object"


def set_timelatlon(ncobj, ntime, nlat, nlon, timeunit=None):
    """
    Create a skeleton 3-D netCDF object with time, latitude and longitude
    dimensions and corresponding dimension variables (but no data in the
    dimension variables).  The dimension variables have 'long_name',
    'standard_name' and 'units' attributes defined.

    Inputs 'ntime', 'nlat' and 'nlon' are the number of elements for the time,
    latitude and longitude vector dimensions, respectively.
    A length of None or 0 (zero) creates an unlimited dimension.

    The default unit for time is: 'days since 1800-01-01 00:00:00.0'.
    The time unit should be in a Udunits format. The time unit and calendar
    (default = gregorian) are used by add_time() to encode a list of
    datetime objects.

    The skeleton object can be customised with the netCDF4 module methods. See
    http://netcdf4-python.googlecode.com/svn/trunk/docs/netCDF4-module.html

    To write data to the dimension variables see add_time() and add_data().

    Recommended ordering of dimensions is:
      time, height or depth (Z), latitude (Y), longitude (X).
    Any other dimensions should be defined before (placed to the left of) the
    spatio-temporal coordinates.

    Examples of adding data to dimensions:
      latitudes[:] = numpy.linspace(-10,-44,681)
      longitudes[:] = numpy.linspace(112,154,841)
      dates = [datetime(2011,2,1)]
      times[:] = netCDF4.date2num(dates,units=times.units,calendar=times.calendar)
    """
    if timeunit is None:
        timeunit = 'days since 1800-01-01 00:00:00.0'

    # Dimensions can be renamed with the 'renameDimension' method of the file
    ncobj.createDimension('time', ntime)
    ncobj.createDimension('latitude', nlat)
    ncobj.createDimension('longitude', nlon)

    times = ncobj.createVariable('time', 'f8', ('time',))
    latitudes = ncobj.createVariable('latitude', 'f8', ('latitude',))
    longitudes = ncobj.createVariable('longitude', 'f8', ('longitude',))

    latitudes.long_name = 'latitude'
    latitudes.standard_name = 'latitude'
    latitudes.units = 'degrees_north'
    latitudes.axis = 'Y'
    longitudes.long_name = 'longitude'
    longitudes.standard_name = 'longitude'
    longitudes.units = 'degrees_east'
    longitudes.axis = 'X'
    times.long_name = 'time'
    times.standard_name = 'time'
    times.units = timeunit
    times.calendar = 'gregorian'
    times.axis = 'T'


def show_dimensions(ncobj):
    """
    Print the dimension names, lengths and whether they are unlimited.
    """
    print('{0:10} {1:7} {2}'.format("DimName", "Length", "IsUnlimited"))
    for dim, obj in ncobj.dimensions.iteritems():
        print('{0:10} {1:<7d} {2!s}'.format(dim, len(obj), obj.isunlimited()))


def set_variable(ncobj, varname, dtype='f4', dims=None, chunksize=None, fill=None, zlib=False, **kwargs):
    """
    Define (create) a variable in a netCDF object.  No data is written to the
    variable yet.  Give the variable's dimensions as a tuple of dimension names.
    Dimensions must have been previously created with ncobj.createDimension
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
    if dims is None:
        dims = ('time', 'latitude', 'longitude')
    return ncobj.createVariable(varname, dtype, dimensions=dims,
                                chunksizes=chunksize, fill_value=fill, zlib=zlib, **kwargs)


def add_time(ncobj, datetime_list, timevar='time'):
    """
    Add time data to the time dimension variable.  This routine is separate to
    add_data() because data/time data is encoded in a special way
    according to the units and calendar associated with the time dimension.

    Timelist is a list of datetime objects.

    The time variable should have already been defined with units and calendar
    attributes.

    Examples from:
      http://netcdf4-python.googlecode.com/svn/trunk/docs/netCDF4-module.html

      from datetime import datetime, timedelta
      datetime_list = [datetime(2001,3,1)+n*timedelta(hours=12) for n in range(...)]

      from netCDF3 import num2date
      dates = num2date(nctime[:],units=nctime.units,calendar=nctime.calendar)
    """
    nctime = ncobj.variables[timevar]
    nctime[:] = netCDF4.date2num(datetime_list,
                                 units=nctime.units,
                                 calendar=nctime.calendar)


def add_bounds(ncobj, dimname, bounds, bndname=None):
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
    # Convert bounds data to a numpy array if needed
    if isinstance(bounds, (list, tuple)):
        bounds = np.array(bounds)
    bndshp = bounds.shape  # tuple
    nverts = bndshp[-1]
    # Get variable object of the dimension
    dimobj = ncobj.variables[dimname]
    # Get/set the name of corresponding bounds variable
    if 'bounds' in dimobj.ncattrs():
        bndname = dimobj.bounds
    else:
        if bndname is None:
            bndname = dimname + '_bounds'
        dimobj.bounds = bndname
    # Get/set the variable object of the bounds variable
    if bndname in ncobj.variables:
        bndobj = ncobj.variables[bndname]
        if bndobj.shape != bndshp:
            raise ValueError('Existing bounds variable shape does not ' +
                             'match data:', bndname, bndobj.shape, bshp)
    else:
        # Need a number of vertices dimension
        nvname = None
        for k, v in ncobj.dimensions.items():
            if len(v) == nverts:
                nvname = k
                break
        if nvname is None:
            nvname = 'nv'
            if nvname in ncobj.dimensions:
                nvname = dimname + '_nv'  # Change it if there's a conflict
            ncobj.createDimension(nvname, nverts)
        # Create the bounds variable
        bndobj = ncobj.createVariable(bndname, dimobj.dtype,
                                      dimobj.dimensions + (nvname,))
    # Is this a time dimension
    istime = False
    if dimname.lower() == 'time':
        istime = True
    elif 'axis' in dimobj.ncattrs():
        if dimobj.axis.lower() == 't':
            istime = True
    # Add the bounds array
    if istime:
        bndobj[:] = netCDF4.date2num(bounds, units=dimobj.units,
                                     calendar=dimobj.calendar)
    else:
        bndobj[:] = bounds


def add_data(ncobj, varname, data, index=None):
    """
    *****
    THIS FUNCTION DOES NOT WORK AS EXPECTED (at least last time it was tested).
    You are better off using the netCDF4 module directly, i.e.,
        ncobj.variables[varname][:] = numpy_array
    or to add 2D array to a 3D variable,
        ncobj.variables[varname][0,:,:] = numpy_array
    *****

    Add variable data to the given variable name.
    Data can be a list, tuple or NumPy array.

    In general, the size and shape of data must correspond to the size and
    shape of the variable, except when:
    - There are dimension(s) of size 1 defined for the variable, in which
      case these dimension(s) don't need to be included in data.
    - There are unlimited dimension(s) defined for the variable, in which
      case these dimension(s) of the data can be of any size.

    It is also possible to add data to a subset of the variable via slices
    (ranges and strides) passed in through the index parameter.  The only
    caveat to using this feature is that the size and shape suggested by index
    must match the size and shape of data and follow the "In general..." rules
    for the variable as given above.
    This is potentially a very complicated feature.  The netCDF module
    (netCDF4.Variable) will fail if the variable and data dimensions and
    index slices are not compatible.  It is difficult to catch or manipulate
    the inputs to satisfy the netCDF module due to the variety and flexibility
    of this feature.  Experimentation before production is highly recommended.

    Index is a sequence (list,tuple) of slice objects, lists, tuples, integers
    and/or strings.  Each element of the sequence is internally converted to a
    slice object.
    For best results, a sequence of slice objects is recommended so that you
    have explicit control over where the data is placed within the variable.
    The other element types are provided for convenience.

    A slice(None,None,None) object is equivalent to filling the variable with
    the data along the corresponding dimension.  Thus, the default behaviour
    (with index not given) is to fill the variable with data.

    Integers are taken to be the start index.  The end index is then chosen to
    match the data array.  This feature only works if the number of data
    dimensions matches the number of variable dimensions.  Otherwise its too
    hard to guess which data dimension the index integer refers to.
    If an integer element is provided in index, a ValueError will be raised if
    the number of data and variable dimensions do not match.

    Strings are probably only really useful for testing.  Strings are of the
    form "start:stop:stride" with any missing element chosen to be None, i.e.:
      '' or ':' -> slice(None,None,None)
      '2' or '2:' -> slice(2,None,None)
      ':2' -> slice(None,2,None)
      '::2' -> slice(None,None,2)
      '2:4' -> slice(2,4,None)
    """

    # Get the variable object and its shape
    var = ncobj.variables[varname]
    vshp = var.shape  # tuple

    # If data is a list then first convert it to a numpy aray so that
    # the shape can be properly interogated
    if isinstance(data, (list, tuple)):
        data = np.array(data)
    dshp = data.shape  # tuple

    # Fill dshp if required
    # Fill index if required

    # Not quite but close:
    # Loop through vshp, check each dim for either size=1 or unlimited
    # if unlimited, dshp[i]=dshp[i] and index[i]='' or ':'
    # if size=1, dshp[i]=1=vshp[i] and index[i]='' or ':'
    # else
    #    if dshp[i] exists dshp[i]=dshp[i]
    #    else dshp[i]=vshp[i]
    #    if index[i] exists index[i]=index[i]
    #    else index[i]='' or ':'

    if index is None:
        index = ('',) * len(vshp)

    range = []  # List of slice objects, one per dimension
    for i, x in enumerate(index):
        if isinstance(x, slice):
            range.append(x)
        elif isinstance(x, (tuple, list)):
            range.append(slice(x))
        elif isinstance(x, int):
            # Assume x is start index and we slice to the corresponding
            # shape of data
            if len(vshp) == len(dshp):
                # dshp must be same size as vshp for this to work!
                range.append(slice(x, x + dshp[i]))
            else:
                raise ValueError(
                    "Number of dimensions for the data and variable do not match, so I can't guess which data dimension this index refers to. Be explicit with the index range in a slice or string")
        elif isinstance(x, str):
            # Assume its some sort of start:stop:stride string
            p = x.split(':')
            for j in [0, 1, 2]:
                if j < len(p):
                    if p[j] == '':
                        p[j] = None
                    else:
                        p[j] = int(p[j])
                else:
                    p.append(None)
            range.append(slice(p[0], p[1], p[2]))
        else:
            raise TypeError("Index element is not a valid type: ", x, type(x))

    # Try to add the data to the variable.
    # netCDF4.Variable will complain if dimensions and size ar not valid
    var[range] = data


# Alias functions to support some back compatibility with code that imported
# earlier versions of this file.
def nc_open(*args, **kwargs): return ncopen(args, kwargs)


def nc_close(*args, **kwargs): return ncclose(args, kwargs)


def nc_get_attributes(*args, **kwargs): return get_attributes(args, kwargs)


def nc_set_attributes(*args, **kwargs): return set_attributes(args, kwargs)


def nc_set_timelatlon(*args, **kwargs): return set_timelatlon(args, kwargs)


def nc_show_dims(*args, **kwargs): return show_dims(args, kwargs)


def nc_set_var(*args, **kwargs): return set_var(args, kwargs)


def nc_add_time(*args, **kwargs): return add_time(args, kwargs)


def nc_add_data(*args, **kwargs): return add_data(args, kwargs)


def nc3_open(*args, **kwargs): return ncopen(args, kwargs)


def nc3_close(*args, **kwargs): return ncclose(args, kwargs)


def nc3_get_attributes(*args, **kwargs): return get_attributes(args, kwargs)


def nc3_set_attributes(*args, **kwargs): return set_attributes(args, kwargs)


def nc3_set_timelatlon(*args, **kwargs): return set_timelatlon(args, kwargs)


def nc3_show_dims(*args, **kwargs): return show_dims(args, kwargs)


def nc3_set_var(*args, **kwargs): return set_var(args, kwargs)


def nc3_add_time(*args, **kwargs): return add_time(args, kwargs)


def nc3_add_data(*args, **kwargs): return add_data(args, kwargs)
