"""
Provides `SafeStringsDataset`, a replacement netCDF4.Dataset class which works
around a bug in NetCDF4 which causes attribute strings written to files to
be incompatible with older NetCDF software. Ensures that strings are only
written as UTF-8 encoded bytes.

For more information see https://github.com/Unidata/netcdf4-python/issues/448
"""
import netCDF4


class _VariableProxy(object):
    """
    Wraps a netCDF4 Variable object, ensuring that any attributes are written
    as bytes and not unicode strings.
    """
    __initialized = False

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.__initialized = True

    @property  # type: ignore
    def __class__(self):
        return self._wrapped.__class__

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def __setattr__(self, name, value):
        if self.__initialized:
            if isinstance(value, str):
                value = value.encode('ascii')
            setattr(self._wrapped, name, value)
        else:
            super(_VariableProxy, self).__setattr__(name, value)

    def __getitem__(self, key):
        return self._wrapped.__getitem__(key)

    def __setitem__(self, key, value):
        self._wrapped.__setitem__(key, value)

    def setncattr(self, name, value):
        self._wrapped.setncattr(name, value)


class _NC4DatasetProxy(object):
    """
    Mixin to the NetCDF4.Dataset, ensuring that attributes are written as bytes
    and not unicode strings, and that created and accessed variables are
    wrapped in _VariableProxy objects.

    Overrides the `createVariable()` method, and the `nco[varname]` style access.

    Doesn't yet support `nco.variables[varname]` style access of variables.
    """

    def __setattr__(self, name, value):
        if isinstance(value, str):
            value = value.encode('ascii')
        super(_NC4DatasetProxy, self).__setattr__(name, value)

    def setncattr(self, name, value):
        if isinstance(value, str):
            value = value.encode('ascii')
        super(_NC4DatasetProxy, self).setncattr(name, value)

    def __getitem__(self, name):
        var = super(_NC4DatasetProxy, self).__getitem__(name)
        return _VariableProxy(var)

    #: pylint: disable=invalid-name
    def createVariable(self, *args, **kwargs):
        new_var = super(_NC4DatasetProxy, self).createVariable(*args, **kwargs)
        return _VariableProxy(new_var)


class SafeStringsDataset(_NC4DatasetProxy, netCDF4.Dataset):
    """
    A wrapper for NetCDF4.Dataset, which ensures all attributes and variable attributes
    are stored using encoded bytes and not unicode strings.

    Unicode strings cause a bug in the NetCDF4 library which make them unreadable by
    some older software.
    """
    pass
