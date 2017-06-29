"""This module implements a simple plugin manager for storage drivers.

Drivers are automatically loaded provided they:
  - Store all code in a direct subdirectory, e.g. `s3/`
  - Include a `DRIVER_SPEC` attribute in the `__init__.py`. This
    attribute must be a tuple indicating `(<name>, <class_name>,
    <filepath>)` where the `<name>` is the driver's name as specified
    by the end user, e.g. `NetCDF CF` or `s3`; `<class_name>` is the
    python class name for that driver, e.g. `S3Driver`; and
    `<filepath>` is the filepath to the python module containing that
    class, e.g. `./driver.py`. The reason for specifying this
    information is to optimise the search for the driver, without
    loading all modules in the subdirectory.
  - Extend the `driver.Driver` abstract class,
    e.g. `S3Driver(Driver)`.

`drivers.loader.drivers` returns a dictionary of drivers instances,
indexed by their `name` as defined in `DRIVER_SPEC`. These are
instantiated on the first call to that method and cached until the
loader object is deleted.
"""
