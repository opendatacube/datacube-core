"""This module implements a simple plugin manager for storage drivers.

Public entry points:

 - new_datasource
 - storage_writer_by_name
 - reader_drivers
 - writer_drivers

TODO: update docs post DriverManager
"""
from __future__ import absolute_import

from .indexes import index_driver_by_name, index_drivers
from .readers import new_datasource, reader_drivers
from .writers import storage_writer_by_name, writer_drivers
