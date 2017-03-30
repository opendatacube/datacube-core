'''S3 storage driver class
'''
from __future__ import absolute_import

from datacube.drivers.driver import Driver

class S3Driver(Driver):
    '''S3 storage driver. A placeholder for now.
    '''

    @property
    def name(self):
        return 's3'
