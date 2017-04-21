'''S3 Test storage driver class. To avoid credential issues with
travis accessing s3, we use a filesystem based s3 driver for testing
purposes.

CAUTION: if run as root, this may write anywhere in the filesystem.
'''
from __future__ import absolute_import

from datacube.drivers.s3.driver import S3Driver
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO

class S3TestDriver(S3Driver):
    '''S3 Test storage driver, using filesystem rather than actual s3, for
    testing purposes only.
    '''

    def __init__(self, name, local_config=None, application_name=None, validate_connection=True):
        '''Initialise the s3 test driver.

        CAUTION: if run as root, this may write anywhere in the
        filesystem.
        '''
        super(S3TestDriver, self).__init__(name, local_config, application_name, validate_connection)
        # Initialise with the root at the top of the filesystem, so
        # that the `container` path can be absolute.
        self.storage = S3LIO(False, '/')
