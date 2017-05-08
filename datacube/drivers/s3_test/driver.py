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

    def __init__(self, name, index=None, *index_args, **index_kargs):
        '''Initialise the s3 test driver.

        Caution: if run as root, this may write anywhere in the
        filesystem.
        '''
        super(S3TestDriver, self).__init__(name, index, *index_args, **index_kargs)
        # Initialise with the root at the top of the filesystem, so
        # that the `container` path can be absolute.
        self.storage = S3LIO(False, '/')


    @property
    def uri_scheme(self):
        '''URI scheme used by this driver.'''
        return 'file_s3'
