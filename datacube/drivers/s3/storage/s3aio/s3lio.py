'''
S3LIO Class

Labeled Array access, backed by multiple S3 objects.

'''

import os
import uuid
import itertools
import numpy as np
from itertools import repeat
from multiprocessing import Pool, freeze_support, cpu_count
import SharedArray as sa
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
from pprint import pprint
from .s3aio import S3AIO


class S3LIO(object):

    DECIMAL_PLACES = 6

    def __init__(self, enable_s3=True, file_path=None):
        self.s3aio = S3AIO(enable_s3, file_path)

    def chunks_indices_1d(self, begin, end, step):
        for i in range(begin, end, step):
            yield slice(i, min(end, i + step))

    def chunk_indices_nd(self, shape, chunk):
        var1 = map(self.chunks_indices_1d, itertools.repeat(0), shape, chunk)
        return itertools.product(*var1)

    def put_array_in_s3(self, array, chunk_size, base_name, bucket):
        idx = list(self.chunk_indices_nd(array.shape, chunk_size))
        keys = [base_name+'_'+str(i) for i in range(len(idx))]
        self.shard_array_to_s3_mp(array, idx, bucket, keys)
        return list(zip(keys, idx))

    def shard_array_to_s3(self, array, indices, s3_bucket, s3_keys):
        # todo: multiprocess put_bytes or if large put_bytes_mpu
        for s3_key, index in zip(s3_keys, indices):
            self.s3aio.s3io.put_bytes(s3_bucket, s3_key, bytes(array[index].data))

    def work_shard_array_to_s3(self, args):
        return self.work_shard_array_to_s3_impl(*args)

    def work_shard_array_to_s3_impl(self, s3_key, index, array_name, s3_bucket):
        array = sa.attach(array_name)
        self.s3aio.s3io.put_bytes(s3_bucket, s3_key, bytes(array[index].data))

    def shard_array_to_s3_mp(self, array, indices, s3_bucket, s3_keys):
        num_processes = cpu_count()
        pool = Pool(num_processes)
        array_name = '_'.join(['SA3IO', str(uuid.uuid4()), str(os.getpid())])
        sa.create(array_name, shape=array.shape, dtype=array.dtype)
        shared_array = sa.attach(array_name)
        shared_array[:] = array

        pool.map_async(self.work_shard_array_to_s3, zip(s3_keys, indices, repeat(array_name), repeat(s3_bucket)))
        pool.close()
        pool.join()
        sa.delete(array_name)

    def assemble_array_from_s3(self, array, indices, s3_bucket, s3_keys, dtype):
        # TODO: parallelize this
        for s3_key, index in zip(s3_keys, indices):
            b = self.s3aio.s3io.get_bytes(s3_bucket, s3_key)
            m = memoryview(b)
            shape = tuple((i.stop - i.start) for i in index)
            array[index] = np.ndarray(shape, buffer=m, dtype=dtype)
        return array

    # converts positional(spatial/temporal) coordinates to array integer coordinates
    def regular_index(self, query, dimension_range, shape):
        # regular_index((-35+2*0.128, 149+2*0.128), ((-35,-34),(149,150)), (4000, 4000))
        # regular_index((-35+0.128, 149+0.128), ((-35, -35+0.128),(149, 148+0.128)), (512, 512))

        length = np.around([dr[1] - dr[0] for dr in dimension_range], S3LIO.DECIMAL_PLACES)
        offset = [dr[0] for dr in dimension_range]
        point = np.around([q-o for q, o in zip(query, offset)], S3LIO.DECIMAL_PLACES)

        result = np.floor([(p/l)*s for p, l, s in zip(point, length, shape)])
        return result

    # labeled geo-coordinates data retrieval.
    def get_data(self, base_location, macro_shape, micro_shape, dtype, slice, s3_bucket):
        # shape and chunk are overloaded.
        # should use macro_shape to mean shape of the array pre-chunking.
        # should use micro_shape to mean chunk size of the array.
        pass

    # integer index data retrieval.
    def get_data_unlabeled(self, base_location, macro_shape, micro_shape, dtype, slice, s3_bucket):
        # shape and chunk are overloaded.
        # should use macro_shape to mean shape of the array pre-chunking.
        # should use micro_shape to mean chunk size of the array.
        pass
