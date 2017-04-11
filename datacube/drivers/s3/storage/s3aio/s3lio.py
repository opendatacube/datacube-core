'''
S3LIO Class

Labeled Array access, backed by multiple S3 objects.

'''

import os
import sys
import uuid
import hashlib
import numpy as np
from six.moves import map, zip
from itertools import repeat, product
from pathos.multiprocessing import ProcessingPool as Pool
from pathos.multiprocessing import freeze_support, cpu_count
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

    def chunk_indices_1d(self, begin, end, step, bound_slice=None):
        if bound_slice is None:
            for i in range(begin, end, step):
                yield slice(i, min(end, i + step))
        else:
            bound_begin = bound_slice.start
            bound_end = bound_slice.stop
            end = min(end, bound_end)
            for i in range(begin, end, step):
                if i < bound_begin and i+step <= bound_begin:
                    continue
                yield slice(max(i, bound_begin), min(end, i + step))

    def chunk_indices_nd(self, shape, chunk, array_slice=None):
        if array_slice is None:
            array_slice = repeat(None)
        var1 = map(self.chunk_indices_1d, repeat(0), shape, chunk, array_slice)
        return product(*var1)

    def put_array_in_s3(self, array, chunk_size, base_name, bucket, spread=False):
        idx = list(self.chunk_indices_nd(array.shape, chunk_size))
        keys = [base_name+'_'+str(i) for i in range(len(idx))]
        if spread:
            keys = [hashlib.md5(k.encode('utf-8')).hexdigest()[0:6] + '_' + k for k in keys]
        self.shard_array_to_s3(array, idx, bucket, keys)
        return list(zip(keys, idx))

    def put_array_in_s3_mp(self, array, chunk_size, base_name, bucket, spread=False):
        idx = list(self.chunk_indices_nd(array.shape, chunk_size))
        keys = [base_name+'_'+str(i) for i in range(len(idx))]
        if spread:
            keys = [hashlib.md5(k.encode('utf-8')).hexdigest()[0:6] + '_' + k for k in keys]
        self.shard_array_to_s3_mp(array, idx, bucket, keys)
        return list(zip(keys, idx))

    def shard_array_to_s3(self, array, indices, s3_bucket, s3_keys):
        # todo: multiprocess put_bytes or if large put_bytes_mpu
        for s3_key, index in zip(s3_keys, indices):
            if sys.version_info >= (3, 5):
                self.s3aio.s3io.put_bytes(s3_bucket, s3_key, bytes(array[index].data))
            else:
                self.s3aio.s3io.put_bytes(s3_bucket, s3_key, bytes(np.ascontiguousarray(array[index]).data))

    def shard_array_to_s3_mp(self, array, indices, s3_bucket, s3_keys):
        def work_shard_array_to_s3(s3_key, index, array_name, s3_bucket):
            array = sa.attach(array_name)
            if sys.version_info >= (3, 5):
                self.s3aio.s3io.put_bytes(s3_bucket, s3_key, bytes(array[index].data))
            else:
                self.s3aio.s3io.put_bytes(s3_bucket, s3_key, bytes(np.ascontiguousarray(array[index]).data))

        num_processes = cpu_count()
        pool = Pool(num_processes)
        array_name = '_'.join(['SA3IO', str(uuid.uuid4()), str(os.getpid())])
        sa.create(array_name, shape=array.shape, dtype=array.dtype)
        shared_array = sa.attach(array_name)
        shared_array[:] = array
        results = pool.map(work_shard_array_to_s3, s3_keys, indices, repeat(array_name), repeat(s3_bucket))
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
    def regular_index(self, query, dimension_range, shape, flatten=False):
        # regular_index((-35+2*0.128, 149+2*0.128), ((-35,-34),(149,150)), (4000, 4000))
        # regular_index((-35+0.128, 149+0.128), ((-35, -35+0.128),(149, 148+0.128)), (512, 512))

        length = np.around([dr[1] - dr[0] for dr in dimension_range], S3LIO.DECIMAL_PLACES)
        offset = [dr[0] for dr in dimension_range]
        point = np.around([q-o for q, o in zip(query, offset)], S3LIO.DECIMAL_PLACES)

        result = np.floor([(p/l)*s for p, l, s in zip(point, length, shape)]).astype(int)

        if flatten:
            return self.s3aio.to_1d(tuple(result), shape)

        return result

    # labeled geo-coordinates data retrieval.
    def get_data(self, base_location, macro_shape, micro_shape, dtype, labeled_slice, s3_bucket):
        # shape and chunk are overloaded.
        # should use macro_shape to mean shape of the array pre-chunking.
        # should use micro_shape to mean chunk size of the array.
        # 1. reproject query crs -> native crs
        # 2. regular_index native crs -> integer indexing
        # 3. get_data_unlabeled
        pass

    # integer index data retrieval.
    def get_data_unlabeled(self, base_location, macro_shape, micro_shape, dtype, array_slice, s3_bucket):
        # shape and chunk are overloaded.
        # should use macro_shape to mean shape of the array pre-chunking.
        # should use micro_shape to mean chunk size of the array.
        # slices = self.chunk_indices_nd(macro_shape, micro_shape, array_slice)
        # chunk_ids = [np.ravel_multi_index(tuple([s.start for s in s]), macro_shape) for s in slices]
        # keys = ['_'.join(["base_location", str(i)]) for i in chunk_ids]
        # keys = [hashlib.md5(k.encode('utf-8')).hexdigest()[0:6] + '_' + k for k in keys]
        # zipped = zip(keys, chunk_ids, slices)

        # 1. create shared array of "macro_shape" size and of dtype
        # 2. create 1 process per zipped task.
        # 3. get slice from s3 and write into shared array.
        pass
