'''
S3LIO Class

Labeled Array access, backed by multiple S3 objects.

'''

import os
import sys
import uuid
import hashlib
import numpy as np
import SharedArray as sa
from six import integer_types
from six.moves import map, zip
from itertools import repeat, product
from pathos.multiprocessing import ProcessingPool
from pathos.multiprocessing import freeze_support, cpu_count
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
from .s3aio import S3AIO


class S3LIO(object):

    DECIMAL_PLACES = 6

    def __init__(self, enable_s3=True, file_path=None, num_threads=30):
        self.s3aio = S3AIO(enable_s3, file_path, num_threads)

        if num_threads is None:
            num_threads = cpu_count()

        self.pool = ProcessingPool(num_threads)

    def chunk_indices_1d(self, begin, end, step, bound_slice=None, return_as_shape=False):
        if bound_slice is None:
            for i in range(begin, end, step):
                if return_as_shape:
                    yield min(end, i + step) - i
                else:
                    yield slice(i, min(end, i + step))
        else:
            bound_begin = bound_slice.start
            bound_end = bound_slice.stop
            end = min(end, bound_end)
            for i in range(begin, end, step):
                if i < bound_begin and i+step <= bound_begin:
                    continue
                if return_as_shape:
                    yield min(end, i + step) - max(i, bound_begin)
                else:
                    yield slice(max(i, bound_begin), min(end, i + step))

    def chunk_indices_nd(self, shape, chunk, array_slice=None, return_as_shape=False):
        if array_slice is None:
            array_slice = repeat(None)
        var1 = map(self.chunk_indices_1d, repeat(0), shape, chunk, array_slice, repeat(return_as_shape))
        return product(*var1)

    def put_array_in_s3(self, array, chunk_size, base_name, bucket, spread=False):
        idx = list(self.chunk_indices_nd(array.shape, chunk_size))
        chunk_ids = [i for i in range(len(idx))]
        keys = [base_name+'_'+str(i) for i in chunk_ids]
        if spread:
            keys = [hashlib.md5(k.encode('utf-8')).hexdigest()[0:6] + '_' + k for k in keys]
        self.shard_array_to_s3(array, idx, bucket, keys)
        return list(zip(keys, idx, chunk_ids))

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

        array_name = '_'.join(['SA3IO', str(uuid.uuid4()), str(os.getpid())])
        sa.create(array_name, shape=array.shape, dtype=array.dtype)
        shared_array = sa.attach(array_name)
        shared_array[:] = array
        results = self.pool.map(work_shard_array_to_s3, s3_keys, indices, repeat(array_name), repeat(s3_bucket))

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
    # pylint: disable=too-many-locals
    def regular_index(self, query, dimension_range, shape, flatten=False):
        # regular_index((-35+2*0.128, 149+2*0.128), ((-35,-34),(149,150)), (4000, 4000))
        # regular_index((-35+0.128, 149+0.128), ((-35, -35+0.128),(149, 148+0.128)), (512, 512))

        if all(isinstance(i, integer_types) for i in dimension_range):
            length = dimension_range
            offset = [0 for dr in dimension_range]
        else:
            length = np.around([dr[1] - dr[0] for dr in dimension_range], S3LIO.DECIMAL_PLACES)
            offset = [dr[0] for dr in dimension_range]

        point = np.around([q-o for q, o in zip(query, offset)], S3LIO.DECIMAL_PLACES)

        result = np.floor([(p/l)*s for p, l, s in zip(point, length, shape)]).astype(int)
        result = [min(r, s-1) for r, s in zip(result, shape)]

        print(length, offset, point, result)

        if flatten:
            # return self.s3aio.to_1d(tuple(result), shape)
            macro_shape = tuple([(int(np.ceil(a/b))) for a, b in zip(dimension_range, shape)])
            print(result, macro_shape)
            return self.s3aio.to_1d(tuple(result), macro_shape)

        return result

    # labeled geo-coordinates data retrieval.
    def get_data(self, base_location, dimension_range, micro_shape, dtype, labeled_slice, s3_bucket):
        # shape and chunk are overloaded.
        # should use macro_shape to mean shape of the array pre-chunking.
        # should use micro_shape to mean chunk size of the array.
        # 1. reproject query crs -> native crs
        # 2. regular_index native crs -> integer indexing
        # 3. get_data_unlabeled
        pass

    # integer index data retrieval.
    # pylint: disable=too-many-locals
    def get_data_unlabeled(self, base_location, macro_shape, micro_shape, dtype, array_slice, s3_bucket,
                           use_hash=False):
        # TODO(csiro):
        #     - use SharedArray for data
        #     - multiprocess the for loop depending on slice size.
        #     - not very efficient, redo
        #     - point retrieval via integer index instead of slicing operator.
        #
        # element_ids = [np.ravel_multi_index(tuple([s.start for s in s]), macro_shape) for s in slices]

        # data slices for each chunk
        slices = list(self.chunk_indices_nd(macro_shape, micro_shape, array_slice))

        # chunk id's for each data slice
        slice_starts = [tuple([s.start for s in s]) for s in slices]

        chunk_ids = [self.s3aio.to_1d(tuple(np.floor([(p/float(s)) for p, s, in zip(c, micro_shape)]).astype(int)),
                                      tuple([(int(np.ceil(a/float(b)))) for a, b in zip(macro_shape, micro_shape)]))
                     for c in slice_starts]

        # chunk_sizes for each chunk
        chunk_shapes = list(self.chunk_indices_nd(macro_shape, micro_shape, None, True))
        chunk_shapes = [chunk_shapes[c] for c in chunk_ids]

        # compute keys
        keys = ['_'.join([base_location, str(i)]) for i in chunk_ids]
        if use_hash:
            keys = [hashlib.md5(k.encode('utf-8')).hexdigest()[0:6] + '_' + k for k in keys]

        data = np.zeros(shape=[s.stop - s.start for s in array_slice], dtype=dtype)

        # calculate offsets
        offset = tuple([i.start for i in array_slice])
        # calculate data slices
        data_slices = [tuple([slice(s.start-o, s.stop-o) for s, o in zip(s, offset)]) for s in slices]
        # calculate local slices
        origin = [[s.start % cs if s.start >= cs else s.start for s, cs in zip(s, micro_shape)] for s in slices]
        size = [[s.stop-s.start for s in s] for s in data_slices]
        local_slices = [[slice(o, o+s) for o, s in zip(o, s)] for o, s in zip(origin, size)]

        zipped = zip(keys, data_slices, local_slices, chunk_shapes, repeat(offset))

        # get the slices and populate the data array.
        for s3_key, data_slice, local_slice, shape, offset in zipped:
            data[data_slice] = self.s3aio.get_slice_by_bbox(local_slice, shape, dtype, s3_bucket, s3_key)

        return data

    def get_data_unlabeled_mp(self, base_location, macro_shape, micro_shape, dtype, array_slice, s3_bucket,
                              use_hash=False):
        # TODO(csiro):
        #     - use SharedArray for data
        #     - multiprocess the for loop depending on slice size.
        #     - not very efficient, redo
        #     - point retrieval via integer index instead of slicing operator.
        #
        # element_ids = [np.ravel_multi_index(tuple([s.start for s in s]), macro_shape) for s in slices]
        def work_data_unlabeled(array_name, s3_key, data_slice, local_slice, shape, offset):
            result = sa.attach(array_name)
            result[data_slice] = self.s3aio.get_slice_by_bbox(local_slice, shape, dtype, s3_bucket, s3_key)

        # data slices for each chunk
        slices = list(self.chunk_indices_nd(macro_shape, micro_shape, array_slice))

        # chunk id's for each data slice
        slice_starts = [tuple([s.start for s in s]) for s in slices]

        chunk_ids = [self.s3aio.to_1d(tuple(np.floor([(p/float(s)) for p, s, in zip(c, micro_shape)]).astype(int)),
                                      tuple([(int(np.ceil(a/float(b)))) for a, b in zip(macro_shape, micro_shape)]))
                     for c in slice_starts]

        # chunk_sizes for each chunk
        chunk_shapes = list(self.chunk_indices_nd(macro_shape, micro_shape, None, True))
        chunk_shapes = [chunk_shapes[c] for c in chunk_ids]

        # compute keys
        keys = ['_'.join([base_location, str(i)]) for i in chunk_ids]
        if use_hash:
            keys = [hashlib.md5(k.encode('utf-8')).hexdigest()[0:6] + '_' + k for k in keys]

        # create shared array
        array_name = '_'.join(['S3LIO', str(uuid.uuid4()), str(os.getpid())])
        sa.create(array_name, shape=[s.stop - s.start for s in array_slice], dtype=dtype)
        data = sa.attach(array_name)

        # calculate offsets
        offset = tuple([i.start for i in array_slice])
        # calculate data slices
        data_slices = [tuple([slice(s.start-o, s.stop-o) for s, o in zip(s, offset)]) for s in slices]
        # calculate local slices
        origin = [[s.start % cs if s.start >= cs else s.start for s, cs in zip(s, micro_shape)] for s in slices]
        size = [[s.stop-s.start for s in s] for s in data_slices]
        local_slices = [[slice(o, o+s) for o, s in zip(o, s)] for o, s in zip(origin, size)]

        zipped = zip(keys, data_slices, local_slices, chunk_shapes, repeat(offset))

        self.pool.map(work_data_unlabeled, repeat(array_name), keys, data_slices, local_slices, chunk_shapes,
                      repeat(offset))

        sa.delete(array_name)

        return data
