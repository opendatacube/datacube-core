'''
S3AIO Class

Array wrapper class

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
from .s3io import S3IO


class S3AIO(object):

    def __init__(self):
        self.s3io = S3IO()

    def bytes_to_array(self, data, shape, dtype):
        array = np.empty(shape=shape, dtype=dtype)
        array.data[0:len(data)] = data
        return array

    def copy_bytes_to_shared_array(self, shared_array, start, end, data):
        shared_array.data[start:end] = data

    def chunks_indices_1d(self, begin, end, step):
        for i in range(begin, end, step):
            yield slice(i, min(end, i + step))

    def chunk_indices_nd(self, shape, chunk):
        var1 = map(self.chunks_indices_1d, itertools.repeat(0), shape, chunk)
        return itertools.product(*var1)

    def shard_array_to_s3(self, array, indices, s3_bucket, s3_keys):
        # todo: multiprocess put_bytes or if large put_bytes_mpu
        for s3_key, index in zip(s3_keys, indices):
            self.s3io.put_bytes(s3_bucket, s3_key, bytes(array[index].data))

    # @profile
    def assemble_array_from_s3(self, array, indices, s3_bucket, s3_keys, dtype):
        for s3_key, index in zip(s3_keys, indices):
            b = self.s3io.get_bytes(s3_bucket, s3_key)
            m = memoryview(b)
            shape = tuple((i.stop - i.start) for i in index)
            array[index] = np.ndarray(shape, buffer=m, dtype=dtype)
        return array

    def to_1d(self, index, shape):
        return np.ravel_multi_index(index, shape)

    def to_nd(self, index, shape):
        np.unravel_index(index, shape)

    def get_point(self, index_point, shape, dtype, s3_bucket, s3_key):
        item_size = np.dtype(dtype).itemsize
        idx = self.to_1d(index_point, shape) * item_size
        b = self.s3io.get_byte_range(s3_bucket, s3_key, idx, idx+item_size)
        a = np.frombuffer(b, dtype=dtype, count=-1, offset=0)
        return a

    def cdims(self, slices, shape):
        return [sl.start == 0 and sl.stop == sh and (sl.step is None or sl.step == 1)
                for sl, sh in zip(slices, shape)]

    def get_slice(self, array_slice, shape, dtype, s3_bucket, s3_key):  # pylint: disable=too-many-locals
        # convert array_slice into into sub-slices of maximum contiguous blocks

        # Todo:
        #   - parallelise reads and writes
        #     - option 1. get memory rows in parallel and merge
        #     - option 2. smarter byte range subsets depending on:
        #       - data size
        #       - data contiguity

        cdim = self.cdims(array_slice, shape)

        try:
            end = cdim[::-1].index(False)+1
        except ValueError:
            end = len(shape)

        start = len(shape) - end

        outer = array_slice[:-end]
        outer_ranges = [range(s.start, s.stop) for s in outer]
        outer_cells = list(itertools.product(*outer_ranges))
        blocks = list(zip(outer_cells, itertools.repeat(array_slice[start:])))
        item_size = np.dtype(dtype).itemsize

        results = []
        for cell, sub_range in blocks:
            # print(cell, sub_range)
            s3_start = (np.ravel_multi_index(cell+tuple([s.start for s in sub_range]), shape)) * item_size
            s3_end = (np.ravel_multi_index(cell+tuple([s.stop-1 for s in sub_range]), shape)+1) * item_size
            # print(s3_start, s3_end)
            data = self.s3io.get_byte_range(s3_bucket, s3_key, s3_start, s3_end)
            results.append((cell, sub_range, data))

        result = np.empty([s.stop - s.start for s in array_slice], dtype=dtype)
        offset = [s.start for s in array_slice]

        for cell, sub_range, data in results:
            t = [slice(x.start-o, x.stop-o) if isinstance(x, slice) else x-o for x, o in
                 zip(cell+tuple(sub_range), offset)]
            if data.dtype != dtype:
                data = np.frombuffer(data, dtype=dtype, count=-1, offset=0)
            result[t] = data.reshape([s.stop - s.start for s in sub_range])

        return result

    def work_get_slice(self, args):
        return self.work_get_slice_impl(*args)

    def work_get_slice_impl(self, block, array_name, offset, s3_bucket, s3_key, shape, dtype):
        result = sa.attach(array_name)
        cell, sub_range = block
        pprint(block)

        item_size = np.dtype(dtype).itemsize
        s3_start = (np.ravel_multi_index(cell+tuple([s.start for s in sub_range]), shape)) * item_size
        s3_end = (np.ravel_multi_index(cell+tuple([s.stop-1 for s in sub_range]), shape)+1) * item_size
        data = self.s3io.get_byte_range(s3_bucket, s3_key, s3_start, s3_end)

        t = [slice(x.start-o, x.stop-o) if isinstance(x, slice) else x-o for x, o in
             zip(cell+tuple(sub_range), offset)]
        if data.dtype != dtype:
            data = np.frombuffer(data, dtype=dtype, count=-1, offset=0)
            # data = data.reshape([s.stop - s.start for s in sub_range])

        # pprint(data)
        result[t] = data.reshape([s.stop - s.start for s in sub_range])

    def get_slice_mp(self, array_slice, shape, dtype, s3_bucket, s3_key):  # pylint: disable=too-many-locals
        cdim = self.cdims(array_slice, shape)

        try:
            end = cdim[::-1].index(False)+1
        except ValueError:
            end = len(shape)

        start = len(shape) - end

        outer = array_slice[:-end]
        outer_ranges = [range(s.start, s.stop) for s in outer]
        outer_cells = list(itertools.product(*outer_ranges))
        blocks = list(zip(outer_cells, itertools.repeat(array_slice[start:])))
        offset = [s.start for s in array_slice]

        num_processes = cpu_count()
        pool = Pool(num_processes)
        array_name = '_'.join(['SA3IO', str(uuid.uuid4()), str(os.getpid())])
        sa.create(array_name, shape=[s.stop - s.start for s in array_slice], dtype=dtype)
        shared_array = sa.attach(array_name)

        pool.map_async(self.work_get_slice, zip(blocks, repeat(array_name), repeat(offset), repeat(s3_bucket),
                                                repeat(s3_key), repeat(shape), repeat(dtype)))
        pool.close()
        pool.join()
        sa.delete(array_name)
        return shared_array

    def get_slice_by_bbox(self, array_slice, shape, dtype, s3_bucket, s3_key):  # pylint: disable=too-many-locals
        # Todo:
        #   - parallelise reads and writes
        #     - option 1. use get_byte_range_mp
        #     - option 2. smarter byte range subsets depending on:
        #       - data size
        #       - data contiguity

        item_size = np.dtype(dtype).itemsize
        s3_begin = (np.ravel_multi_index(tuple([s.start for s in array_slice]), shape)) * item_size
        s3_end = (np.ravel_multi_index(tuple([s.stop-1 for s in array_slice]), shape)+1) * item_size

        if s3_end-s3_begin <= 5*1024*1024:
            d = self.s3io.get_byte_range(s3_bucket, s3_key, s3_begin, s3_end)
        else:
            d = self.s3io.get_byte_range_mp(s3_bucket, s3_key, s3_begin, s3_end, 5*1024*1024)

        cdim = self.cdims(array_slice, shape)

        try:
            end = cdim[::-1].index(False)+1
        except ValueError:
            end = len(shape)

        start = len(shape) - end

        outer = array_slice[:-end]
        outer_ranges = [range(s.start, s.stop) for s in outer]
        outer_cells = list(itertools.product(*outer_ranges))
        blocks = list(zip(outer_cells, itertools.repeat(array_slice[start:])))
        item_size = np.dtype(dtype).itemsize

        results = []
        for cell, sub_range in blocks:
            s3_start = (np.ravel_multi_index(cell+tuple([s.start for s in sub_range]), shape)) * item_size
            s3_end = (np.ravel_multi_index(cell+tuple([s.stop-1 for s in sub_range]), shape)+1) * item_size
            data = d[s3_start-s3_begin:s3_end-s3_begin]
            results.append((cell, sub_range, data))

        result = np.empty([s.stop - s.start for s in array_slice], dtype=dtype)
        offset = [s.start for s in array_slice]

        for cell, sub_range, data in results:
            t = [slice(x.start-o, x.stop-o) if isinstance(x, slice) else x-o for x, o in
                 zip(cell+tuple(sub_range), offset)]
            if data.dtype != dtype:
                data = np.frombuffer(data, dtype=dtype, count=-1, offset=0)
            result[t] = data.reshape([s.stop - s.start for s in sub_range])

        return result

    # def shape_to_idx(self, slices):
    #     dims_but_last = slices[:-1]
    #     ranges = [range(0, s) for s in slices]
    #     cell_addresses = list(itertools.product(*ranges))
    #     return cell_addresses

    # def slice_to_idx(self, slices):
    #     # slices_but_last = slices[:-1]
    #     ranges = [range(s.start, s.stop) for s in slices]
    #     cell_addresses = list(itertools.product(*ranges))
    #     return cell_addresses
