"""
S3AIO Class

Array access to a single S3 object

"""
import SharedArray as sa
import zstd
from itertools import repeat, product

import numpy as np
from pathos.multiprocessing import ProcessingPool

from .s3io import S3IO, generate_array_name


class S3AIO(object):

    def __init__(self, enable_compression=True, enable_s3=True, file_path=None, num_workers=30):
        """Initialise the S3 array IO interface.

        :param bool enable_s3: Flag to store objects in s3 or disk.
            True: store in S3
            False: store on disk (for testing purposes)
        :param str file_path: The root directory for the emulated s3 buckets when enable_se is set to False.
        :param int num_workers: The number of workers for parallel IO.
        """
        self.s3io = S3IO(enable_s3, file_path, num_workers)

        self.pool = ProcessingPool(num_workers)
        self.enable_compression = enable_compression

    def to_1d(self, index, shape):
        """Converts nD index to 1D index.

        :param tuple index: N-D Index to be converted.
        :param tuple shape: Shape to be used for conversion.
        :return: Returns the 1D index.
        """
        return np.ravel_multi_index(index, shape)

    def to_nd(self, index, shape):
        """Converts 1D index to nD index.

        :param tuple index: 1D Index to be converted.
        :param tuple shape: Shape to be used for conversion.
        :return: Returns the ND index.
        """
        return np.unravel_index(index, shape)

    def get_point(self, index_point, shape, dtype, s3_bucket, s3_key):
        """Gets a point in the nd array stored in S3.

        Only works if compression is off.

        :param tuple index_point: Index of the point to be retrieved.
        :param tuple shape: Shape of the stored data.
        :param numpy.dtype: dtype of the stored data.
        :param str s3_bucket: S3 bucket name
        :param str s3_key: S3 key name
        :return: Returns the point data.
        """
        item_size = np.dtype(dtype).itemsize
        idx = self.to_1d(index_point, shape) * item_size
        if self.enable_compression:
            b = self.s3io.get_bytes(s3_bucket, s3_key)
            cctx = zstd.ZstdDecompressor()
            b = cctx.decompress(b)[idx:idx + item_size]
        else:
            b = self.s3io.get_byte_range(s3_bucket, s3_key, idx, idx + item_size)
        a = np.frombuffer(b, dtype=dtype, count=-1, offset=0)
        return a

    def cdims(self, slices, shape):
        return [sl.start == 0 and sl.stop == sh and (sl.step is None or sl.step == 1)
                for sl, sh in zip(slices, shape)]

    def get_slice(self, array_slice, shape, dtype, s3_bucket, s3_key):  # pylint: disable=too-many-locals
        """Gets a slice of the nd array stored in S3.

        Only works if compression is off.

        :param tuple array_slice: tuple of slices to retrieve.
        :param tuple shape: Shape of the stored data.
        :param numpy.dtype: dtype of the stored data.
        :param str s3_bucket: S3 bucket name
        :param str s3_key: S3 key name
        :return: Returns the data slice.
        """
        # convert array_slice into into sub-slices of maximum contiguous blocks

        # Todo:
        #   - parallelise reads and writes
        #     - option 1. get memory rows in parallel and merge
        #     - option 2. smarter byte range subsets depending on:
        #       - data size
        #       - data contiguity

        if self.enable_compression:
            return self.get_slice_by_bbox(array_slice, shape, dtype, s3_bucket, s3_key)

        # truncate array_slice to shape
        # array_slice = [slice(max(0, s.start) - min(sh, s.stop)) for s, sh in zip(array_sliced, shape)]
        array_slice = [slice(max(0, s.start), min(sh, s.stop)) for s, sh in zip(array_slice, shape)]

        cdim = self.cdims(array_slice, shape)

        try:
            end = cdim[::-1].index(False) + 1
        except ValueError:
            end = len(shape)

        start = len(shape) - end

        outer = array_slice[:-end]
        outer_ranges = [range(s.start, s.stop) for s in outer]
        outer_cells = list(product(*outer_ranges))
        blocks = list(zip(outer_cells, repeat(array_slice[start:])))
        item_size = np.dtype(dtype).itemsize

        results = []
        for cell, sub_range in blocks:
            # print(cell, sub_range)
            s3_start = (np.ravel_multi_index(cell + tuple([s.start for s in sub_range]), shape)) * item_size
            s3_end = (np.ravel_multi_index(cell + tuple([s.stop - 1 for s in sub_range]), shape) + 1) * item_size
            # print(s3_start, s3_end)
            data = self.s3io.get_byte_range(s3_bucket, s3_key, s3_start, s3_end)
            results.append((cell, sub_range, data))

        result = np.empty([s.stop - s.start for s in array_slice], dtype=dtype)
        offset = [s.start for s in array_slice]

        for cell, sub_range, data in results:
            t = [slice(x.start - o, x.stop - o) if isinstance(x, slice) else x - o for x, o in
                 zip(cell + tuple(sub_range), offset)]
            if data.dtype != dtype:
                data = np.frombuffer(data, dtype=dtype, count=-1, offset=0)
            result[t] = data.reshape([s.stop - s.start for s in sub_range])

        return result

    def get_slice_mp(self, array_slice, shape, dtype, s3_bucket, s3_key):  # pylint: disable=too-many-locals
        """Gets a slice of the nd array stored in S3 in parallel.

        Only works if compression is off.

        :param tuple array_slice: tuple of slices to retrieve.
        :param tuple shape: Shape of the stored data.
        :param numpy.dtype: dtype of the stored data.
        :param str s3_bucket: S3 bucket name
        :param str s3_key: S3 key name
        :return: Returns the data slice.
        """

        # pylint: disable=too-many-locals
        def work_get_slice(block, array_name, offset, s3_bucket, s3_key, shape, dtype):
            result = sa.attach(array_name)
            cell, sub_range = block

            item_size = np.dtype(dtype).itemsize
            s3_start = (np.ravel_multi_index(cell + tuple([s.start for s in sub_range]), shape)) * item_size
            s3_end = (np.ravel_multi_index(cell + tuple([s.stop - 1 for s in sub_range]), shape) + 1) * item_size
            data = self.s3io.get_byte_range(s3_bucket, s3_key, s3_start, s3_end)

            t = [slice(x.start - o, x.stop - o) if isinstance(x, slice) else x - o for x, o in
                 zip(cell + tuple(sub_range), offset)]
            if data.dtype != dtype:
                data = np.frombuffer(data, dtype=dtype, count=-1, offset=0)
                # data = data.reshape([s.stop - s.start for s in sub_range])

            result[t] = data.reshape([s.stop - s.start for s in sub_range])

        if self.enable_compression:
            return self.get_slice_by_bbox(array_slice, shape, dtype, s3_bucket, s3_key)

        cdim = self.cdims(array_slice, shape)

        try:
            end = cdim[::-1].index(False) + 1
        except ValueError:
            end = len(shape)

        start = len(shape) - end

        outer = array_slice[:-end]
        outer_ranges = [range(s.start, s.stop) for s in outer]
        outer_cells = list(product(*outer_ranges))
        blocks = list(zip(outer_cells, repeat(array_slice[start:])))
        offset = [s.start for s in array_slice]

        array_name = generate_array_name('S3AIO')
        sa.create(array_name, shape=[s.stop - s.start for s in array_slice], dtype=dtype)
        shared_array = sa.attach(array_name)

        self.pool.map(work_get_slice, blocks, repeat(array_name), repeat(offset), repeat(s3_bucket),
                      repeat(s3_key), repeat(shape), repeat(dtype))

        sa.delete(array_name)
        return shared_array

    def get_slice_by_bbox(self, array_slice, shape, dtype, s3_bucket, s3_key):  # pylint: disable=too-many-locals
        """Gets a slice of the nd array stored in S3 by bounding box.

        :param tuple array_slice: tuple of slices to retrieve.
        :param tuple shape: Shape of the stored data.
        :param numpy.dtype: dtype of the stored data.
        :param str s3_bucket: S3 bucket name
        :param str s3_key: S3 key name
        :return: Returns the data slice.
        """
        # Todo:
        #   - parallelise reads and writes
        #     - option 1. use get_byte_range_mp
        #     - option 2. smarter byte range subsets depending on:
        #       - data size
        #       - data contiguity

        item_size = np.dtype(dtype).itemsize
        s3_begin = (np.ravel_multi_index(tuple([s.start for s in array_slice]), shape)) * item_size
        s3_end = (np.ravel_multi_index(tuple([s.stop - 1 for s in array_slice]), shape) + 1) * item_size

        # if s3_end-s3_begin <= 5*1024*1024:
        #     d = self.s3io.get_byte_range(s3_bucket, s3_key, s3_begin, s3_end)
        # else:
        #     d = self.s3io.get_byte_range_mp(s3_bucket, s3_key, s3_begin, s3_end, 5*1024*1024)

        d = self.s3io.get_bytes(s3_bucket, s3_key)

        if self.enable_compression:
            cctx = zstd.ZstdDecompressor()
            d = cctx.decompress(d)

        d = np.frombuffer(d, dtype=np.uint8, count=-1, offset=0)
        d = d[s3_begin:s3_end]

        cdim = self.cdims(array_slice, shape)

        try:
            end = cdim[::-1].index(False) + 1
        except ValueError:
            end = len(shape)

        start = len(shape) - end

        outer = array_slice[:-end]
        outer_ranges = [range(s.start, s.stop) for s in outer]
        outer_cells = list(product(*outer_ranges))
        blocks = list(zip(outer_cells, repeat(array_slice[start:])))
        item_size = np.dtype(dtype).itemsize

        results = []
        for cell, sub_range in blocks:
            s3_start = (np.ravel_multi_index(cell + tuple([s.start for s in sub_range]), shape)) * item_size
            s3_end = (np.ravel_multi_index(cell + tuple([s.stop - 1 for s in sub_range]), shape) + 1) * item_size
            data = d[s3_start - s3_begin:s3_end - s3_begin]
            results.append((cell, sub_range, data))

        result = np.empty([s.stop - s.start for s in array_slice], dtype=dtype)
        offset = [s.start for s in array_slice]

        for cell, sub_range, data in results:
            t = [slice(x.start - o, x.stop - o) if isinstance(x, slice) else x - o for x, o in
                 zip(cell + tuple(sub_range), offset)]
            if data.dtype != dtype:
                data = np.frombuffer(data, dtype=dtype, count=-1, offset=0)
            result[tuple(t)] = data.reshape([s.stop - s.start for s in sub_range])

        return result
