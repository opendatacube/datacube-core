import numpy as np
import pytest
import sys

sa = pytest.importorskip('SharedArray')


class TestS3LIO(object):
    def test_create_s3lio(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio
        s = s3aio.S3LIO()

    def test_chunk_indices_1d(self):
        import datacube.drivers.s3.storage.s3aio as s3aio
        s = s3aio.S3LIO()
        assert list(s.chunk_indices_1d(0, 10, 2)) == [slice(0, 2, None), slice(2, 4, None), slice(4, 6, None),
                                                      slice(6, 8, None), slice(8, 10, None)]
        assert list(s.chunk_indices_1d(0, 10, 2, slice(2, 8))) == [slice(2, 4, None), slice(4, 6, None),
                                                                   slice(6, 8, None)]
        assert list(s.chunk_indices_1d(0, 10, 2, slice(2, 8), True)) == [2, 2, 2]

    def test_chunk_indices_nd(self):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3LIO()
        x = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        assert (list(s.chunk_indices_nd(x.shape, (2, 2, 2))) ==
                [(slice(0, 2, None), slice(0, 2, None), slice(0, 2, None)),
                 (slice(0, 2, None), slice(0, 2, None), slice(2, 4, None)),
                 (slice(0, 2, None), slice(2, 4, None), slice(0, 2, None)),
                 (slice(0, 2, None), slice(2, 4, None), slice(2, 4, None)),
                 (slice(2, 4, None), slice(0, 2, None), slice(0, 2, None)),
                 (slice(2, 4, None), slice(0, 2, None), slice(2, 4, None)),
                 (slice(2, 4, None), slice(2, 4, None), slice(0, 2, None)),
                 (slice(2, 4, None), slice(2, 4, None), slice(2, 4, None))])

        assert list(s.chunk_indices_nd(x.shape, (2, 2, 2), (slice(1, 3), slice(1, 3), slice(1, 3)))) == [
            (slice(1, 2, None), slice(1, 2, None), slice(1, 2, None)), (slice(1, 2, None), slice(1, 2, None),
                                                                        slice(2, 3, None)),
            (slice(1, 2, None), slice(2, 3, None), slice(1, 2, None)), (slice(1, 2, None),
                                                                        slice(2, 3, None), slice(2, 3, None)),
            (slice(2, 3, None), slice(1, 2, None), slice(1, 2, None)),
            (slice(2, 3, None), slice(1, 2, None), slice(2, 3, None)), (slice(2, 3, None), slice(2, 3, None),
                                                                        slice(1, 2, None)),
            (slice(2, 3, None), slice(2, 3, None), slice(2, 3, None))]

        assert list(s.chunk_indices_nd(x.shape, (2, 2, 2), (slice(1, 3), slice(1, 3), slice(1, 3)), True)) == [
            (1, 1, 1), (1, 1, 1), (1, 1, 1), (1, 1, 1), (1, 1, 1), (1, 1, 1), (1, 1, 1), (1, 1, 1)]

    def test_put_array_in_s3_without_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3LIO(False, False, str(tmpdir))
        x = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        key_map = s.put_array_in_s3(x, (2, 2, 2), "base_name", 'arrayio')

        e = np.empty(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        keys = [a[0] for a in key_map]
        idx = [a[1] for a in key_map]
        e = s.assemble_array_from_s3(e, idx, 'arrayio', keys, np.uint8)
        assert np.array_equal(x, e)

    def test_put_array_in_s3_mp_without_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio
        s = s3aio.S3LIO(False, False, str(tmpdir))
        x = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        key_map = s.put_array_in_s3_mp(x, (2, 2, 2), "base_name", 'arrayio')

        e = np.empty(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        keys = [a[0] for a in key_map]
        idx = [a[1] for a in key_map]
        e = s.assemble_array_from_s3(e, idx, 'arrayio', keys, np.uint8)
        assert np.array_equal(x, e)

    def test_put_array_in_s3_with_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio
        s = s3aio.S3LIO(True, False, str(tmpdir))
        x = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        key_map = s.put_array_in_s3(x, (2, 2, 2), "base_name", 'arrayio')

        e = np.empty(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        keys = [a[0] for a in key_map]
        idx = [a[1] for a in key_map]
        e = s.assemble_array_from_s3(e, idx, 'arrayio', keys, np.uint8)
        assert np.array_equal(x, e)

    def test_put_array_in_s3_mp_with_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3LIO(True, False, str(tmpdir))
        x = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        key_map = s.put_array_in_s3_mp(x, (2, 2, 2), "base_name", 'arrayio')

        e = np.empty(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        keys = [a[0] for a in key_map]
        idx = [a[1] for a in key_map]
        e = s.assemble_array_from_s3(e, idx, 'arrayio', keys, np.uint8)
        assert np.array_equal(x, e)

    def test_regular_index(self):
        import datacube.drivers.s3.storage.s3aio as s3aio
        s = s3aio.S3LIO()
        i = s.regular_index((-35 + 2 * 0.128, 149 + 2 * 0.128), ((-35, -34), (149, 150)), (4000, 4000))
        assert i == [1024, 1024]

        i = s.regular_index((3, 1, 1), (5, 5, 5), (3, 3, 3))
        assert i == [1, 0, 0]
        i = s.regular_index((3, 1, 1), (5, 5, 5), (3, 3, 3), True)
        assert i == 4

    def test_get_data_with_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3LIO(True, False, str(tmpdir))

        x = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        key_map = s.put_array_in_s3(x, (2, 2, 2), "base_name", 'arrayio')

        d = s.get_data('base_name', (4, 4, 4), (2, 2, 2), np.int8, (slice(1, 3), slice(1, 3), slice(1, 3)), 'arrayio')
        d = s.get_data_unlabeled('base_name', (4, 4, 4), (2, 2, 2), np.int8, (slice(1, 3), slice(1, 3), slice(1, 3)),
                                 'arrayio')
        assert np.array_equal(x[1:3, 1:3, 1:3], d)

        d = s.get_data_unlabeled_mp('base_name', (4, 4, 4), (2, 2, 2), np.int8, (slice(1, 3), slice(1, 3), slice(1, 3)),
                                    'arrayio')
        assert np.array_equal(x[1:3, 1:3, 1:3], d)

    def test_get_data_without_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3LIO(False, False, str(tmpdir))

        x = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        key_map = s.put_array_in_s3(x, (2, 2, 2), "base_name", 'arrayio')

        d = s.get_data_unlabeled('base_name', (4, 4, 4), (2, 2, 2), np.int8, (slice(1, 3), slice(1, 3), slice(1, 3)),
                                 'arrayio')
        assert np.array_equal(x[1:3, 1:3, 1:3], d)

        d = s.get_data_unlabeled_mp('base_name', (4, 4, 4), (2, 2, 2), np.int8, (slice(1, 3), slice(1, 3), slice(1, 3)),
                                    'arrayio')
        assert np.array_equal(x[1:3, 1:3, 1:3], d)


# S3AIO

class TestS3AOI(object):
    def test_create_s3aio(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio
        s = s3aio.S3AIO()

    def test_1d_nd_converions(self):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3AIO()
        assert s.to_1d((3, 1, 4, 1), (6, 7, 8, 9)) == np.ravel_multi_index((3, 1, 4, 1), (6, 7, 8, 9))
        assert s.to_nd(1621, (6, 7, 8, 9)) == np.unravel_index(1621, (6, 7, 8, 9))

    def test_get_point_without_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3IO(False, str(tmpdir))
        data = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        s.put_bytes("arrayio", "array444", bytes(data.data))

        s = s3aio.S3AIO(False, False, str(tmpdir))
        d = s.get_point((0, 0, 0), (4, 4, 4), np.uint8, 'arrayio', 'array444')

        assert d == 0

    def test_get_point_with_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3IO(False, str(tmpdir))
        data = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        import zstd
        cctx = zstd.ZstdCompressor(level=9, write_content_size=True)
        data = cctx.compress(data)
        s.put_bytes("arrayio", "array444", bytes(data))

        s = s3aio.S3AIO(True, False, str(tmpdir))
        d = s.get_point((0, 0, 0), (4, 4, 4), np.uint8, 'arrayio', 'array444')

        assert d == 0

    def test_get_slice_with_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3IO(False, str(tmpdir))

        data = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        import zstd
        cctx = zstd.ZstdCompressor(level=9, write_content_size=True)
        cdata = cctx.compress(data)
        s.put_bytes("arrayio", "array444", bytes(cdata))

        s = s3aio.S3AIO(True, False, str(tmpdir))

        d = s.get_slice((slice(0, 2), slice(0, 4), slice(0, 4)), (4, 4, 4), np.uint8, 'arrayio', 'array444')
        assert np.array_equal(d, data[0:2, 0:4, 0:4])

        d = s.get_slice_mp((slice(0, 2), slice(0, 4), slice(0, 4)), (4, 4, 4), np.uint8, 'arrayio', 'array444')
        assert np.array_equal(d, data[0:2, 0:4, 0:4])

        d = s.get_slice_by_bbox((slice(0, 2), slice(0, 4), slice(0, 4)), (4, 4, 4), np.uint8, 'arrayio', 'array444')
        assert np.array_equal(d, data[0:2, 0:4, 0:4])

    def test_get_slice_without_compression(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3IO(False, str(tmpdir))

        data = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        s.put_bytes("arrayio", "array444", bytes(data.data))

        s = s3aio.S3AIO(False, False, str(tmpdir))

        d = s.get_slice((slice(0, 2), slice(0, 4), slice(0, 4)), (4, 4, 4), np.uint8, 'arrayio', 'array444')
        assert np.array_equal(d, data[0:2, 0:4, 0:4])

        d = s.get_slice_mp((slice(0, 2), slice(0, 4), slice(0, 4)), (4, 4, 4), np.uint8, 'arrayio', 'array444')
        assert np.array_equal(d, data[0:2, 0:4, 0:4])

        d = s.get_slice_by_bbox((slice(0, 2), slice(0, 4), slice(0, 4)), (4, 4, 4), np.uint8, 'arrayio', 'array444')
        assert np.array_equal(d, data[0:2, 0:4, 0:4])

        # S3IO


class TestS3IO(object):
    def test_create_s3io(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio
        s = s3aio.S3IO(False)
        s = s3aio.S3IO(False, str(tmpdir))

    def test_s3_resources(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio
        s = s3aio.S3IO(False, str(tmpdir))
        a = s.s3_resource()
        b = s.s3_bucket('bucket')
        b = s.s3_object('bucket', 'key')

    def test_put_bytes(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3IO(False, str(tmpdir))
        data = np.arange(20, dtype=np.uint8)
        s.put_bytes("arrayio", "1234test", bytes(data.data))

        assert s.bucket_exists('arrayio')
        assert s.object_exists('arrayio', '1234test')

        bs = s.list_buckets()
        assert 'arrayio' in bs
        l = s.list_objects('arrayio')
        assert '1234test' in l

        b = s.get_bytes('arrayio', '1234test')
        assert bytes(data.data) == bytes(b)
        b = s.get_byte_range('arrayio', '1234test', 1, 4)
        assert bytes(data[1:4]) == bytes(b)
        b = s.get_byte_range_mp('arrayio', '1234test', 1, 4, 2)
        assert bytes(data[1:4]) == bytes(b)

        s.delete_objects('arrayio', ['1234test'])
        assert not s.object_exists('arrayio', '1234test')

        l = s.list_objects('arrayio')
        assert '1234test' not in l

    def test_put_bytes_mpu(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3IO(False, str(tmpdir))
        data = np.arange(20, dtype=np.uint8)
        s.put_bytes_mpu("arrayio", "1234test", bytes(data.data), 10)

        assert s.bucket_exists('arrayio')
        assert s.object_exists('arrayio', '1234test')

        bs = s.list_buckets()
        assert 'arrayio' in bs
        l = s.list_objects('arrayio')
        assert '1234test' in l

        b = s.get_bytes('arrayio', '1234test')
        assert bytes(data.data) == bytes(b)
        b = s.get_byte_range('arrayio', '1234test', 1, 4)
        assert bytes(data[1:4]) == bytes(b)
        b = s.get_byte_range_mp('arrayio', '1234test', 1, 4, 2)
        assert bytes(data[1:4]) == bytes(b)

        s.delete_objects('arrayio', ['1234test'])
        assert not s.object_exists('arrayio', '1234test')

        l = s.list_objects('arrayio')
        assert '1234test' not in l

    def test_put_bytes_mpu_mp(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3IO(False, str(tmpdir))
        data = np.arange(20, dtype=np.uint8)
        s.put_bytes_mpu_mp("arrayio", "1234test", bytes(data.data), 10)

        assert s.bucket_exists('arrayio')
        assert s.object_exists('arrayio', '1234test')

        bs = s.list_buckets()
        assert 'arrayio' in bs
        l = s.list_objects('arrayio')
        assert '1234test' in l

        b = s.get_bytes('arrayio', '1234test')
        assert bytes(data.data) == bytes(b)
        b = s.get_byte_range('arrayio', '1234test', 1, 4)
        assert bytes(data[1:4]) == bytes(b)
        b = s.get_byte_range_mp('arrayio', '1234test', 1, 4, 2)
        assert bytes(data[1:4]) == bytes(b)

        s.delete_objects('arrayio', ['1234test'])
        assert not s.object_exists('arrayio', '1234test')

        l = s.list_objects('arrayio')
        assert '1234test' not in l

    @pytest.mark.skipif(sys.platform != 'linux',
                        reason='delete_created_arrays() only works on linux')
    def test_put_bytes_mpu_mp_shm(self, tmpdir):
        import datacube.drivers.s3.storage.s3aio as s3aio

        s = s3aio.S3IO(False, str(tmpdir))

        s.delete_created_arrays()
        assert not s.list_created_arrays()

        sa.create("S3_test_upload", shape=(20), dtype=np.uint8)
        assert len(s.list_created_arrays())
        data = sa.attach("S3_test_upload")
        data[:] = np.arange(20, dtype=np.uint8)
        s.put_bytes_mpu_mp_shm("arrayio", "1234test", "S3_test_upload", 10)
        sa.delete('S3_test_upload')

        assert s.bucket_exists('arrayio')
        assert s.object_exists('arrayio', '1234test')

        bs = s.list_buckets()
        assert 'arrayio' in bs
        l = s.list_objects('arrayio')
        assert '1234test' in l

        b = s.get_bytes('arrayio', '1234test')
        assert bytes(data.data) == bytes(b)
        b = s.get_byte_range('arrayio', '1234test', 1, 4)
        assert bytes(data[1:4]) == bytes(b)
        b = s.get_byte_range_mp('arrayio', '1234test', 1, 4, 2)
        assert bytes(data[1:4]) == bytes(b)

        s.delete_objects('arrayio', ['1234test'])
        assert not s.object_exists('arrayio', '1234test')

        l = s.list_objects('arrayio')
        assert '1234test' not in l
