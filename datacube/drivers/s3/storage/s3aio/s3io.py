'''
S3IO Class

Low level byte read/writes to S3

Single-threaded. set new_session = False
Multi-threaded. set new_session = True

'''

import io
import os
import uuid
import time
import boto3
import botocore
import numpy as np
from os.path import expanduser, exists
from itertools import repeat
from multiprocessing import Pool, freeze_support, cpu_count
import SharedArray as sa
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# pylint: disable=too-many-locals, too-many-public-methods


class S3IO:

    # enable_s3: True = reads/writes to s3
    # enable_s3: False = reads/writes to disk ***for testing only***
    def __init__(self, enable_s3=True, file_path=None):
        self.enable_s3 = enable_s3
        if file_path is None:
            self.file_path = expanduser("~")+"/S3IO/"
        else:
            self.file_path = file_path

    def list_created_arrays(self):
        result = [f for f in os.listdir("/dev/shm") if f.startswith('S3IO')]
        print(result)
        return result

    def delete_created_arrays(self):
        for a in self.list_created_arrays():
            sa.delete(a)

    def s3_resource(self, new_session=False):
        if not self.enable_s3:
            return
        if new_session is True:
            s3 = boto3.session.Session().resource('s3')
        else:
            s3 = boto3.resource('s3')
        return s3

    def s3_bucket(self, s3_bucket, new_session=False):
        if not self.enable_s3:
            return
        s3 = self.s3_resource(new_session)
        return s3.Bucket(s3_bucket)

    def s3_object(self, s3_bucket, s3_key, new_session=False):
        if not self.enable_s3:
            return
        s3 = self.s3_resource(new_session)
        return s3.Bucket(s3_bucket).Object(s3_key)

    def list_buckets(self, new_session=False):
        if self.enable_s3:
            try:
                s3 = self.s3_resource(new_session)
                return [b['Name'] for b in s3.meta.client.list_buckets()['Buckets']]
            except botocore.exceptions.ClientError as e:
                error_code = int(e.response['Error']['Code'])
                raise Exception("ClientError", error_code)
        else:
            directory = self.file_path
            return [f for f in os.listdir(directory) if os.path.isdir(os.path.join(directory, f))]

    def list_objects(self, s3_bucket, prefix='', max_keys=100, new_session=False):
        if self.enable_s3:
            try:
                s3 = self.s3_resource(new_session)
                objects = s3.meta.client.list_objects(Bucket=s3_bucket, Prefix=prefix, MaxKeys=max_keys)
                if 'Contents' not in objects:
                    return []
                return [o['Key'] for o in
                        s3.meta.client.list_objects(Bucket=s3_bucket, Prefix=prefix, MaxKeys=max_keys)['Contents']]
            except botocore.exceptions.ClientError as e:
                error_code = int(e.response['Error']['Code'])
                raise Exception("ClientError", error_code)
        else:
            directory = self.file_path+"/"+str(s3_bucket)
            return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

    def bucket_exists(self, s3_bucket, new_session=False):
        if self.enable_s3:
            try:
                s3 = self.s3_resource(new_session)
                s3.meta.client.head_bucket(Bucket=s3_bucket)
            except botocore.exceptions.ClientError as e:
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    return False
                raise Exception("ClientError", error_code)
            return True
        else:
            directory = self.file_path+"/"+str(s3_bucket)
            return os.path.exists(directory) and os.path.isdir(directory)

    def object_exists(self, s3_bucket, s3_key, new_session=False):
        if self.enable_s3:
            try:
                s3 = self.s3_resource(new_session)
                s3.meta.client.head_object(Bucket=s3_bucket, Key=s3_key)
            except botocore.exceptions.ClientError as e:
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    return False
                raise Exception("ClientError", error_code)
            return True
        else:
            directory = self.file_path+"/"+str(s3_bucket)+"/"+str(s3_key)
            return os.path.exists(directory) and os.path.isfile(directory)

    def put_bytes(self, s3_bucket, s3_key, data, new_session=False):
        # assert isinstance(data, memoryview), 'data must be a memoryview'
        if self.enable_s3:
            s3 = self.s3_resource(new_session)
            s3.meta.client.put_object(Bucket=s3_bucket, Key=s3_key, Body=io.BytesIO(data))
        else:
            directory = self.file_path+"/"+str(s3_bucket)
            if not os.path.exists(directory):
                os.makedirs(directory)
            f = open(directory+"/"+str(s3_key), "wb")
            f.write(data)
            f.close()

    # # functionality for byte range put does not exist in S3 API
    # # need to do a get, change the bytes in the byte range and upload_part
    # # do later
    # def put_byte_range(self, s3_bucket, s3_key, data, s3_start, s3_end, new_session=False):
    #     if new_session is True:
    #         s3 = boto3.session.Session().resource('s3')
    #     else:
    #         s3 = boto3.resource('s3')
    #     s3.meta.client.put_object(Bucket=s3_bucket, Key=s3_key, Range='bytes='+str(s3_start)+'-'+str(s3_end-1),
    #                               Body=io.BytesIO(data))

    def put_bytes_mpu(self, s3_bucket, s3_key, data, block_size, new_session=False):
        if not self.enable_s3:
            return self.put_bytes(s3_bucket, s3_key, data, new_session)

        assert isinstance(data, memoryview), 'data must be a memoryview'
        s3 = self.s3_resource(new_session)
        mpu = s3.meta.client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)

        num_blocks = int(np.ceil(data.nbytes/float(block_size)))
        parts_dict = dict(Parts=[])

        for block_number in range(num_blocks):
            part_number = block_number + 1
            start = block_number*block_size
            end = (block_number+1)*block_size
            if end > data.nbytes:
                end = data.nbytes
            data_chunk = io.BytesIO(data[start:end])

            response = s3.meta.client.upload_part(Bucket=s3_bucket,
                                                  Key=s3_key,
                                                  UploadId=mpu['UploadId'],
                                                  PartNumber=part_number,
                                                  Body=data_chunk)

            parts_dict['Parts'].append(dict(PartNumber=part_number, ETag=response['ETag']))

        mpu_response = s3.meta.client.complete_multipart_upload(Bucket=s3_bucket,
                                                                Key=s3_key,
                                                                UploadId=mpu['UploadId'],
                                                                MultipartUpload=parts_dict)
        return mpu_response

    def work_put(self, args):
        return self.work_put_impl(*args)

    def work_put_impl(self, block_number, data, s3_bucket, s3_key, block_size, mpu):
        response = boto3.resource('s3').meta.client.upload_part(Bucket=s3_bucket,
                                                                Key=s3_key,
                                                                UploadId=mpu['UploadId'],
                                                                PartNumber=block_number + 1,
                                                                Body=data)

        return dict(PartNumber=block_number + 1, ETag=response['ETag'])

    def put_bytes_mpu_mp(self, s3_bucket, s3_key, data, block_size, new_session=False):
        if not self.enable_s3:
            return self.put_bytes(s3_bucket, s3_key, data, new_session)

        s3 = self.s3_resource(new_session)

        mpu = s3.meta.client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
        num_blocks = int(np.ceil(data.nbytes/float(block_size)))
        data_chunks = []
        for block_number in range(num_blocks):
            start = block_number*block_size
            end = (block_number+1)*block_size
            if end > data.nbytes:
                end = data.nbytes
            data_chunks.append(io.BytesIO(data[start:end]))

        parts_dict = dict(Parts=[])
        blocks = range(num_blocks)
        num_processes = cpu_count()
        pool = Pool(num_processes)
        results = pool.map_async(self.work_put, zip(blocks, data_chunks, repeat(s3_bucket), repeat(s3_key),
                                                    repeat(block_size), repeat(mpu)))
        pool.close()
        pool.join()

        for result in results.get():
            parts_dict['Parts'].append(result)

        mpu_response = boto3.resource('s3').meta.client.complete_multipart_upload(Bucket=s3_bucket,
                                                                                  Key=s3_key,
                                                                                  UploadId=mpu['UploadId'],
                                                                                  MultipartUpload=parts_dict)

        return mpu_response

    def work_put_shm(self, args):
        return self.work_put_impl_shm(*args)

    def work_put_impl_shm(self, block_number, array_name, s3_bucket, s3_key, block_size, mpu):
        part_number = block_number + 1
        start = block_number*block_size
        end = (block_number+1)*block_size
        shared_array = sa.attach(array_name)
        data_chunk = io.BytesIO(shared_array.data[start:end])

        s3 = boto3.session.Session().resource('s3')
        # s3 = boto3.resource('s3')
        response = s3.meta.client.upload_part(Bucket=s3_bucket,
                                              Key=s3_key,
                                              UploadId=mpu['UploadId'],
                                              PartNumber=part_number,
                                              Body=data_chunk)

        return dict(PartNumber=part_number, ETag=response['ETag'])

    def put_bytes_mpu_mp_shm(self, s3_bucket, s3_key, array_name, block_size, new_session=False):
        if not self.enable_s3:
            data = sa.attach(array_name)
            return self.put_bytes(s3_bucket, s3_key, data, new_session)

        s3 = self.s3_resource(new_session)

        mpu = s3.meta.client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)

        shared_array = sa.attach(array_name)
        num_blocks = int(np.ceil(shared_array.nbytes/float(block_size)))
        parts_dict = dict(Parts=[])
        blocks = range(num_blocks)
        num_processes = cpu_count()
        pool = Pool(num_processes)
        results = pool.map_async(self.work_put_shm, zip(blocks, repeat(array_name), repeat(s3_bucket),
                                                        repeat(s3_key), repeat(block_size), repeat(mpu)))
        pool.close()
        pool.join()

        for result in results.get():
            parts_dict['Parts'].append(result)

        mpu_response = s3.meta.client.complete_multipart_upload(Bucket=s3_bucket,
                                                                Key=s3_key,
                                                                UploadId=mpu['UploadId'],
                                                                MultipartUpload=parts_dict)
        return mpu_response

    def get_bytes(self, s3_bucket, s3_key, new_session=False):
        if self.enable_s3:
            while True:
                s3 = self.s3_resource(new_session)
                b = s3.Bucket(s3_bucket)
                o = b.Object(s3_key)
                try:
                    d = o.get()['Body'].read()
                    # d = np.frombuffer(d, dtype=np.uint8, count=-1, offset=0)
                    return d
                except botocore.exceptions.ClientError as e:
                    break
                break
        else:
            directory = self.file_path+"/"+str(s3_bucket)
            if not os.path.exists(directory):
                return
            f = open(directory+"/"+str(s3_key), "rb")
            f.seek(0, 0)
            d = f.read()
            f.close()
            return d

    def get_byte_range(self, s3_bucket, s3_key, s3_start, s3_end, new_session=False):
        if self.enable_s3:
            while True:
                s3 = self.s3_resource(new_session)
                b = s3.Bucket(s3_bucket)
                o = b.Object(s3_key)
                try:
                    d = o.get(Range='bytes='+str(s3_start)+'-'+str(s3_end-1))['Body'].read()
                    d = np.frombuffer(d, dtype=np.uint8, count=-1, offset=0)
                    return d
                except botocore.exceptions.ClientError as e:
                    break
                break
        else:
            directory = self.file_path+"/"+str(s3_bucket)
            if not os.path.exists(directory):
                return
            f = open(directory+"/"+str(s3_key), "rb")
            f.seek(s3_start, 0)
            d = f.read(s3_end-s3_start)
            d = np.frombuffer(d, dtype=np.uint8, count=-1, offset=0)
            f.close()
            return d

    def work_get(self, args):
        return self.work_get_impl(*args)

    def work_get_impl(self, block_number, array_name, s3_bucket, s3_key, s3_max_size, block_size):
        start = block_number*block_size
        end = (block_number+1)*block_size
        if end > s3_max_size:
            end = s3_max_size
        d = self.get_byte_range(s3_bucket, s3_key, start, end, True)
        # d = np.frombuffer(d, dtype=np.uint8, count=-1, offset=0)
        shared_array = sa.attach(array_name)
        shared_array[start:end] = d

    def get_byte_range_mp(self, s3_bucket, s3_key, s3_start, s3_end, block_size, new_session=False):
        if not self.enable_s3:
            return self.get_byte_range(s3_bucket, s3_key, s3_start, s3_end, new_session)

        s3 = self.s3_resource(new_session)

        s3o = s3.Bucket(s3_bucket).Object(s3_key).get()
        s3_max_size = s3o['ContentLength']
        s3_obj_size = s3_end-s3_start
        num_streams = int(np.ceil(s3_obj_size/block_size))
        num_processes = cpu_count()
        pool = Pool(num_processes)
        blocks = range(num_streams)
        array_name = '_'.join(['S3IO', s3_bucket, s3_key, str(uuid.uuid4()), str(os.getpid())])
        sa.create(array_name, shape=(s3_obj_size), dtype=np.uint8)
        shared_array = sa.attach(array_name)

        pool.map_async(self.work_get, zip(blocks, repeat(array_name), repeat(s3_bucket), repeat(s3_key),
                                          repeat(s3_max_size), repeat(block_size)))
        pool.close()
        pool.join()
        sa.delete(array_name)
        return shared_array
