import os
import posixpath
import hashlib
import multiprocessing as mp
import boto3


class S3Helper:

    def __init__(
        self,
    ):
        self.__s3_resource = boto3.resource('s3')
        self.__s3_object_hash_field = 'x-amz-meta-object-hash'


    def list_objects(
        self,
        bucket: str,
        prefix: str,
        filter=''
    ):
        items  = list()
        kwargs = {
            'Bucket': bucket,
            'Prefix': prefix
        }

        while True:
            objects = self.__s3_resource.meta.client.list_objects_v2(**kwargs)
            if 'Contents' not in objects:
                return items

            for obj in objects['Contents']:
                key = obj['Key']
                if filter in key:
                    items.append(key)

            try:
                kwargs['ContinuationToken'] = objects['NextContinuationToken']

            except KeyError:
                break

        return items


    ### copy ###
    def copy_object(
        self,
        old_bucket: str,
        old_key: str,
        new_bucket: str,
        new_key: str,
    ):
        copy_source = {
            'Bucket' : old_bucket,
            'Key'    : old_key,
        }

        self.__s3_resource.meta.client.copy(copy_source, new_bucket, new_key)


    def copy_objects(
        self,
        payloads: list[dict],
        use_multiprocessing=False,
    ):
        if use_multiprocessing:
            with mp.Pool() as pool:
                pool.map(self.__copy_object_mp_unpack, payloads)

        else:
            for payload in payloads:
                self.copy_object(**payload)


    def __copy_object_mp_unpack(self, payload):
        return self.copy_object(**payload)


    ### move ###
    def move_objects(
        self,
        payloads: list[dict],
        use_multiprocessing=False,
    ):
        if use_multiprocessing:
            with mp.Pool() as pool:
                pool.map(self.__move_object_mp_unpack, payloads)

        else:
            for payload in payloads:
                self.move_object(**payload)


    def __move_object_mp_unpack(self, payload):
        return self.move_object(**payload)


    def move_object(
        self,
        old_bucket: str,
        old_key: str,
        new_bucket: str,
        new_key: str,
    ):
        self.copy_object(
            old_bucket=old_bucket,
            old_key=old_key,
            new_bucket=new_bucket,
            new_key=new_key,
        )

        self.delete_object(
            bucket=old_bucket,
            key=old_key,
        )


    ### delete ###
    def delete_object(
        self,
        bucket: str,
        key: str,
    ):
        self.__s3_resource.meta.client.delete_object(
            Bucket=bucket,
            Key=key,
        )


    ### upload ###
    def upload_objects(
        self,
        local_directory: str,
        bucket: str,
        prefix: str,
        use_multiprocessing=False,
    ):
        if use_multiprocessing:
            filepaths = [os.path.join(root, filename) for root, dirs, files in os.walk(local_directory) for filename in files]
            payloads  = [
                {
                    'filepath'        : filepath,
                    'local_directory' : local_directory,
                    'bucket'          : bucket,
                    'prefix'          : prefix,
                }
                for filepath in filepaths
            ]

            with mp.Pool() as pool:
                pool.map(self.__upload_object_mp_unpack, payloads)

        else:
            for root, dirs, files in os.walk(local_directory):
                for filename in files:
                    filepath = os.path.join(root, filename)
                    filepath_no_prefix = filepath.replace(local_directory, '')
                    partial_key = convert_filepath_to_posix(filepath_no_prefix)
                    key = posixpath.join(prefix, partial_key)

                    self.upload_object(
                        filepath=filepath,
                        bucket=bucket,
                        key=key,
                    )


    def __upload_object_mp_unpack(payload):
        filepath        = payload['filepath']
        local_directory = payload['local_directory']

        bucket = payload['bucket']
        prefix = payload['prefix']

        filepath_no_prefix = filepath.replace(local_directory, '')
        partial_key = convert_filepath_to_posix(filepath_no_prefix)
        key = posixpath.join(prefix, partial_key)

        self.upload_object(
            filepath=filepath,
            bucket=bucket,
            key=key,
        )


    def upload_object(
        self,
        filepath: str,
        bucket: str,
        key: str,
        extra_args=None,
    ):
        filepath_hash = get_local_file_hash(filepath)

        hash_args = {
            'Metadata' : {
                self.__s3_object_hash_field : filepath_hash,
            }
        }

        if extra_args is not None:
            all_args = {**hash_args, **extra_args}

        else:
            all_args = hash_args

        self.__s3_resource.meta.client.upload_file(
            Filename=filepath,
            Bucket=bucket,
            Key=key,
            ExtraArgs=all_args,
        )


    ### sync ###
    def sync(
        self,
        local_directory: str,
        bucket: str,
        prefix: str,
        use_multiprocessing=False,
    ):
        if use_multiprocessing:
            filepaths = [os.path.join(root, filename) for root, dirs, files in os.walk(local_directory) for filename in files]
            payloads  = [
                {
                    'filepath'        : filepath,
                    'local_directory' : local_directory,
                    'bucket'          : bucket,
                    'prefix'          : prefix,
                }
                for filepath in filepaths
            ]

            with mp.Pool() as pool:
                pool.map(self.__sync_mp, payloads)

        else:
            for root, dirs, files in os.walk(local_directory):
                for filename in files:
                    filepath = os.path.join(root, filename)
                    filepath_no_prefix = filepath.replace(local_directory, '')
                    partial_key = convert_filepath_to_posix(filepath_no_prefix)
                    key = posixpath.join(prefix, partial_key)

                    local_file_hash  = get_local_file_hash(filepath)
                    remote_file_hash = self.get_remote_file_hash(bucket=bucket, key=key)

                    if local_file_hash != remote_file_hash:
                        self.upload_object(
                            filepath=filepath,
                            bucket=bucket,
                            key=key,
                        )


    def __sync_mp(
        self,
        payload: str,
    ):
        filepath        = payload['filepath']
        local_directory = payload['local_directory']

        bucket = payload['bucket']
        prefix = payload['prefix']


        filepath_no_prefix = filepath.replace(local_directory, '')
        partial_key = convert_filepath_to_posix(filepath_no_prefix)
        key = posixpath.join(prefix, partial_key)

        local_file_hash  = get_local_file_hash(filepath)
        remote_file_hash = self.get_remote_file_hash(key)

        if local_file_hash != remote_file_hash:
            self.upload_object(
                filepath=filepath,
                bucket=bucket,
                key=key,
            )


    ### other ###
    def get_size_of_object(
        self,
        bucket: str,
        key: str,
    ) -> int:
        response = self.__s3_resource.meta.client.head_object(
            Bucket=bucket,
            Key=key,
        )

        if 'ContentLength' in response:
            size_in_bytes = response['ContentLength']
        else:
            raise RuntimeError(f'Provided object "s3://{bucket}/{key}" does not exist...')

        return size_in_bytes


    def get_remote_file_hash(
        self,
        bucket: str,
        key: str,
    ):
        metadata = self.get_remote_file_metadata(bucket=bucket, key=key)
        hash_value = metadata[self.__s3_object_hash_field]

        return hash_value


    def get_remote_file_metadata(
        self,
        bucket: str,
        key: str,
    ) -> dict:
        s3_object = self.__s3_resource.Object(bucket_name=bucket, key=key)
        metadata  = s3_object.metadata
        return metadata


def convert_filepath_to_posix(
    filepath,
):
    # Windows filepath provided
    if '\\' in filepath:
        # drive is specified -- it must be removed
        if ':' in filepath:
            idx = filepath.index(':')
            filepath_no_drive = filepath[idx+1:]
        else:
            filepath_no_drive = filepath

        output_filepath = filepath_no_drive.replace('\\', '/')
        
    else:
        output_filepath = filepath

    return output_filepath


def get_local_file_hash(filepath, chunk_size=4096):
    hash_md5 = hashlib.md5()
    with open(filepath, 'rb') as file:
        for chunk in iter(lambda: file.read(chunk_size), b''):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()

