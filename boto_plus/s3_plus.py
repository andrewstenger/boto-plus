import os
import posixpath
import multiprocessing as mp
import boto3
import botocore

import boto_plus


class S3Plus:

    def __init__(
        self,
        s3_resource=None,
    ):
        if s3_resource is None:
            self.__s3_resource = boto3.resource('s3')
        else:
            type_str = str(type(s3_resource))
            expected_type_str = "<class 'boto3.resources.factory.s3.ServiceResource'>"
            if type_str != expected_type_str:
                raise RuntimeError(f'The provided s3_resource variable is not of the expected type (expected "{type_str}", received "{expected_type_str}").')

            self.__s3_resource = s3_resource

        self.__s3_object_hash_field = 'x-amz-meta-object-hash'


    ### list ###
    def list_objects(
        self,
        bucket: str,
        prefix: str,
        filter=''
    ) -> list[str]:
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


    def list_all_versions_of_object(
        self,
        bucket: str,
        key: str,
    ) -> list[str]:
        versions = list()
        for version in self.__s3_resource.Bucket(bucket).object_versions.filter(Prefix=key):
            versions.append(version.id)

        return versions


    ### copy ###
    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        target_bucket: str,
        target_key: str,
        dryrun=True,
        verbose=True,
    ) -> str:
        if verbose:
            prefix = '(dryrun)' if dryrun else ''
            source_uri = f's3://{source_bucket}/{source_key}'
            target_uri = f's3://{target_bucket}/{target_key}'
            print(f'{prefix} Copying {source_uri} to {target_uri}...')

        if not dryrun:
            copy_source = {
                'Bucket' : source_bucket,
                'Key'    : source_key,
            }

            self.__s3_resource.meta.client.copy(
                CopySource=copy_source, 
                Bucket=target_bucket, 
                Key=target_key,
            )

        uri = f's3://{target_bucket}/{target_key}'
        return uri


    def copy_objects(
        self,
        payloads: list[dict],
        use_multiprocessing=False,
        dryrun=True,
        verbose=True,
    ) -> list[str]:
        if use_multiprocessing:
            for p in payloads:
                p['dryrun']  = dryrun
                p['verbose'] = verbose

            with mp.Pool() as pool:
                uris = pool.map(self.__copy_object_mp_unpack, payloads)

        else:
            uris = list()
            for payload in payloads:
                s3_uri = self.copy_object(**payload, dryrun=dryrun, verbose=verbose)
                uris.append(s3_uri)

        return uris


    ### move ###
    def move_objects(
        self,
        payloads: list[dict],
        use_multiprocessing=False,
        dryrun=True,
        verbose=True,
    ) -> list[str]:
        if use_multiprocessing:
            for p in payloads:
                p['dryrun']  = dryrun
                p['verbose'] = verbose

            with mp.Pool() as pool:
                uris = pool.map(self.__move_object_mp_unpack, payloads)

        else:
            uris = list()
            for payload in payloads:
                s3_uri = self.move_object(**payload, dryrun=dryrun, verbose=verbose)
                uris.append(s3_uri)

        return uris


    def move_object(
        self,
        source_bucket: str,
        source_key: str,
        target_bucket: str,
        target_key: str,
        dryrun=True,
        verbose=True,
    ) -> str:
        self.copy_object(
            source_bucket=source_bucket,
            source_key=source_key,
            target_bucket=target_bucket,
            target_key=target_key,
            dryrun=dryrun,
            verbose=verbose,
        )

        self.delete_object(
            bucket=source_bucket,
            key=source_key,
            dryrun=dryrun,
            verbose=verbose,
        )

        uri = f's3://{target_bucket}/{target_key}'
        return uri


    ### delete ###
    def delete_object(
        self,
        bucket: str,
        key: str,
        version_id=None,
        dryrun=True,
        verbose=True,
    ) -> str:
        if verbose:
            prefix = '(dryrun)' if dryrun else ''
            if version_id is not None:
                print(f'{prefix} Deleting version "{version_id}" of object "s3://{bucket}/{key}"...')
            else:
                print(f'{prefix} Deleting s3://{bucket}/{key}...')

        if not dryrun:
            if version_id is not None:
                self.__s3_resource.meta.client.delete_object(
                    Bucket=bucket,
                    Key=key,
                    VersionId=version_id,
                )
            else:
                self.__s3_resource.meta.client.delete_object(
                    Bucket=bucket,
                    Key=key,
                )

        uri = f's3://{bucket}/{key}'
        return uri


    def delete_objects(
        self,
        payloads: list[dict],
        dryrun=True,
        verbose=True,
        use_multiprocessing=False,
    ) -> list[str]:
        if use_multiprocessing:
            for p in payloads:
                p['dryrun']  = dryrun
                p['verbose'] = verbose

            with mp.Pool() as pool:
                uris = pool.map(self.__delete_object_mp_unpack, payloads)

        else:
            uris = list()
            for payload in payloads:
                s3_uri = self.delete_object(**payload, verbose=verbose, dryrun=dryrun)
                uris.append(s3_uri)

        return uris


    def delete_objects_at_prefix(
        self,
        bucket: str,
        prefix: str,
        dryrun=True,
        verbose=True,
        use_multiprocessing=False,
    ) -> list[str]:
        if use_multiprocessing:
            payloads = [
                {
                    'bucket'  : bucket,
                    'key'     : key,
                    'dryrun'  : dryrun,
                    'verbose' : verbose,
                }
                for key in self.list_objects(bucket=bucket, prefix=prefix)
            ]

            with mp.Pool() as pool:
                uris = pool.map(self.__delete_object_mp_unpack, payloads)

        else:
            uris = list()
            for key in self.list_objects(bucket=bucket, prefix=prefix):
                s3_uri = self.delete_object(
                    bucket=bucket,
                    key=key,
                    dryrun=dryrun,
                    verbose=verbose,
                )
                uris.append(s3_uri)

        return uris


    def delete_all_versions_of_object(
        self,
        bucket: str,
        key: str,
        dryrun=True,
        verbose=True,
    ) -> list[str]:
        versions = self.list_all_versions_of_object(bucket=bucket, key=key)
        for version in versions:
            versions.append(version)

            self.delete_object(
                bucket=bucket,
                key=key,
                version_id=version,
                verbose=verbose,
                dryrun=dryrun,
            )

        return versions


    ### upload ###
    def upload_objects(
        self,
        payloads: list[dict],
        use_multiprocessing=False,
        dryrun=True,
        verbose=True,
    ) -> list[str]:
        if use_multiprocessing:
            for p in payloads:
                p['dryrun']  = dryrun
                p['verbose'] = verbose

            with mp.Pool() as pool:
                uris = pool.map(self.__upload_object_mp_unpack, payloads)

        else:
            uris = list()
            for payload in payloads:
                s3_uri = self.upload_object(**payload, verbose=verbose, dryrun=dryrun)
                uris.append(s3_uri)

        return uris


    def upload_object(
        self,
        filepath: str,
        bucket: str,
        key: str,
        extra_args=None,
        kms_key=None,
        dryrun=True,
        verbose=True,
    ) -> str:
        filepath_hash = boto_plus.get_local_file_hash(filepath)

        hash_args = {
            'Metadata' : {
                self.__s3_object_hash_field : filepath_hash,
            }
        }

        if extra_args is not None:
            all_args = {**hash_args, **extra_args}

        else:
            all_args = hash_args

        if kms_key is not None:
            kms_args = {
                'ServerSideEncryption' : 'aws:kms',
                'SSEKMSKeyId'          : kms_key,
            }

            all_args = {**all_args, **kms_args}

        if verbose:
            prefix = '(dryrun)' if dryrun else ''
            target_uri = f's3://{bucket}/{key}'
            print(f'{prefix} Uploading "{filepath}" to "{target_uri}"...')

        if not dryrun:
            self.__s3_resource.meta.client.upload_file(
                Filename=filepath,
                Bucket=bucket,
                Key=key,
                ExtraArgs=all_args,
            )

        uri = f's3://{bucket}/{key}'
        return uri


    ### download ###
    def download_object(
        self,
        bucket: str,
        key: str,
        filepath: str,
        dryrun=True,
        verbose=True,
    ):
        directory = os.path.dirname(filepath)
        os.makedirs(directory, exist_ok=True)

        if verbose:
            prefix = '(dryrun)' if dryrun else ''
            source_uri = f's3://{bucket}/{key}'
            print(f'{prefix} Downloading "{source_uri}" to "{filepath}"...')

        if not dryrun:
            self.__s3_resource.meta.client.download_file(
                Bucket=bucket,
                Key=key,
                Filename=filepath,
            )


    def download_objects(
        self,
        payloads: list[dict],
        use_multiprocessing=False,
        dryrun=True,
        verbose=True,
    ):
            if use_multiprocessing:
                for p in payloads:
                    p['dryrun']  = dryrun
                    p['verbose'] = verbose

                with mp.Pool() as pool:
                    pool.map(self.__download_object_mp_unpack, payloads)

            else:
                for payload in payloads:
                    self.download_object(**payload, verbose=verbose, dryrun=dryrun)


    ### sync ###
    def sync(
        self,
        source: str,
        target: str,
        use_multiprocessing=False,
        dryrun=True,
        verbose=True,
    ):
        if not source.startswith('s3://') and not target.startswith('s3://'):
            raise RuntimeError(f'At least one of "source", "target" must be an S3 URI. (Received "{source}", "{target}")')

        sync_type = self.__get_sync_type(source, target)

        # run in parallel with multiprocessing
        if use_multiprocessing:
            if sync_type == 's3-to-s3':
                source_bucket, source_prefix = self.get_bucket_and_key_from_uri(source)
                target_bucket, target_prefix = self.get_bucket_and_key_from_uri(target)
                source_keys = self.list_objects(bucket=source_bucket, prefix=source_prefix)
                payloads = [
                    {
                        'source-bucket' : source_bucket,
                        'source-prefix' : source_prefix,
                        'source-key'    : key,
                        'target-bucket' : target_bucket,
                        'target-prefix' : target_prefix,
                        'sync-type'     : sync_type,
                        'dryrun'        : dryrun,
                        'verbose'       : verbose,
                    }
                    for key in source_keys
                ]

            elif sync_type == 'local-to-s3':
                source_filepaths = boto_plus.get_filepaths_in_directory(
                    local_directory=source,
                    recursive=True,
                )

                target_bucket, target_prefix = self.get_bucket_and_key_from_uri(target)
                payloads = [
                    {
                        'source-directory' : source,
                        'source-filepath'  : filepath,
                        'target-bucket'    : target_bucket,
                        'target-prefix'    : target_prefix, 
                        'sync-type'        : sync_type,
                        'dryrun'           : dryrun,
                        'verbose'          : verbose,
                    }
                    for filepath in source_filepaths
                ]

            elif sync_type == 's3-to-local':
                os.makedirs(target, exist_ok=True)
                source_bucket, source_prefix = self.get_bucket_and_key_from_uri(source)
                source_keys = self.list_objects(bucket=source_bucket, prefix=source_prefix)
                payloads = [
                    {
                        'source-bucket'    : source_bucket,
                        'source-prefix'    : source_prefix,
                        'source-key'       : key,
                        'target-directory' : target,
                        'sync-type'        : sync_type,
                        'dryrun'           : dryrun,
                        'verbose'          : verbose,
                    }
                    for key in source_keys
                ]

            with mp.Pool() as pool:
                pool.map(self.__sync_item, payloads)

        # run sequentially
        else:
            if sync_type == 's3-to-s3':
                source_bucket, source_prefix = self.get_bucket_and_key_from_uri(source)
                target_bucket, target_prefix = self.get_bucket_and_key_from_uri(target)
                source_keys = self.list_objects(bucket=source_bucket, prefix=source_prefix)

                for source_key in source_keys:
                    self.__sync_item({
                        'sync-type'     : sync_type,
                        'source-bucket' : source_bucket,
                        'source-prefix' : source_prefix,
                        'source-key'    : source_key,
                        'target-bucket' : target_bucket,
                        'target-prefix' : target_prefix,
                        'dryrun'        : dryrun,
                        'verbose'       : verbose,
                    })

            elif sync_type == 'local-to-s3':
                target_bucket, target_prefix = self.get_bucket_and_key_from_uri(target)
                source_filepaths = boto_plus.get_filepaths_in_directory(
                    local_directory=source,
                    recursive=True,
                )

                for source_filepath in source_filepaths:
                    self.__sync_item({
                        'sync-type'        : sync_type,
                        'source-directory' : source,
                        'source-filepath'  : source_filepath,
                        'target-bucket'    : target_bucket,
                        'target-prefix'    : target_prefix,
                        'dryrun'           : dryrun,
                        'verbose'          : verbose,
                    })

            elif sync_type == 's3-to-local':
                os.makedirs(target, exist_ok=True)
                source_bucket, source_prefix = self.get_bucket_and_key_from_uri(source)
                source_keys = self.list_objects(bucket=source_bucket, prefix=source_prefix)
                for source_key in source_keys:
                    self.__sync_item({
                        'sync-type'        : sync_type,
                        'source-bucket'    : source_bucket,
                        'source-prefix'    : source_prefix,
                        'source-key'       : source_key,
                        'target-directory' : target,
                        'dryrun'           : dryrun,
                        'verbose'          : verbose,
                    })


    def __sync_item(
        self,
        payload: dict,
    ):
        sync_type = payload['sync-type']
        dryrun    = payload['dryrun']
        verbose   = payload['verbose']

        if sync_type == 's3-to-s3':
            source_bucket = payload['source-bucket']
            source_prefix = payload['source-prefix']
            source_key    = payload['source-key']
            target_bucket = payload['target-bucket']
            target_prefix = payload['target-prefix']

            partial_target_key = source_key.replace(source_prefix, '')
            target_key = posixpath.join(target_prefix, partial_target_key)

            if self.does_object_exist(bucket=source_bucket, key=source_key):
                source_remote_file_hash = self.get_object_hash(bucket=source_bucket, key=source_key)
            else:
                uri = f's3://{source_bucket}/{source_key}'
                raise RuntimeError(f'The provided S3 object does not exist: "{uri}"')

            if self.does_object_exist(bucket=target_bucket, key=target_key):
                target_object_hash = self.get_object_hash(bucket=target_bucket, key=target_key)
            else:
                target_object_hash = None

            if source_remote_file_hash != target_object_hash:
                self.copy_object(
                    source_bucket=source_bucket,
                    source_key=source_key,
                    target_bucket=target_bucket,
                    target_key=target_key,
                    dryrun=dryrun,
                    verbose=verbose,
                )

        elif sync_type == 'local-to-s3':
            source_directory = payload['source-directory']
            source_filepath  = payload['source-filepath']
            target_bucket    = payload['target-bucket']
            target_prefix    = payload['target-prefix']

            filepath_no_prefix = source_filepath.replace(source_directory, '')
            partial_key = boto_plus.convert_filepath_to_posix(filepath_no_prefix)
            target_key  = posixpath.join(target_prefix, partial_key)

            if os.path.isfile(source_filepath):
                source_local_file_hash = boto_plus.get_local_file_hash(source_filepath)
            else:
                raise RuntimeError(f'The provided S3 object does not exist: "{source_filepath}"')

            if self.does_object_exist(bucket=target_bucket, key=target_key):
                target_object_hash = self.get_object_hash(bucket=target_bucket, key=target_key)
            else:
                target_object_hash = None

            if source_local_file_hash != target_object_hash:
                self.upload_object(
                    filepath=source_filepath,
                    bucket=target_bucket,
                    key=target_key,
                    dryrun=dryrun,
                    verbose=verbose,
                )

        elif sync_type == 's3-to-local':
            source_bucket = payload['source-bucket']
            source_prefix = payload['source-prefix']
            source_key    = payload['source-key']
            target_directory = payload['target-directory']

            partial_target_filepath = source_key.replace(source_prefix, '')

            if boto_plus.is_windows_filepath(target_directory):
                partial_target_filepath = boto_plus.convert_filepath_to_windows(partial_target_filepath)
                target_filepath = os.join(target_directory, partial_target_filepath)

            else:
                partial_target_filepath = boto_plus.convert_filepath_to_posix(partial_target_filepath)
                target_filepath = posixpath.join(target_directory, partial_target_filepath)

            if self.does_object_exist(bucket=source_bucket, key=source_key):
                source_remote_file_hash = self.get_object_hash(bucket=source_bucket, key=source_key)
            else:
                uri = f's3://{source_bucket}/{source_key}'
                raise RuntimeError(f'The provided S3 object does not exist: "{uri}"')

            if os.path.isfile(target_filepath):
                target_local_file_hash = boto_plus.get_local_file_hash(target_filepath)
            else:
                target_local_file_hash = None

            if source_remote_file_hash != target_local_file_hash:
                self.download_object(
                    bucket=source_bucket,
                    key=source_key,
                    filepath=target_filepath,
                    dryrun=dryrun,
                    verbose=verbose,
                )


    def __get_sync_type(
        self,
        source: str,
        target: str,
    ):
        sync_type = None
        if source.startswith('s3://') and target.startswith('s3://'):
            sync_type = 's3-to-s3'

        elif source.startswith('s3://') and not target.startswith('s3://'):
            sync_type = 's3-to-local'

        elif not source.startswith('s3://') and target.startswith('s3://'):
            sync_type = 'local-to-s3'

        return sync_type


    ### other ###
    def does_object_exist(
        self,
        bucket: str,
        key: str,
    ) -> bool:
        try:
            self.__s3_resource.meta.client.head_object(Bucket=bucket, Key=key)
            return True

        except botocore.exceptions.ClientError as exception:
            # S3 object not found
            if exception.response['Error']['Code'] == '404':
                return False

            else:
                # Something else has gone wrong -- raise error
                raise exception


    def get_object_size(
        self,
        bucket: str,
        key: str,
    ) -> int:
        size_in_bytes = None

        if self.does_object_exist(bucket=bucket, key=key):
            response = self.__s3_resource.meta.client.head_object(
                Bucket=bucket,
                Key=key,
            )

            size_in_bytes = response['ContentLength']

        else:
            raise RuntimeError(f'Provided S3 object "s3://{bucket}/{key}" does not exist...')

        return size_in_bytes


    def get_object_creation_datetime(
        self,
        bucket: str,
        key: str,
    ) -> str:
        creation_datetime = None

        if self.does_object_exist(bucket=bucket, key=key):
            s3_object = self.__s3_resource.Object(bucket, key)
            creation_datetime = s3_object.last_modified.strftime('%Y-%m-%d %H:%M:%S')

        else:
            raise RuntimeError(f'Provided S3 object "s3://{bucket}/{key}" does not exist...')

        return creation_datetime


    def get_object_hash(
        self,
        bucket: str,
        key: str,
    ) -> str:
        metadata = self.get_object_metadata(bucket=bucket, key=key)

        if self.__s3_object_hash_field in metadata:
            hash_value = metadata[self.__s3_object_hash_field]

        else:
            raise RuntimeError(f'Provided S3 object "s3://{bucket}/{key}" does not have metadata field "{self.__s3_object_hash_field}".')


        return hash_value


    def get_object_metadata(
        self,
        bucket: str,
        key: str,
    ) -> dict:
        if self.does_object_exist(bucket=bucket, key=key):
            s3_object = self.__s3_resource.Object(bucket_name=bucket, key=key)
            return s3_object.metadata

        else:
            raise RuntimeError(f'Provided S3 object "s3://{bucket}/{key}" does not exist.')


    def get_bucket_and_key_from_uri(
        self,
        uri: str,
    ) -> tuple:
        bucket = uri.replace('s3://', '').split('/')[0]
        key    = '/'.join(uri.replace('s3://', '').split('/')[1:])

        return bucket, key


    def get_prefix_from_key(
        self, 
        key: str,
    ) -> str:
        prefix = posixpath.dirname(key)
        return prefix


    ### helpers to unpack dictionary-records for multiprocessing ###
    def __download_object_mp_unpack(
        self,
        payload: dict,
    ):
        return self.download_object(**payload)


    def __copy_object_mp_unpack(
        self, 
        payload: dict,
    ):
        return self.copy_object(**payload)


    def __move_object_mp_unpack(
        self,
        payload: dict,
    ):
        return self.move_object(**payload)


    def __delete_object_mp_unpack(
        self, 
        payload: dict,
    ):
        return self.delete_object(**payload)


    def __upload_object_mp_unpack(
        self,
        payload: dict,
    ):
        return self.upload_object(**payload)


if __name__ == '__main__':
    s3_helper = boto_plus.S3Plus()
    s3_helper.list_objects(
        bucket='astenger-test',
        prefix='sync-test/'
    )
