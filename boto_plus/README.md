## S3Helper -- Public Functions
- list_objects(bucket: str, prefix: str, filter='')
- list_all_versions_of_object(bucket: str, key: str)

- copy_object(source_bucket: str, source_key: str, target_bucket: str, target_key: str)
- copy_objects(payloads: list[dict], use_multiprocessing=False)

- move_object(source_bucket: str, source_key: str, target_bucket: str, target_key: str)
- move_objects(payloads: list[dict], use_multiprocessing=False)

- delete_object(bucket: str, key: str, version_id=None, dryrun=True, verbose=True)
- delete_objects(payloads: list[dict], dryrun=True, verbose=True, use_multiprocessing=False)
- delete_objects_at_prefix(bucket: str, prefix: str, dryrun=True, verbose=True, use_multiprocessing=False)
- delete_all_versions_of_object(bucket: str, key: str, dryrun=True, verbose=True)

- upload_object(filepath: str, bucket: str, key: str, extra_args=None, kms_key=None)
- upload_objects(local_directory: str, bucket: str, prefix: str, use_multiprocessing=False)

- download_object(bucket: str, key: str, filepath: str)
- download_objects(bucket: str, prefix: str, local_directory: str, use_multiprocessing=False)

- sync(source: str, target: str, use_multiprocessing=False)

- does_object_exist(bucket: str, key: str)
- get_object_size(bucket: str, key: str)
- get_object_creation_datetime(bucket: str, key: str)
- get_object_hash(bucket: str, key: str)
- get_object_metadata(bucket: str, key: str)


### To-Do
- delete_all_versions_of_all_objects_at_prefix
- 