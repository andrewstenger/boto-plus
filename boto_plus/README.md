## BatchPlus -- Public Functions
- `get_runtime_of_jobs(job_ids: list[str])`
- `get_status_of_jobs(job_ids: list[str])`

## DynamoPlus -- Public Functions
- `does_table_exist(table_name: str)`
- `get_record_with_primary_key_from_table(primary_key: str, primary_key_value: any, dynamo_table: str)`
- `get_record_with_composite_key_from_table(primary_key: str, primary_key_value: any, secondary_key: str, secondary_key_value: any, dynamo_table: str)`
- `get_records_with_attribute_from_table(attribute: str, attribute_value: any, dynamo_table: str)`
- `put_record_in_table(record: dict, dynamo_table: str)`

## S3Plus -- Public Functions
- `list_objects(bucket: str, prefix: str, filter='')`
- `list_all_versions_of_object(bucket: str, key: str)`

- `copy_object(source_bucket: str, source_key: str, target_bucket: str, target_key: str, dryrun=True, verbose=True)`
- `copy_objects(payloads: list[dict], use_multiprocessing=False)`

- `move_object(source_bucket: str, source_key: str, target_bucket: str, target_key: str, dryrun=True, verbose=True)`
- `move_objects(payloads: list[dict], use_multiprocessing=False, dryrun=True, verbose=True)`

- `delete_object(bucket: str, key: str, version_id=None, dryrun=True, verbose=True)`
- `delete_objects(payloads: list[dict], dryrun=True, verbose=True, use_multiprocessing=False)`
- `delete_objects_at_prefix(bucket: str, prefix: str, dryrun=True, verbose=True, use_multiprocessing=False)`
- `delete_all_versions_of_object(bucket: str, key: str, dryrun=True, verbose=True)`

- `upload_object(filepath: str, bucket: str, key: str, extra_args=None, kms_key=None, dryrun=True, verbose=True)`
- `upload_objects(local_directory: str, bucket: str, prefix: str, use_multiprocessing=False, dryrun=True, verbose=True)`

- `download_object(bucket: str, key: str, filepath: str)`
- `download_objects(bucket: str, prefix: str, local_directory: str, use_multiprocessing=False, dryrun=True, verbose=True)`

- `sync(source: str, target: str, use_multiprocessing=False, dryrun=True, verbose=True)`

- `does_object_exist(bucket: str, key: str)`
- `get_object_size(bucket: str, key: str)`
- `get_object_creation_datetime(bucket: str, key: str)`
- `get_object_hash(bucket: str, key: str)`
- `get_object_metadata(bucket: str, key: str)`
- `get_bucket_and_key_from_uri(uri: str)`
- `get_prefix_from_key(key: str)`

### To-Do
- create `delete_all_versions_of_all_objects_at_prefix()`
- 