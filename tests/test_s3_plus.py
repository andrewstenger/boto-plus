import os
import boto3
import botocore
import moto
import unittest
import shutil

import boto_plus


class TestS3Plus(unittest.TestCase):

    def setUp(self):
        self.region = 'us-east-1'
        self.boto_config = botocore.config.Config(region_name=self.region)
        self.boto_session = boto3.session.Session()

        self.data_directory = 'data'
        if os.path.isdir(self.data_directory):
            shutil.rmtree(self.data_directory)
        os.makedirs(self.data_directory, exist_ok=False)


    def tearDown(self):
        #shutil.rmtree(self.data_directory)
        pass


    @moto.mock_aws
    def test_list_objects(self):
        # setup
        s3 = boto3.client('s3')
        mock_bucket = 'test-bucket'
        s3.create_bucket(Bucket=mock_bucket)
        s3.put_object(Bucket=mock_bucket, Key='path/to/file.txt', Body='test-content-1')
        s3.put_object(Bucket=mock_bucket, Key='path/to/another/file.txt', Body='test-content-2')
        s3.put_object(Bucket=mock_bucket, Key='test.jpg')

        s3_plus = boto_plus.S3Plus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        # test 1 -- general test of provided prefix
        objects = s3_plus.list_objects(bucket=mock_bucket, prefix='path/')
        self.assertIn('path/to/file.txt', objects)
        self.assertIn('path/to/another/file.txt', objects)

        # test 2 -- test the filter
        objects = s3_plus.list_objects(bucket=mock_bucket, prefix='', filter='test')
        self.assertIn('test.jpg', objects)
        self.assertNotIn('path/to/file.txt', objects)
        self.assertNotIn('path/to/another/file.txt', objects)


    @moto.mock_aws
    def test_upload_object(self):
        # setup
        dryrun = False
        verbose = False
        s3 = boto3.resource('s3')
        mock_bucket = 'test-bucket'
        mock_key = 'fake/key.txt'
        s3.meta.client.create_bucket(Bucket=mock_bucket)

        local_filepath = '/tmp/test-upload-object.txt'
        mock_content = 'mock text file content'
        boto_plus.create_textfile(
            filepath=local_filepath,
            content=mock_content,
        )

        s3_plus = boto_plus.S3Plus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        # test 1 -- assert object and content are uploaded
        s3_plus.upload_object(
            filepath=local_filepath,
            bucket=mock_bucket,
            key=mock_key,
            extra_args={
                'Metadata' : {
                    'x-amz-meta-object-hash' : 'abc123'
                }
            },
            dryrun=dryrun,
            verbose=verbose,
        )
        response = s3.Object(bucket_name=mock_bucket, key=mock_key).get()
        response_content = response['Body'].read().decode('utf-8')
        self.assertEqual(mock_content, response_content)

        # test 2 -- assert associated metadata for object hash is uploaded
        s3_object = s3.Object(bucket_name=mock_bucket, key=mock_key)
        metadata  = s3_object.metadata
        self.assertEqual(metadata['x-amz-meta-object-hash'], 'abc123')

        # cleanup
        os.remove(local_filepath)


    @moto.mock_aws
    def test_copy_object(self):
        # setup
        dryrun = False
        verbose = False
        s3 = boto3.resource('s3')
        mock_bucket = 'test-bucket'
        source_mock_key = 'fake/key.txt'
        target_mock_key = 'fake/nested/directory/key2.txt'
        mock_content = 'this is some mock content'
        s3.meta.client.create_bucket(Bucket=mock_bucket)
        s3.meta.client.put_object(Bucket=mock_bucket, Key=source_mock_key, Body=mock_content, Metadata={'x-amz-meta-object-hash' : 'abc123'})

        s3_plus = boto_plus.S3Plus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        s3_plus.copy_object(
            source_bucket=mock_bucket,
            source_key=source_mock_key,
            target_bucket=mock_bucket,
            target_key=target_mock_key,
            dryrun=dryrun,
            verbose=verbose,
        )

        # old key was copied to the new location...
        s3_object = s3.Object(bucket_name=mock_bucket, key=target_mock_key)
        content   = s3_object.get()['Body'].read().decode('utf-8')
        self.assertEqual(content, mock_content)

        # and its metadata was copied too
        self.assertEqual(s3_object.metadata['x-amz-meta-object-hash'], 'abc123')


    @moto.mock_aws
    def test_move_object(self):
        # setup
        dryrun = False
        verbose = False
        s3 = boto3.resource('s3')
        mock_bucket  = 'test-bucket'
        source_mock_key = 'fake/key.txt'
        target_mock_key = 'fake/nested/directory/key2.txt'
        mock_content = 'this is some mock content'
        s3.meta.client.create_bucket(Bucket=mock_bucket)
        s3.meta.client.put_object(Bucket=mock_bucket, Key=source_mock_key, Body=mock_content, Metadata={'x-amz-meta-object-hash' : 'abc123'})

        s3_plus = boto_plus.S3Plus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        s3_plus.move_object(
            source_bucket=mock_bucket,
            source_key=source_mock_key,
            target_bucket=mock_bucket,
            target_key=target_mock_key,
            dryrun=dryrun,
            verbose=verbose,
        )

        # old key was destroyed...
        self.assertRaises(
            botocore.errorfactory.ClientError,
            s3.Object(bucket_name=mock_bucket, key=source_mock_key).get
        )

        # but first it was copied to the new location...
        s3_object = s3.Object(bucket_name=mock_bucket, key=target_mock_key)
        content   = s3_object.get()['Body'].read().decode('utf-8')
        self.assertEqual(content, mock_content)

        # and its metadata was copied too
        self.assertEqual(s3_object.metadata['x-amz-meta-object-hash'], 'abc123')


    @moto.mock_aws
    def test_delete_object(self):
        # setup
        dryrun = False
        verbose = False
        s3 = boto3.resource('s3')
        mock_bucket  = 'test-bucket'
        mock_key = 'fake/nested/directory/key.txt'
        mock_content = 'this is some mock content'
        s3.meta.client.create_bucket(Bucket=mock_bucket)
        s3.meta.client.put_object(Bucket=mock_bucket, Key=mock_key, Body=mock_content, Metadata={'x-amz-meta-object-hash' : 'abc123'})

        s3_plus = boto_plus.S3Plus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        s3_plus.delete_object(
            bucket=mock_bucket,
            key=mock_key,
            dryrun=dryrun,
            verbose=verbose,
        )

        # key was destroyed...
        self.assertRaises(
            botocore.errorfactory.ClientError,
            s3.Object(bucket_name=mock_bucket, key=mock_key).get
        )


    @moto.mock_aws
    def test_sync_s3_to_s3(self):
        ### test 1 -- s3-to-s3 sync ###
        # setup
        dryrun = False
        verbose = False
        s3 = boto3.resource('s3')
        mock_bucket = 'test-bucket'

        s3.meta.client.create_bucket(Bucket=mock_bucket)

        s3_plus = boto_plus.S3Plus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        s3.meta.client.put_object(Bucket=mock_bucket, Key='s3-to-s3/inputs/subjects/1001/metadata/1001-metadata.csv', Body='column1\nvalue', Metadata={'x-amz-meta-object-hash' : 'abc123'})
        s3.meta.client.put_object(Bucket=mock_bucket, Key='s3-to-s3/inputs/subjects/1002/metadata/1002-metadata.csv', Body='column1\nvalue', Metadata={'x-amz-meta-object-hash' : 'def456'})
        s3.meta.client.put_object(Bucket=mock_bucket, Key='s3-to-s3/inputs/subjects/10002/data/10001-image.nii', Body='', Metadata={'x-amz-meta-object-hash' : 'ghi789'})

        source = f's3://{mock_bucket}/s3-to-s3/inputs/'
        target = f's3://{mock_bucket}/s3-to-s3/outputs/'

        uris = s3_plus.sync(
            source=source,
            target=target,
            use_multiprocessing=False,
            dryrun=dryrun,
            verbose=verbose,
        )

        # if object hash metadata values are equal, the object was synced correctly
        s3_object = s3.Object(bucket_name=mock_bucket, key='s3-to-s3/outputs/subjects/1001/metadata/1001-metadata.csv')
        self.assertEqual(s3_object.metadata['x-amz-meta-object-hash'], 'abc123')

        s3_object = s3.Object(bucket_name=mock_bucket, key='s3-to-s3/outputs/subjects/1002/metadata/1002-metadata.csv')
        self.assertEqual(s3_object.metadata['x-amz-meta-object-hash'], 'def456')

        s3_object = s3.Object(bucket_name=mock_bucket, key='s3-to-s3/outputs/subjects/10002/data/10001-image.nii')
        self.assertEqual(s3_object.metadata['x-amz-meta-object-hash'], 'ghi789')

        # returned uris are as-expected
        self.assertIn(f's3://{mock_bucket}/s3-to-s3/outputs/subjects/1001/metadata/1001-metadata.csv', uris)
        self.assertIn(f's3://{mock_bucket}/s3-to-s3/outputs/subjects/1002/metadata/1002-metadata.csv', uris)
        self.assertIn(f's3://{mock_bucket}/s3-to-s3/outputs/subjects/10002/data/10001-image.nii', uris)


    @moto.mock_aws
    def test_sync_s3_to_local(self):
        ### test 2 -- s3-to-local sync ###
        # setup
        dryrun = False
        verbose = False
        s3 = boto3.resource('s3')
        mock_bucket = 'test-bucket'

        s3.create_bucket(Bucket=mock_bucket)

        s3_plus = boto_plus.S3Plus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        s3.meta.client.put_object(Bucket=mock_bucket, Key='s3-to-local/inputs/subjects/1001/metadata/1001-metadata.csv', Body='column1\nvalue', Metadata={'x-amz-meta-object-hash' : 'abc123'})
        s3.meta.client.put_object(Bucket=mock_bucket, Key='s3-to-local/inputs/subjects/1002/metadata/1002-metadata.csv', Body='column2\nvalue', Metadata={'x-amz-meta-object-hash' : 'def456'})
        s3.meta.client.put_object(Bucket=mock_bucket, Key='s3-to-local/inputs/subjects/1002/data/1002.txt', Body='test-1002', Metadata={'x-amz-meta-object-hash' : 'ghi789'})

        filepaths = s3_plus.sync(
            source=f's3://{mock_bucket}/s3-to-local/inputs/',
            target=f'data/s3-to-local/local-outputs/',
            use_multiprocessing=False,
            dryrun=dryrun,
            verbose=verbose,
        )

        path = 'data/s3-to-local/local-outputs/subjects/1001/metadata/1001-metadata.csv'
        self.assertTrue(os.path.isfile(path))
        self.assertEqual(boto_plus.get_textfile_content(path), 'column1\nvalue')

        path = 'data/s3-to-local/local-outputs/subjects/1002/metadata/1002-metadata.csv'
        self.assertTrue(os.path.isfile(path))
        self.assertEqual(boto_plus.get_textfile_content(path), 'column2\nvalue')

        path = 'data/s3-to-local/local-outputs/subjects/1002/data/1002.txt'
        self.assertTrue(os.path.isfile(path))
        self.assertEqual(boto_plus.get_textfile_content(path), 'test-1002')

        # returned filepaths are as-expected
        self.assertIn('data/s3-to-local/local-outputs/subjects/1001/metadata/1001-metadata.csv', filepaths)
        self.assertIn('data/s3-to-local/local-outputs/subjects/1002/metadata/1002-metadata.csv', filepaths)
        self.assertIn('data/s3-to-local/local-outputs/subjects/1002/data/1002.txt', filepaths)

        shutil.rmtree('data/s3-to-local/')


    @moto.mock_aws
    def test_sync_local_to_s3(self):
        ### test 3 -- local-to-s3 sync ###
        # setup
        dryrun = False
        verbose = False
        s3 = boto3.resource('s3')
        mock_bucket = 'test-bucket'

        s3.meta.client.create_bucket(Bucket=mock_bucket)

        s3_plus = boto_plus.S3Plus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        os.makedirs('data/local-to-s3/local-inputs/subject/1001/', exist_ok=False)
        os.makedirs('data/local-to-s3/local-inputs/subject/1002/nested/', exist_ok=False)

        boto_plus.create_textfile(
            content='this is the first test',
            filepath='data/local-to-s3/local-inputs/subject/1001/test-file-1.txt',
        )

        boto_plus.create_textfile(
            content='this is the second test',
            filepath='data/local-to-s3/local-inputs/subject/1002/nested/test-file-2.txt',
        )

        uris = s3_plus.sync(
            source=f'data/local-to-s3/local-inputs/',
            target=f's3://{mock_bucket}/local-to-s3/outputs/',
            use_multiprocessing=False,
            dryrun=dryrun,
            verbose=verbose,
        )

        s3_object = s3.Object(bucket_name=mock_bucket, key='local-to-s3/outputs/subject/1001/test-file-1.txt')
        content   = s3_object.get()['Body'].read().decode('utf-8')
        self.assertEqual(content, 'this is the first test')

        s3_object = s3.Object(bucket_name=mock_bucket, key='local-to-s3/outputs/subject/1002/nested/test-file-2.txt')
        content   = s3_object.get()['Body'].read().decode('utf-8')
        self.assertEqual(content, 'this is the second test')

        # returned uris are as-expected
        self.assertIn(f's3://{mock_bucket}/local-to-s3/outputs/subject/1001/test-file-1.txt', uris)
        self.assertIn(f's3://{mock_bucket}/local-to-s3/outputs/subject/1002/nested/test-file-2.txt', uris)

        shutil.rmtree('data/local-to-s3/')


if __name__ == "__main__":
    unittest.main()
