import os
import boto3
import botocore
import moto
import unittest
import shutil

import boto_plus


class TestDynamoPlus(unittest.TestCase):

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
    def test_does_table_exist(self):
        dynamo = boto3.resource('dynamodb', region_name=self.region)

        dynamo.meta.client.create_table(
            TableName='mock-table',
            AttributeDefinitions=[
                {
                    'AttributeName': 'mock-field',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'mock-field',
                    'KeyType': 'HASH',
                },
            ],
            BillingMode='provisioned',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1,
            },
        )

        dynamo_plus = boto_plus.DynamoPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        # function finds created table and returns True
        self.assertTrue(dynamo_plus.does_table_exist(table_name='mock-table'))

        # function returns False if nonexistent table provided
        self.assertFalse(dynamo_plus.does_table_exist(table_name='nonexistent-mock-table'))


    @moto.mock_aws
    def test_get_record_with_primary_key_from_table(self):
        dynamo = boto3.resource('dynamodb', region_name=self.region)

        dynamo.meta.client.create_table(
            TableName='mock-table',
            AttributeDefinitions=[
                {
                    'AttributeName': 'mock-field',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'mock-field',
                    'KeyType': 'HASH',
                },
            ],
            BillingMode='provisioned',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1,
            },
        )

        dynamo_plus = boto_plus.DynamoPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        dynamo.Table('mock-table').put_item(
            Item={
                'mock-field' : 'abc123',
                'other-field' : 'test',
            }
        )

        # function retrieves record for the provided primary-key
        record = dynamo_plus.get_record_with_primary_key_from_table(
            pk='mock-field',
            pk_value='abc123',
            dynamo_table='mock-table',
        )

        self.assertEqual(record['other-field'], 'test')

        # function raises error if the primary-key field does not exist
        with self.assertRaises(botocore.exceptions.ClientError):
            dynamo_plus.get_record_with_primary_key_from_table(
                pk='nonexistent-mock-field',
                pk_value='def456',
                dynamo_table='mock-table',
            )

        # function returns empty list if the primary key field's value is not present in any records
        record = dynamo_plus.get_record_with_primary_key_from_table(
            pk='mock-field',
            pk_value='def456',
            dynamo_table='mock-table',
        )
        self.assertEqual(len(record), 0)



    @moto.mock_aws
    def test_get_record_with_composite_key_from_table(self):
        dynamo = boto3.resource('dynamodb', region_name=self.region)

        dynamo.meta.client.create_table(
            TableName='mock-table',
            AttributeDefinitions=[
                {
                    'AttributeName': 'mock-field-hash',
                    'AttributeType': 'S',
                },
                {
                    'AttributeName': 'mock-field-sort',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'mock-field-hash',
                    'KeyType': 'HASH',
                },
                {
                    'AttributeName': 'mock-field-sort',
                    'KeyType': 'RANGE',
                },
            ],
            BillingMode='provisioned',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1,
            },
        )

        dynamo_plus = boto_plus.DynamoPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        dynamo.Table('mock-table').put_item(
            Item={
                'mock-field-hash' : 'abc123',
                'mock-field-sort' : 'def456',
                'other-field' : 'test',
            }
        )

        # function retrieves record for the provided composite-key
        record = dynamo_plus.get_record_with_composite_key_from_table(
            pk='mock-field-hash',
            pk_value='abc123',
            sk='mock-field-sort',
            sk_value='def456',
            dynamo_table='mock-table',
        )

        self.assertEqual(record['other-field'], 'test')

        # function raises error if one of the composite-key fields does not exist
        with self.assertRaises(botocore.exceptions.ClientError):
            dynamo_plus.get_record_with_composite_key_from_table(
                pk='nonexistent-hash-field',
                pk_value='v1',
                sk='nonexistent-sort-field',
                sk_value='v2',
                dynamo_table='mock-table',
            )

        # function returns empty list if one of the composite-key field's value is not present in any records
        record = dynamo_plus.get_record_with_composite_key_from_table(
            pk='mock-field-hash',
            pk_value='def456',
            sk='mock-field-sort',
            sk_value='mock-value',
            dynamo_table='mock-table',
        )
        self.assertEqual(len(record), 0)


    @moto.mock_aws
    def test_get_records_with_attribute_from_table(self):
        dynamo = boto3.resource('dynamodb', region_name=self.region)

        dynamo.meta.client.create_table(
            TableName='mock-table',
            AttributeDefinitions=[
                {
                    'AttributeName': 'mock-field-hash',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'mock-field-hash',
                    'KeyType': 'HASH',
                },
            ],
            BillingMode='provisioned',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1,
            },
        )

        dynamo_plus = boto_plus.DynamoPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        dynamo.Table('mock-table').put_item(
            Item={
                'mock-field-hash' : 'abc123',
                'random-attribute' : 'mock-attribute-value',
                'other-field' : 'test1',
            }
        )
        dynamo.Table('mock-table').put_item(
            Item={
                'mock-field-hash' : 'def456',
                'random-attribute' : 'mock-attribute-value',
                'other-field' : 'test2',
            }
        )

        # function retrieves records for the provided attribute
        records = dynamo_plus.get_records_with_attribute_from_table(
            attribute='random-attribute',
            attribute_value='mock-attribute-value',
            dynamo_table='mock-table',
        )

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]['mock-field-hash'], 'abc123')
        self.assertEqual(records[1]['mock-field-hash'], 'def456')

        # function returns empty list if the attribute FIELD does not exist
        record = dynamo_plus.get_records_with_attribute_from_table(
            attribute='nonexistent-field',
            attribute_value='nonexistent-attribute-value',
            dynamo_table='mock-table',
        )
        self.assertEqual(len(record), 0)

        # function returns empty list if the attribute's value is not present in any records
        record = dynamo_plus.get_records_with_attribute_from_table(
            attribute='random-attribute',
            attribute_value='not-real',
            dynamo_table='mock-table',
        )
        self.assertEqual(len(record), 0)


    @moto.mock_aws
    def test_put_record_in_table(self):
        dynamo = boto3.resource('dynamodb', region_name=self.region)

        dynamo.meta.client.create_table(
            TableName='mock-table',
            AttributeDefinitions=[
                {
                    'AttributeName': 'mock-field-hash',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'mock-field-hash',
                    'KeyType': 'HASH',
                },
            ],
            BillingMode='provisioned',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1,
            },
        )

        dynamo_plus = boto_plus.DynamoPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        dynamo_plus.put_record_in_table(
            record={
                'mock-field-hash' : 'abc123',
                'random-attribute' : 'mock-attribute-value',
            },
            dynamo_table='mock-table',
        )

        # function retrieves records for the provided attribute
        response = dynamo.Table('mock-table').get_item(
            Key={
                'mock-field-hash' : 'abc123',
            }
        )

        self.assertEqual(response['Item']['random-attribute'], 'mock-attribute-value')


    @moto.mock_aws
    def test_delete_record_with_primary_key_from_table(self):
        dynamo = boto3.resource('dynamodb', region_name=self.region)

        dynamo.meta.client.create_table(
            TableName='mock-table',
            AttributeDefinitions=[
                {
                    'AttributeName': 'mock-field-hash',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'mock-field-hash',
                    'KeyType': 'HASH',
                },
            ],
            BillingMode='provisioned',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1,
            },
        )

        dynamo.Table('mock-table').put_item(
            Item={
                'mock-field-hash' : 'abc123',
            },
        )

        dynamo_plus = boto_plus.DynamoPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        dynamo_plus.delete_record_with_primary_key_from_table(
            pk='mock-field-hash',
            pk_value='abc123',
            dynamo_table='mock-table',
        )

        # record was deleted
        record = dynamo.Table('mock-table').get_item(
            Key={
                'mock-field-hash' : 'abc123',
            }
        )
        self.assertNotIn('Item', record)
              

    @moto.mock_aws
    def test_delete_record_with_composite_key_from_table(self):
        dynamo = boto3.resource('dynamodb', region_name=self.region)

        dynamo.meta.client.create_table(
            TableName='mock-table',
            AttributeDefinitions=[
                {
                    'AttributeName': 'mock-field-hash',
                    'AttributeType': 'S',
                },
                {
                    'AttributeName': 'mock-field-sort',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'mock-field-hash',
                    'KeyType': 'HASH',
                },
                {
                    'AttributeName': 'mock-field-sort',
                    'KeyType': 'RANGE',
                },
            ],
            BillingMode='provisioned',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1,
            },
        )

        dynamo.Table('mock-table').put_item(
            Item={
                'mock-field-hash' : 'abc123',
                'mock-field-sort' : 'sort123',
            },
        )

        dynamo_plus = boto_plus.DynamoPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        dynamo_plus.delete_record_with_composite_key_from_table(
            pk='mock-field-hash',
            pk_value='abc123',
            sk='mock-field-sort',
            sk_value='sort123',
            dynamo_table='mock-table',
        )

        # record was deleted
        record = dynamo.Table('mock-table').get_item(
            Key={
                'mock-field-hash' : 'abc123',
                'mock-field-sort' : 'sort123',
            }
        )
        self.assertNotIn('Item', record)


    @moto.mock_aws
    def test_delete_records_with_attribute_from_table(self):
        dynamo = boto3.resource('dynamodb', region_name=self.region)

        dynamo.meta.client.create_table(
            TableName='mock-table',
            AttributeDefinitions=[
                {
                    'AttributeName': 'mock-field-hash',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'mock-field-hash',
                    'KeyType': 'HASH',
                },
            ],
            BillingMode='provisioned',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1,
            },
        )

        dynamo.Table('mock-table').put_item(
            Item={
                'mock-field-hash' : 'abc123',
                'attribute-field' : 'attribute123',
            },
        )

        dynamo_plus = boto_plus.DynamoPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        dynamo_plus.delete_records_with_attribute_from_table(
            attribute='attribute-field',
            attribute_value='attribute123',
            pk='mock-field-hash',
            dynamo_table='mock-table',
        )

        # record was deleted
        record = dynamo.Table('mock-table').get_item(
            Key={
                'mock-field-hash' : 'abc123',
            }
        )
        self.assertNotIn('Item', record)
