import boto3
import botocore


class DynamoPlus:

    def __init__(
        self,
        boto_config,
        boto_session=None,
    ):
        if boto_session is not None:
            self.__dynamo_resource = boto_session.resource('dynamodb', config=boto_config)
        else:
            self.__dynamo_resource = boto3.resource('dynamodb', config=boto_config)


    def does_table_exist(
        self,
        table_name: str,
    ) -> bool:
        try:
            table = self.__dynamo_resource.Table(table_name)
            # This line will trigger an exception if the table does not exist
            table.load()
            return True

        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            else:
                raise exception


    def get_record_with_primary_key_from_table(
        self,
        pk: str,
        pk_value: any,
        dynamo_table: str,
    ) -> dict:
        response = self.__dynamo_resource.Table(dynamo_table).get_item(
            Key={
                pk : pk_value,
            }
        )

        if 'Item' in response:
            item = response['Item']

        return item


    def get_record_with_composite_key_from_table(
        self,
        pk: str,
        pk_value: any,
        sk: str,
        sk_value: any,
        dynamo_table: str,
    ) -> dict:
        response = self.__dynamo_resource.Table(dynamo_table).get_item(
            Key={
                pk : pk_value,
                sk : sk_value,
            }
        )

        if 'Item' in response:
            item = response['Item']

        return item


    def get_records_with_attribute_from_table(
        self,
        attribute: str,
        attribute_value: any,
        dynamo_table: str,
    ) -> list[dict]:
        """
        Get the records for the (non-primary key) attribute from the provided
        dynamo table.
        """
        response = self.__dynamo_resource.Table(dynamo_table).scan(
            FilterExpression=boto3.dynamodb.conditions.Attr(attribute).eq(attribute_value)
        )

        if 'Items' in response and len(response['Items']) > 0:
            items = response['Items']
        else:
            error_str = f'No records found for attribute {attribute} = "{attribute_value}" ' \
                f'in Dynamo table "{dynamo_table}".'
            raise RuntimeError(error_str)

        return items


    def put_record_in_table(
        self,
        record: dict,
        dynamo_table: str,
    ):
        self.__dynamo_resource.Table(dynamo_table).put_item(
            Item=record,
        )
