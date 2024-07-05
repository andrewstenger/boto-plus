import string
import uuid
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


    def get_all_records_from_table(
        self,
        table_name: str,
        select='ALL_ATTRIBUTES',
        fields=None,
    ) -> list[dict]:
        valid_select = ('ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES', 'COUNT', 'SPECIFIC_ATTRIBUTES')
        if select not in valid_select:
            raise RuntimeError(f'The provided value for "select" must be one of "{valid_select}"')

        if select == 'SPECIFIC_ATTRIBUTES' and fields is None:
            error_str = 'Provided argument "select" = "SPECIFIC_ATTRIBUTES", but no value for "fields" was provided.'
            raise RuntimeError(error_str)

        if select != 'SPECIFIC_ATTRIBUTES' and fields is not None:
            error_str = f'Provided argument "select" = "{select}", but "fields" = "{fields}" (should be None).'
            raise RuntimeError(error_str)

        table = self.__dynamo_resource.Table(table_name)
        limit = 1000

        query = {
            'Limit' : limit,
            'Select' : select,
        }

        if fields is not None:
            expr_attr_names = {f'#{uuid.uuid4().hex[:8]}' : c for c in fields}
            query['ProjectionExpression'] = ','.join(list(expr_attr_names.keys()))
            query['ExpressionAttributeNames'] = expr_attr_names

        records = list()

        response = table.scan(**query)
        records.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            query['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = table.scan(**query)
            records.extend(response['Items'])

        return records


    def get_record_with_primary_key_from_table(
        self,
        pk: str,
        pk_value: any,
        table_name: str,
    ) -> dict:
        response = self.__dynamo_resource.Table(table_name).get_item(
            Key={
                pk : pk_value,
            }
        )

        item = dict()
        if 'Item' in response:
            item = response['Item']

        return item


    def get_record_with_composite_key_from_table(
        self,
        pk: str,
        pk_value: any,
        sk: str,
        sk_value: any,
        table_name: str,
    ) -> dict:
        response = self.__dynamo_resource.Table(table_name).get_item(
            Key={
                pk : pk_value,
                sk : sk_value,
            }
        )

        item = dict()
        if 'Item' in response:
            item = response['Item']

        return item


    def get_records_with_attribute_from_table(
        self,
        attribute: str,
        attribute_value: any,
        table_name: str,
    ) -> list[dict]:
        """
        Get the records for the (non-primary key) attribute from the provided
        dynamo table.
        """
        response = self.__dynamo_resource.Table(table_name).scan(
            FilterExpression=boto3.dynamodb.conditions.Attr(attribute).eq(attribute_value)
        )

        items = list()
        if 'Items' in response and len(response['Items']) > 0:
            items = response['Items']

        return items


    def put_record_in_table(
        self,
        record: dict,
        table_name: str,
    ):
        self.__dynamo_resource.Table(table_name).put_item(
            Item=record,
        )


    def delete_record_with_primary_key_from_table(
        self,
        pk: str,
        pk_value: any,
        table_name: str,
    ) -> dict:
        response = self.__dynamo_resource.Table(table_name).delete_item(
            Key={
                pk : pk_value,
            }
        )

        return response


    def delete_record_with_composite_key_from_table(
        self,
        pk: str,
        pk_value: any,
        sk: str,
        sk_value: any,
        table_name: str,
    ) -> dict:
        response = self.__dynamo_resource.Table(table_name).delete_item(
            Key={
                pk : pk_value,
                sk : sk_value,
            }
        )

        return response


    def delete_records_with_attribute_from_table(
        self,
        attribute: str,
        attribute_value: any,
        pk: str,
        table_name: str,
        sk=None,
    ) -> dict:
        table = self.__dynamo_resource.Table(table_name)

        scan_response = self.__dynamo_resource.Table(table_name).scan(
            FilterExpression=boto3.dynamodb.conditions.Attr(attribute).eq(attribute_value)
        )

        for item in scan_response['Items']:
            key = {
                pk : item[pk],
            }

            if sk is not None:
                key[sk] = item[sk]

            table.delete_item(
                Key=key,
            )


    """
    # To-Do: fill this in
    def scan(
        self,
        index_name=None,
        attributes_to_get=None,
        select=None,
        scan_filter=None,
        conditional_operator=None,
        exclusive_start_key=None,
        return_consumed_capacity=None,
        total_segments=None,
        segment=None,
        projection_expression=None,
        filter_expression=None,
        expression_attribute_names=None,
        expression_attribute_values=None,
        consistent_read=False,
    ):
        item = dict()

        if index_name is not None:
            item['IndexName'] = index_name
        if attributes_to_get is not None:
            item['AttributesToGet'] = attributes_to_get
        if select is not None:
            item['Select'] = select
        ...
    """

