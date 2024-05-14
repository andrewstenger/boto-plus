import boto3
import botocore


class DynamoPlus:

    def __init__(
        self,
        dynamo_resource=None,
        region=None,
    ):
        if dynamo_resource is not None and region is not None:
            raise RuntimeError('Only one of "dynamo_resource", "region" can be provided (received both).')

        if dynamo_resource is None:
            self.__dynamo_resource = boto3.resource('dynamodb', region_name=region)
        else:
            self.__dynamo_resource = dynamo_resource


    def does_table_exist(
        self,
        table_name: str,
    ):
        try:
            table = self.__dynamo_resource.Table(table_name)
            # This line will trigger an exception if the table does not exist
            table.load()
            return True

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            else:
                raise e
