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
