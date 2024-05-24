import json
import uuid
import boto3

import boto_plus


class StepFunctionPlus:

    def __init__(
        self,
        boto_config,
        boto_session=None,
    ):
        if boto_session is not None:
            self.__sfn_client = boto_session.client('stepfunctions', config=boto_config)
            sts = boto_session.client('sts', config=boto_config)

        else:
            self.__sfn_client = boto3.client('stepfunctions', config=boto_config)
            sts = boto3.client('sts', config=boto_config)

        self.__region_name = boto_config.region_name
        self.__account_id = sts.get_caller_identity()['Account']


    def describe_state_machine(
        self,
        name: str,
        version=None,
    ) -> dict:
        state_machine_arn = self.create_state_machine_arn(name=name, version=version)
        response = self.__sfn_client.describe_state_machine(stateMachineArn=state_machine_arn)
        return response


    def list_state_machines(
        self,
        name_only=False,
        arn_only=False,
    ) -> list:
        if name_only and arn_only:
            raise RuntimeError('Only one of "name_only", "arn_only" may be supplied (received both).')

        max_results = 1000

        response = self.__sfn_client.list_state_machines(maxResults=max_results)
        state_machines = response['stateMachines']
        next_token = response.get('nextToken', None)

        while next_token:
            response = self.__sfn_client.list_state_machines(
                nextToken=next_token,
                maxResults=max_results,
            )
            state_machines.extend(response['stateMachines'])
            next_token = response.get('nextToken', None)

        if name_only:
            state_machines = [s['name'] for s in state_machines]

        elif arn_only:
            state_machines = [s['stateMachineArn'] for s in state_machines]

        return state_machines


    def list_state_machine_versions(
        self,
        name: str,
        arn_only=False,
        version_only=False,
    ) -> list:
        if arn_only and version_only:
            raise RuntimeError('Only one of "arn_only", "version_only" may be supplied (received both).')

        state_machine_arn = self.create_state_machine_arn(name=name, version=None)

        max_results = 1000

        response = self.__sfn_client.list_state_machine_versions(
            stateMachineArn=state_machine_arn,
            maxResults=max_results,
        )

        state_machine_versions = response['stateMachineVersions']
        next_token = response.get('nextToken', None)

        while next_token:
            response = self.__sfn_client.list_state_machine_versions(
                stateMachineArn=state_machine_arn,
                nextToken=next_token,
                maxResults=max_results,
            )
            state_machine_versions.extend(response['stateMachineVersions'])
            next_token = response.get('nextToken', None)

        if arn_only:
            state_machines = [s['stateMachineVersionArn'] for s in state_machine_versions]

        elif version_only:
            state_machines = [s['stateMachineVersionArn'].split(':')[-1] for s in state_machine_versions]

        return state_machines


    def does_state_machine_exist(
        self,
        name: str,
        version=None,
    ) -> bool:
        state_machine_arn = self.create_state_machine_arn(name=name, version=version)

        try:
            self.__sfn_client.describe_state_machine(
                stateMachineArn=state_machine_arn,
            )
            exists = True

        except self.__sfn_client.exceptions.StateMachineDoesNotExist:
            exists = False

        except Exception as exception:
            raise exception

        return exists


    def execute_state_machine(
        self,
        name: str,
        input: dict,
        execution_name=None,
        version=None,
        trace_header=None,
    ) -> dict:
        state_machine_arn = self.create_state_machine_arn(name=name, version=version)

        if execution_name is None:
            job_hash = uuid.uuid4().hex[:8]
            execution_name = f'{name}-{job_hash}'

        input_str = json.dumps(input)

        execution_payload = {
            'stateMachineArn' : state_machine_arn,
            'name' : execution_name,
            'input' : input_str,
        }

        if not trace_header is None:
            execution_payload['traceHeader'] = trace_header

        response = self.__sfn_client.start_execution(**execution_payload)
        return response


    def create_state_machine(
        self,
        name: str,
        definition: str,
        parse_definition_from_filepath: bool,
        role_arn: str,
        type='STANDARD',
        log_level='OFF',
        log_execution_data=False,
        log_group_arn=None,
        enable_tracing=True,
        publish=True,
        version_description=None,
        tags=None,
    ) -> dict:
        if type not in ('STANDARD', 'EXPRESS'):
            error_str = f'Provided variable "type" must be one of "STANDARD", "EXPRESS" (received "{type}").'
            raise RuntimeError(error_str)

        if log_level not in ('ALL', 'ERROR', 'FATAL', 'OFF'):
            error_str = 'Provided variable "log_level" must be one of "ALL", "ERROR", "FATAL", "OFF" ' \
                f'(received "{log_level}").'
            raise RuntimeError(error_str)

        if log_level == 'OFF' and log_execution_data:
            error_str = 'Provided variable "log_level" is set to "OFF", but variable "log_execution_data" ' \
                'is set to True. Either set "log_level" to one of "ALL", "ERROR", "FATAL", or set ' \
                '"log_execution_data" to False'
            raise RuntimeError(error_str)

        if log_level != 'OFF' and log_group_arn is None:
            error_str = f'Provided variable "log_level" is set to "{log_level}", but variable ' \
                '"log_group_arn" has not been provided. Either set "log_level" to "OFF", or set ' \
                '"log_group_arn" to a valid IAM role.'
            raise RuntimeError(error_str)

        log_config = {
            'level' : log_level,
        }

        if log_level != 'OFF':
            log_config['includeExecutionData'] = log_execution_data

            if log_group_arn is not None:
                log_config['destinations'] = [{
                    'cloudWatchLogsLogGroup': {
                        'logGroupArn': log_group_arn,
                    }
                }]

        tracing_config = {
            'enabled' : enable_tracing,
        }

        if parse_definition_from_filepath:
            parsed_definition = boto_plus.helpers.open_json(definition)
        else:
            parsed_definition = definition

        parsed_definition_str = json.dumps(parsed_definition)

        input_payload = {
            'name' : name,
            'definition' : parsed_definition_str,
            'roleArn' : role_arn,
            'type' : type,
            'loggingConfiguration' : log_config,
            'publish' : publish,
            'tracingConfiguration' : tracing_config,
        }

        if not version_description is None:
            input_payload['versionDescription'] = version_description

        if log_level != 'OFF':
            input_payload['loggingConfiguration'] = log_config

        if not tags is None:
            input_payload['tags'] = tags

        response = self.__sfn_client.create_state_machine(**input_payload)
        return response


    def create_state_machine_arn(
        self,
        name: str,
        version=None,
    ) -> str:
        """
        Uses the Botocore config and boto3 session provided during initialization to get the region
        & account ID, which is used to build the state machine arn.

        Arguments
        ----------
        name : str
            The name of the state machine

        version: str
            The version number/identifier of the state machine version (if applicable)

        Returns
        ----------
        state_machine_arn : str
            The created ARN of the state machine with the provided name/version.

        """
        if version is None:
            state_machine_arn = f'arn:aws:states:{self.__region_name}:{self.__account_id}:stateMachine:{name}'
        else:
            state_machine_arn = f'arn:aws:states:{self.__region_name}:{self.__account_id}:stateMachine:{name}:{version}'

        return state_machine_arn


    @property
    def account_id(self):
        return self.__account_id

    @property
    def region_name(self):
        return self.__region_name
