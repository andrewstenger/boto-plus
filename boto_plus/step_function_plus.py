import json
import uuid
import boto3


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
        state_machine_arn = self.__create_state_machine_arn(name=name, version=version)
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

        state_machine_arn = self.__create_state_machine_arn(name=name, version=None)

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
        state_machine_arn = self.__create_state_machine_arn(name=name, version=version)

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
        state_machine_arn = self.__create_state_machine_arn(name=name, version=version)

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


    def __create_state_machine_arn(
        self,
        name: str,
        version=None,
    ):
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
