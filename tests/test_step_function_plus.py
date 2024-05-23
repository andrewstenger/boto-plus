import os
import json
import datetime as dt
import boto3
import botocore
import moto
import unittest
import shutil

import boto_plus


def open_json(filepath):
    with open(filepath, 'rb') as in_file:
        payload = json.load(in_file)

    return payload


class TestStepFunctionPlus(unittest.TestCase):

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
    def test_list_state_machines(self):
        iam = self.boto_session.client('iam', config=self.boto_config)
        sfn = self.boto_session.client('stepfunctions', config=self.boto_config)

        mock_admin_policy = open_json(
            'tests/mock-iam-policies/mock-admin-iam-policy.json'
        )
        mock_assume_role_policy = open_json(
            'tests/mock-iam-policies/mock-batch-assume-role-policy.json'
        )

        # create mock IAM admin policy (we are not testing permissions here yet)
        response = iam.create_policy(
            PolicyName='mock-admin',
            PolicyDocument=json.dumps(mock_admin_policy)
        )

        admin_policy_arn = response['Policy']['Arn']

        # create Batch service role
        response = iam.create_role(
            RoleName='mock-service-role',
            AssumeRolePolicyDocument=json.dumps(mock_assume_role_policy)
        )

        service_role_arn = response['Role']['Arn']

        iam.attach_role_policy(
            RoleName='mock-service-role',
            PolicyArn=admin_policy_arn,
        )

        # The boto3 cli has a max number of returned state machines set to 1000.
        # So, we create 1001 and then list them to assert our function is
        # paginating the results correctly.
        for i in range(1001):
            sfn.create_state_machine(
                name=f'mock-machine-{i}',
                definition=json.dumps({}),
                roleArn=service_role_arn,
                type='STANDARD',
                publish=True,
            )

        sfn_plus = boto_plus.StepFunctionPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        machines = sfn_plus.list_state_machines(name_only=True)
        for i in range(1001):
            self.assertIn(f'mock-machine-{i}', machines)


    @moto.mock_aws
    def test_describe_state_machine(self):
        iam = self.boto_session.client('iam', config=self.boto_config)
        sfn = self.boto_session.client('stepfunctions', config=self.boto_config)

        mock_admin_policy = open_json(
            'tests/mock-iam-policies/mock-admin-iam-policy.json'
        )
        mock_assume_role_policy = open_json(
            'tests/mock-iam-policies/mock-batch-assume-role-policy.json'
        )

        # create mock IAM admin policy (we are not testing permissions here yet)
        response = iam.create_policy(
            PolicyName='mock-admin',
            PolicyDocument=json.dumps(mock_admin_policy)
        )

        admin_policy_arn = response['Policy']['Arn']

        # create Batch service role
        response = iam.create_role(
            RoleName='mock-service-role',
            AssumeRolePolicyDocument=json.dumps(mock_assume_role_policy)
        )

        service_role_arn = response['Role']['Arn']

        iam.attach_role_policy(
            RoleName='mock-service-role',
            PolicyArn=admin_policy_arn,
        )

        sfn.create_state_machine(
            name='mock-machine',
            definition=json.dumps({}),
            roleArn=service_role_arn,
            type='STANDARD',
            publish=True,
            versionDescription='9000',
        )

        sfn_plus = boto_plus.StepFunctionPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )
        results = sfn_plus.describe_state_machine(name='mock-machine')
        self.assertEqual(results['name'], 'mock-machine')
        self.assertEqual(results['stateMachineArn'], f'arn:aws:states:{sfn_plus.region_name}:{sfn_plus.account_id}:stateMachine:mock-machine')


    @moto.mock_aws
    def test_execute_state_machine(self):
        iam = self.boto_session.client('iam', config=self.boto_config)
        sfn = self.boto_session.client('stepfunctions', config=self.boto_config)

        mock_admin_policy = open_json(
            'tests/mock-iam-policies/mock-admin-iam-policy.json'
        )
        mock_assume_role_policy = open_json(
            'tests/mock-iam-policies/mock-batch-assume-role-policy.json'
        )

        # create mock IAM admin policy (we are not testing permissions here yet)
        response = iam.create_policy(
            PolicyName='mock-admin',
            PolicyDocument=json.dumps(mock_admin_policy)
        )

        admin_policy_arn = response['Policy']['Arn']

        # create Batch service role
        response = iam.create_role(
            RoleName='mock-service-role',
            AssumeRolePolicyDocument=json.dumps(mock_assume_role_policy)
        )

        service_role_arn = response['Role']['Arn']

        iam.attach_role_policy(
            RoleName='mock-service-role',
            PolicyArn=admin_policy_arn,
        )

        sfn.create_state_machine(
            name='mock-machine',
            definition=json.dumps({}),
            roleArn=service_role_arn,
            type='STANDARD',
            publish=True,
            versionDescription='9000',
        )

        sfn_plus = boto_plus.StepFunctionPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        # assert the state machine was started -- it started an execution with the expected inputs
        sfn_plus.execute_state_machine(
            name='mock-machine',
            input=json.dumps({'var-1' : 'value-1', 'var-2' : 'value-2'}),
            execution_name='mock-test-123',
        )

        executions = sfn.list_executions(
            stateMachineArn=f'arn:aws:states:{sfn_plus.region_name}:{sfn_plus.account_id}:stateMachine:mock-machine',
            statusFilter='RUNNING',
        )
        execution_arn = executions['executions'][0]['executionArn']

        self.assertIn(executions['executions'][0]['name'], 'mock-test-123')

        response = sfn.describe_execution(executionArn=execution_arn)
        input = json.loads(json.loads(response['input']))
        self.assertIn('var-1', input.keys())
        self.assertIn('var-2', input.keys())
        self.assertEqual(input['var-1'], 'value-1')
        self.assertEqual(input['var-2'], 'value-2')


    """
    # Not implemented in Moto's library yet https://docs.getmoto.org/en/latest/docs/services/stepfunctions.html

    @moto.mock_aws
    def test_list_state_machine_versions(self):
        iam = self.boto_session.client('iam', config=self.boto_config)
        sfn = self.boto_session.client('stepfunctions', config=self.boto_config)

        mock_admin_policy = open_json(
            'tests/mock-iam-policies/mock-admin-iam-policy.json'
        )
        mock_assume_role_policy = open_json(
            'tests/mock-iam-policies/mock-batch-assume-role-policy.json'
        )

        # create mock IAM admin policy (we are not testing permissions here yet)
        response = iam.create_policy(
            PolicyName='mock-admin',
            PolicyDocument=json.dumps(mock_admin_policy)
        )

        admin_policy_arn = response['Policy']['Arn']

        # create Batch service role
        response = iam.create_role(
            RoleName='mock-service-role',
            AssumeRolePolicyDocument=json.dumps(mock_assume_role_policy)
        )

        service_role_arn = response['Role']['Arn']

        iam.attach_role_policy(
            RoleName='mock-service-role',
            PolicyArn=admin_policy_arn,
        )

        sfn.create_state_machine(
            name='mock-machine',
            definition=json.dumps({}),
            roleArn=service_role_arn,
            type='STANDARD',
            publish=True,
            versionDescription='9000',
        )
        sfn.create_state_machine(
            name='mock-machine',
            definition=json.dumps({}),
            roleArn=service_role_arn,
            type='STANDARD',
            publish=True,
            versionDescription='9001',
        )

        sfn_plus = boto_plus.StepFunctionPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )
        results = sfn_plus.list_state_machine_versions(name='mock-machine', version_only=True)
        self.assertIn('9000', results)
        self.assertIn('9001', results)
    """