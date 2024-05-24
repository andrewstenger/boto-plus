import os
import json
import math
import boto3
import botocore
import moto
import unittest
import shutil
import datetime as dt

import boto_plus


class TestBatchPlus(unittest.TestCase):

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


    @moto.mock_aws(config={'batch' : {'use_docker' : False}})
    def test_get_runtime_of_jobs(self):
        batch = boto3.client('batch', self.region)
        iam   = boto3.client('iam')
        ec2   = boto3.client('ec2', region_name=self.region)

        mock_admin_policy = boto_plus.helpers.open_json(
            'tests/mock-iam-policies/mock-admin-iam-policy.json'
        )
        mock_assume_role_policy = boto_plus.helpers.open_json(
            'tests/mock-iam-policies/mock-batch-assume-role-policy.json'
        )

        # create job definition using a public ECR image
        response = batch.register_job_definition(
            jobDefinitionName='mock-job',
            type='container',
            containerProperties={
                'image'  : 'public.ecr.aws/docker/library/python:3.9.19',
                'vcpus'  : 1,
                'memory' : 1024,
            },
            timeout={
                'attemptDurationSeconds' : 5,
            }
        )

        job_def_arn = response['jobDefinitionArn']

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
        service_role_name = response['Role']['RoleName']

        iam.attach_role_policy(
            RoleName='mock-service-role',
            PolicyArn=admin_policy_arn,
        )

        # create instance IAM role/profile for Batch
        response = iam.create_instance_profile(
            InstanceProfileName='mock-instance-profile',
        )

        instance_profile_arn = response['InstanceProfile']['Arn']
        instance_profile_name = response['InstanceProfile']['InstanceProfileName']

        iam.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=service_role_name,
        )

        # vpc setup for Batch
        vpc = ec2.create_vpc(CidrBlock="172.16.0.0/16")
        vpc_id = vpc['Vpc']['VpcId']
        vpc_waiter = ec2.get_waiter('vpc_available')
        vpc_waiter.wait(VpcIds=[vpc_id])

        response = ec2.create_subnet(CidrBlock="172.16.0.1/24", VpcId=vpc_id)
        subnet_id = response['Subnet']['SubnetId']

        response = ec2.create_security_group(
            Description="Test security group", GroupName="sg1", VpcId=vpc_id
        )
        sg_id = response['GroupId']

        # Batch infra setup
        response = batch.create_compute_environment(
            computeEnvironmentName='mock-environment',
            type='MANAGED',
            state='ENABLED',
            computeResources={
                'type' : 'EC2',
                'allocationStrategy' : 'BEST_FIT_PROGRESSIVE',
                'minvCpus' : 1,
                'maxvCpus' : 1,
                'desiredvCpus' : 1,
                'instanceTypes' : ['t2.micro'],
                'instanceRole' : instance_profile_arn,
                'subnets' : [subnet_id],
                'securityGroupIds' : [sg_id],
            },
            serviceRole=service_role_arn,
        )

        compute_arn = response['computeEnvironmentArn']

        response = batch.create_job_queue(
            jobQueueName='mock-queue',
            state='ENABLED',
            priority=1,
            computeEnvironmentOrder=[
                {
                    'order' : 1,
                    'computeEnvironment' : compute_arn,
                }
            ]
        )

        queue_arn = response['jobQueueArn']

        os.environ['MOTO_SIMPLE_BATCH_FAIL_AFTER'] = '2'

        response = batch.submit_job(
            jobName='mock-job-1',
            jobQueue=queue_arn,
            jobDefinition=job_def_arn,
        )

        job_id = response['jobId']

        batch_plus = boto_plus.BatchPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        # test with datetime output
        payload = batch_plus.get_runtime_of_jobs(
            job_ids=[job_id],
            convert_datetime_to_string=False,
        )

        job_payload = payload[job_id]

        self.assertIsInstance(job_payload['start'], dt.datetime)
        self.assertIsInstance(job_payload['stop'], dt.datetime)

        self.assertLess(job_payload['start'], job_payload['stop'])

        # test with string output
        payload = batch_plus.get_runtime_of_jobs(
            job_ids=[job_id],
            convert_datetime_to_string=True,
        )

        job_payload = payload[job_id]

        self.assertIsInstance(job_payload['start'], str)
        self.assertIsInstance(job_payload['stop'], str)

        self.assertLess(job_payload['start'], job_payload['stop'])

        """
        Test submission of 101 jobs:
        If get_runtime_of_jobs receives > 100 job ids, it must process them in batches.
        This test asserts all job ids are processed & returned by this function.
        This test takes 100 seconds to run, since unfortunately the smallest increment of time
        that moto can mock a job to run for is 1 second.
        """
        os.environ['MOTO_SIMPLE_BATCH_FAIL_AFTER'] = '1'

        job_ids = list()
        for i in range(101):
            response = batch.submit_job(
                jobName=f'mock-job-{i}',
                jobQueue=queue_arn,
                jobDefinition=job_def_arn
            )
            job_ids.append(response['jobId'])

        payload = batch_plus.get_runtime_of_jobs(
            job_ids=job_ids,
            convert_datetime_to_string=True,
        )

        for job_id, runtimes in payload.items():
            self.assertLess(runtimes['start'], runtimes['stop'])
            self.assertGreater(math.floor(float(runtimes['total-seconds'])), 0)

        del os.environ['MOTO_SIMPLE_BATCH_FAIL_AFTER']


    @moto.mock_aws(config={'batch' : {'use_docker' : False}})
    def test_get_status_of_jobs(self):
        batch = boto3.client('batch', self.region)
        iam   = boto3.client('iam')
        ec2   = boto3.client('ec2', region_name=self.region)

        mock_admin_policy = boto_plus.helpers.open_json(
            'tests/mock-iam-policies/mock-admin-iam-policy.json'
        )
        mock_assume_role_policy = boto_plus.helpers.open_json(
            'tests/mock-iam-policies/mock-batch-assume-role-policy.json'
        )

        # create job definition using a public ECR image
        response = batch.register_job_definition(
            jobDefinitionName='mock-job',
            type='container',
            containerProperties={
                'image'  : 'public.ecr.aws/docker/library/python:3.9.19',
                'vcpus'  : 1,
                'memory' : 1024,
            },
            timeout={
                'attemptDurationSeconds' : 5,
            }
        )

        job_def_arn = response['jobDefinitionArn']

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
        service_role_name = response['Role']['RoleName']

        iam.attach_role_policy(
            RoleName='mock-service-role',
            PolicyArn=admin_policy_arn,
        )

        # create instance IAM role/profile for Batch
        response = iam.create_instance_profile(
            InstanceProfileName='mock-instance-profile',
        )

        instance_profile_arn = response['InstanceProfile']['Arn']
        instance_profile_name = response['InstanceProfile']['InstanceProfileName']

        iam.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=service_role_name,
        )

        # vpc setup for Batch
        vpc = ec2.create_vpc(CidrBlock="172.16.0.0/16")
        vpc_id = vpc['Vpc']['VpcId']
        vpc_waiter = ec2.get_waiter('vpc_available')
        vpc_waiter.wait(VpcIds=[vpc_id])

        response = ec2.create_subnet(CidrBlock="172.16.0.1/24", VpcId=vpc_id)
        subnet_id = response['Subnet']['SubnetId']

        response = ec2.create_security_group(
            Description="Test security group", GroupName="sg1", VpcId=vpc_id
        )
        sg_id = response['GroupId']

        # Batch infra setup
        response = batch.create_compute_environment(
            computeEnvironmentName='mock-environment',
            type='MANAGED',
            state='ENABLED',
            computeResources={
                'type' : 'EC2',
                'allocationStrategy' : 'BEST_FIT_PROGRESSIVE',
                'minvCpus' : 1,
                'maxvCpus' : 1,
                'desiredvCpus' : 1,
                'instanceTypes' : ['t2.micro'],
                'instanceRole' : instance_profile_arn,
                'subnets' : [subnet_id],
                'securityGroupIds' : [sg_id],
            },
            serviceRole=service_role_arn,
        )

        compute_arn = response['computeEnvironmentArn']

        response = batch.create_job_queue(
            jobQueueName='mock-queue',
            state='ENABLED',
            priority=1,
            computeEnvironmentOrder=[
                {
                    'order' : 1,
                    'computeEnvironment' : compute_arn,
                }
            ]
        )

        queue_arn = response['jobQueueArn']

        # first job fails
        os.environ['MOTO_SIMPLE_BATCH_FAIL_AFTER'] = '1'

        response = batch.submit_job(
            jobName='mock-job-1',
            jobQueue=queue_arn,
            jobDefinition=job_def_arn
        )

        job_id1 = response['jobId']

        del os.environ['MOTO_SIMPLE_BATCH_FAIL_AFTER']

        # second job succeeds (only the FAILS_AFTER variable must be removed for this)

        response = batch.submit_job(
            jobName='mock-job-2',
            jobQueue=queue_arn,
            jobDefinition=job_def_arn
        )

        job_id2 = response['jobId']

        batch_plus = boto_plus.BatchPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        # test with datetime output
        payload = batch_plus.get_status_of_jobs(
            job_ids=[job_id1, job_id2],
        )

        self.assertEqual(payload[job_id1], 'FAILED')
        self.assertEqual(payload[job_id2], 'SUCCEEDED')

        """
        Test submission of 101 jobs:
        If get_runtime_of_jobs receives > 100 job ids, it must process them in batches.
        This test asserts all job ids are processed & returned by this function.
        """
        os.environ['MOTO_SIMPLE_BATCH_FAIL_AFTER'] = '0'

        map_job_id_to_status = dict()
        for i in range(101):
            response = batch.submit_job(
                jobName=f'mock-job-{i}',
                jobQueue=queue_arn,
                jobDefinition=job_def_arn
            )
            map_job_id_to_status[response['jobId']] = 'FAILED'

        payload = batch_plus.get_status_of_jobs(
            job_ids=list(map_job_id_to_status.keys()),
        )

        self.assertEqual(payload, map_job_id_to_status)

        del os.environ['MOTO_SIMPLE_BATCH_FAIL_AFTER']


if __name__ == "__main__":
    unittest.main()
