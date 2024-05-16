import os
import json
import boto3
import botocore
import moto
import unittest
import shutil
import datetime as dt

import boto_plus


def open_json(filepath):
    with open(filepath, 'rb') as in_file:
        payload = json.load(in_file)

    return payload


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
    def test_get_runtime_from_batch_job(self):
        """
        Warning: this is placeholder code for future testing of the 
        function get_runtime_from_batch_job(). Currently, the moto mocking library does not 
        support setting runtime lengths for AWS Batch jobs if docker is not being run. In order 
        to run a job for a nonzero length of time, an actual Docker image would have to built 
        and pushed to ECR using an actual script with something like 'time.sleep(2)'. 
        Thus this test will wait until this moto functionality exists. 
        """
        batch = boto3.client('batch', self.region)
        iam   = boto3.client('iam')
        ec2   = boto3.client('ec2', region_name=self.region)

        mock_admin_policy = open_json(
            'tests/mock-iam-policies/mock-admin-iam-policy.json'
        )
        mock_assume_role_policy = open_json(
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

        response = batch.submit_job(
            jobName='mock-job-1',
            jobQueue=queue_arn,
            jobDefinition=job_def_arn
        )

        job_id = response['jobId']

        batch_plus = boto_plus.BatchPlus(
            boto_config=self.boto_config,
            boto_session=self.boto_session,
        )

        # test with datetime output
        payload = batch_plus.get_runtime_from_batch_job(
            job_id=job_id,
            convert_datetime_to_string=False
        )

        self.assertIsInstance(payload['start'], dt.datetime)
        self.assertIsInstance(payload['stop'], dt.datetime)

        # test with string output
        payload = batch_plus.get_runtime_from_batch_job(
            job_id=job_id,
            convert_datetime_to_string=True
        )

        self.assertIsInstance(payload['start'], str)
        self.assertIsInstance(payload['stop'], str)


if __name__ == "__main__":
    unittest.main()
