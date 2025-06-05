from aws_cdk import (
    aws_sagemaker as sagemaker,
)
from constructs import Construct
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam

# s3
from aws_cdk import aws_s3 as s3

# lambda fn
from aws_cdk import aws_lambda as _lambda
import aws_cdk as cdk

VPC_NAME = "local-virginia"


class SagemakerDomainConstruct(Construct):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        vpc = ec2.Vpc.from_lookup(
            self,
            "VpcLookup",
            vpc_name=VPC_NAME,
        )
        stack = cdk.Stack.of(self)

        # Get public and private subnet IDs from the VPC
        public_subnet_ids = [subnet.subnet_id for subnet in vpc.public_subnets]
        private_subnet_ids = [subnet.subnet_id for subnet in vpc.private_subnets]


        self.domain = sagemaker.CfnDomain(
            self,
            "SagemakerDomain",
            domain_name="my-sagemaker-domain",
            auth_mode="IAM",
            default_user_settings={
                "execution_role": f"arn:aws:iam::{stack.account}:role/my-sagemaker-execution-role",
                "jupyter_server_app_settings": {
                    "default_resource_spec": {
                        "instance_type": "ml.t2.medium",
                        "sage_maker_image": "my-sagemaker-image",
                    }
                },
            },
            # Use private subnets for Sagemaker Domain (recommended)
            subnet_ids=private_subnet_ids + public_subnet_ids,
            vpc_id=vpc.vpc_id,
        )
