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

VPC_NAME = "default"
# VPC_NAME = "local-virginia"


class SagemakerDomainConstruct(Construct):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        if VPC_NAME.lower() == "default":
            vpc = ec2.Vpc.from_lookup(self, "VpcLookup", is_default=True)
        else:
            vpc = ec2.Vpc.from_lookup(
                self,
                "VpcLookup",
                vpc_name=VPC_NAME,
            )
        stack = cdk.Stack.of(self)

        # Get public and private subnet IDs from the VPC
        public_subnet_ids = [subnet.subnet_id for subnet in vpc.public_subnets]
        private_subnet_ids = [subnet.subnet_id for subnet in vpc.private_subnets]

        # Create SageMaker execution role
        execution_role = iam.Role(
            self,
            "SageMakerExecutionRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
            ]
        )

        # Create SageMaker domain
        self.domain = sagemaker.CfnDomain(
            self,
            "SagemakerDomain",
            domain_name="my-sagemaker-domain",
            auth_mode="IAM",
            default_user_settings=sagemaker.CfnDomain.UserSettingsProperty(
                execution_role=execution_role.role_arn,
                jupyter_server_app_settings=sagemaker.CfnDomain.JupyterServerAppSettingsProperty(
                    default_resource_spec=sagemaker.CfnDomain.ResourceSpecProperty(
                        instance_type="system",
                        sage_maker_image_arn="arn:aws:sagemaker:" + stack.region + ":081325390199:image/jupyter-server-3"
                    )
                )
            ),
            # Use private subnets for Sagemaker Domain (recommended)
            subnet_ids=private_subnet_ids + public_subnet_ids,
            vpc_id=vpc.vpc_id,
        )
