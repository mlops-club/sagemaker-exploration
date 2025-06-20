from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
)
from constructs import Construct
from sagemaker_exploration.domain_construct import SagemakerDomainConstruct

class SagemakerExplorationStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        SagemakerDomainConstruct(self, id="sagemaker-domain-construct")

        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "SagemakerExplorationQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )


