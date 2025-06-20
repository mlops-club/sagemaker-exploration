# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "aws-cdk-lib",
#     "constructs",
# ]
# ///

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_s3 as s3,
)
from constructs import Construct
import os


class GlueStack(Stack):
    """Stack for data ingestion infrastructure"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        

        
###############
# --- App --- #
###############

# CDK App
app = cdk.App()

# Create the data ingestion stack
GlueStack(
    app,
    "glue-nyc-taxi",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    ),
    description="Data ingestion infrastructure for SageMaker exploration"
)

# Synthesize the app
app.synth()
