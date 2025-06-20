# SageMaker Exploration

We are exploring SageMaker. Why? Because

- it's in our AWS accounts--no need to go through procurement
- our bosses, coworkers, and interviewers ask us if we know it ("why don't we just use SM?")
- we want to have answers ready

## Setup

To explore SageMaker, you need to have a sagemaker domain created in your AWS account.

These steps help you use IaC to deploy one. It won't cost you anything for the domain to exist, but BEWARE as you create notebooks, etc. inside it!

### Step 1 - Prerequisites

Be sure to have `uv`, `node`, `awscli`, and the `aws-cdk` CLI installed. 

```bash
brew install uv node awscli aws-cdk
```

### Step 2 - Have an AWS CLI profile with `AdministratorAccess`

Example of logging with AWS SSO.

```bash
aws configure sso --profile sandbox
```

### Step 3 - bootstrap your AWS account and region for AWS CDK

```bash
# creates a few *free* resources needed to deploy infra with CDK
bash run.sh cdk_bootstrap
```

### Step 4 - deploy the sagemaker domain

```bash
bash run.sh cdk_deploy
```

You should now have a SageMaker domain created in `us-east-1` 🎉!