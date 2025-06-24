# docs: https://aws.amazon.com/blogs/machine-learning/build-an-end-to-end-mlops-pipeline-using-amazon-sagemaker-pipelines-github-and-github-actions/

set -ex

export AWS_PROFILE=sandbox
export AWS_REGION=us-east-1

# disables an annoying cdk cli warning
export JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1


THIS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

function cdk_bootstrap () {
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    cdk bootstrap "aws://${AWS_ACCOUNT_ID}/${AWS_REGION}"
}

function cdk_deploy () {
    uv run -- cdk deploy --app 'python cdk_app.py' '*' --profile $AWS_PROFILE --region $AWS_REGION
}

function cdk_destroy () {
    uv run -- cdk destroy --app 'python cdk_app.py' '*' --profile $AWS_PROFILE --region $AWS_REGION
}

function taxi_flow() {
    uv run ./src/extract_load_taxi_data_flow.py --environment=local ${@}
}

function weather_flow() {
    uv run ./src/extract_load_weather_data_flow.py --environment=local ${@}
}

# sometimes you need to do this if the cdk python library is newer than the CLI can handle
function upgrade_cdk_cli() {
    npm uninstall -g aws-cdk
    npm install -g aws-cdk
}

time "${@}"