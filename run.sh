# docs: https://aws.amazon.com/blogs/machine-learning/build-an-end-to-end-mlops-pipeline-using-amazon-sagemaker-pipelines-github-and-github-actions/

set -ex

export AWS_PROFILE=sbox
export SAGEMAKER_DOMAIN_ID=d-qt4dkqxcevqe
export AWS_REGION=us-east-1


LAMBDA_FN_S3_BUCKET=sagemaker-test-bucket-mc

THIS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

function create_gh_personal_access_token_secret () {
    source .env
    aws secretsmanager create-secret \
        --name sagemaker-github-pat \
         --secret-string "${GITHUB_PAT}"
}

function create_codestar_connection {
    aws codeconnections create-connection \
        --provider-type GitHub \
        --connection-name mc-connection
}

function launch_sagemaker_studio {
    aws sagemaker create-presigned-domain-url \
        --region "${AWS_REGION}" \
        --domain-id "${SAGEMAKER_DOMAIN_ID}" \
        --user-profile-name mc \
        --session-expiration-duration-in-seconds 43200
}

function publish_lambda_fn {
    
    # upload the file to the S3 bucket
    cd "${THIS_DIR}/lambda_functions/lambda_github_workflow_trigger"
    zip lambda-github-workflow-trigger.zip lambda_function.py
    aws s3 cp lambda-github-workflow-trigger.zip s3://${LAMBDA_FN_S3_BUCKET}/
    
    #Set up a Python virtual environment and get the dependencies installed
    mkdir -p lambda_layer
    cd lambda_layer
    python3 -m venv .venv
    source .venv/bin/activate

    rm -rf "site-packages"
    pip install pygithub -t "site-packages"
    deactivate

    # Generate the .zip file
    mv "site-packages/" python
    zip -r layer.zip python

    #Publish the layer to AWS
    aws lambda publish-layer-version --layer-name python39-github-arm64 \
        --description "Python3.9 pygithub" \
        --license-info "MIT" \
        --zip-file fileb://layer.zip \
        --compatible-runtimes python3.9 \
        --compatible-architectures "arm64"

}

time "${@}"