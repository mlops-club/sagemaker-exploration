#!/usr/bin/env bash
set -euo pipefail

# Replace these
ROLE_NAME="DataZoneLineagePoster"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
TRUST_PRINCIPAL_ARN=$(aws sts get-caller-identity --query Arn --output text)
SESSION_PROFILE="datalineage-role-session"

# 1. Create trust policy JSON
cat > trust.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "$TRUST_PRINCIPAL_ARN" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

echo "Creating IAM role $ROLE_NAME with trust policy..."
aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document file://trust.json \
  --description "Allows DataZone lineage posting" >/dev/null

ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME"
echo "Created role: $ROLE_ARN"

# 2. Attach inline policy for DataZone lineage
echo "Attaching inline policy to allow posting lineage events..."
cat > dz-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "datazone:PostLineageEvent"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "AllowPostLineageEvent" \
  --policy-document file://dz-policy.json

echo "Policy attached."

# 3. Configure AWS CLI profile to assume the role
echo "Configuring AWS CLI profile [$SESSION_PROFILE] to assume the role..."
aws configure set profile.$SESSION_PROFILE.role_arn "$ROLE_ARN"
aws configure set profile.$SESSION_PROFILE.source_profile default

echo "Profile [$SESSION_PROFILE] configured."

# 4. Use the new profile to assume role and get identity
echo "Testing assume-role with profile [$SESSION_PROFILE]..."
AWS_PROFILE="$SESSION_PROFILE" aws sts get-caller-identity

echo "✅ Setup complete. Use profile $SESSION_PROFILE to run your boto3 script."
