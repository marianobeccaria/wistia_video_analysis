# GitHub Actions Manual CDK Deploy Setup

This document explains how to configure a manual GitHub Actions deployment workflow that uses GitHub OIDC to assume an AWS IAM role. This avoids storing long-lived AWS access keys in GitHub.

## Overview

The deploy flow is:

```txt
GitHub Actions workflow_dispatch
  -> GitHub OIDC token
  -> AWS IAM role trust policy
  -> sts:AssumeRoleWithWebIdentity
  -> CDK deploy
```

The GitHub repository stores only the deploy role ARN as a repository secret:

```txt
AWS_DEPLOY_ROLE_ARN
```

No AWS access keys are stored in GitHub.

## 1. Confirm GitHub OIDC Provider Exists In AWS

Check for an existing provider:

```bash
aws iam list-open-id-connect-providers
```

You should see:

```txt
arn:aws:iam::<aws-account-id>:oidc-provider/token.actions.githubusercontent.com
```

If it does not exist, create it:

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

## 2. Create IAM Trust Policy

Create the trust policy outside the repo or in `/tmp` so it is not accidentally committed.

```bash
cat > /tmp/github-actions-cdk-trust-policy.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<aws-account-id>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": [
            "repo:<github-user-or-org>/<repo-name>:ref:refs/heads/main",
            "repo:<github-user-or-org>/<repo-name>:ref:refs/heads/dev"
          ]
        }
      }
    }
  ]
}
JSON
```

Replace:

```txt
<aws-account-id>
<github-user-or-org>
<repo-name>
```

Example:

```txt
repo:my-github-user/wistia_video_analysis:ref:refs/heads/main
```

The GitHub user or organization and repository name must match the GitHub URL exactly.

## 3. Create IAM Deploy Role

```bash
aws iam create-role \
  --role-name github-actions-cdk-deploy-role \
  --assume-role-policy-document file:///tmp/github-actions-cdk-trust-policy.json
```

## 4. Attach Deploy Permissions

For a dedicated learning account, the simplest option is:

```bash
aws iam attach-role-policy \
  --role-name github-actions-cdk-deploy-role \
  --policy-arn arn:aws:iam::aws:policy/AdministratorAccess
```

For shared or production AWS accounts, use a narrower custom policy instead of `AdministratorAccess`.

CDK deployment for this project needs permissions for services such as:

- CloudFormation
- S3
- IAM
- Glue
- Secrets Manager
- Lambda
- CloudWatch Logs
- STS

## 5. Get Role ARN

```bash
aws iam get-role \
  --role-name github-actions-cdk-deploy-role \
  --query 'Role.Arn' \
  --output text
```

Example:

```txt
arn:aws:iam::<aws-account-id>:role/github-actions-cdk-deploy-role
```

## 6. Add GitHub Repository Secret

In GitHub:

```txt
Repository -> Settings -> Secrets and variables -> Actions -> New repository secret
```

Name:

```txt
AWS_DEPLOY_ROLE_ARN
```

Value:

```txt
arn:aws:iam::<aws-account-id>:role/github-actions-cdk-deploy-role
```

## 7. Required Workflow Permissions

The deploy workflow must include:

```yaml
permissions:
  id-token: write
  contents: read
```

`id-token: write` allows GitHub Actions to request the OIDC token. Without it, AWS role assumption fails.

## 8. Manual Deploy Workflow

Example workflow:

```yaml
name: Deploy

on:
  workflow_dispatch:
    inputs:
      environment:
        description: Deployment environment name
        required: true
        default: dev
      aws_region:
        description: AWS region
        required: true
        default: us-east-1
      s3_bucket_name:
        description: Existing data lake bucket name
        required: true
      pipeline_schedule_enabled:
        description: Enable scheduled production runs
        required: true
        default: "false"

jobs:
  deploy:
    name: CDK Deploy
    runs-on: ubuntu-latest

    permissions:
      id-token: write
      contents: read

    env:
      ENVIRONMENT: ${{ inputs.environment }}
      AWS_REGION: ${{ inputs.aws_region }}
      S3_BUCKET_NAME: ${{ inputs.s3_bucket_name }}
      PIPELINE_SCHEDULE_ENABLED: ${{ inputs.pipeline_schedule_enabled }}

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_DEPLOY_ROLE_ARN }}
          aws-region: ${{ inputs.aws_region }}

      - name: Set AWS account id
        run: |
          echo "AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)" >> "$GITHUB_ENV"

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Set up Node.js
        uses: actions/setup-node@v4
        with:
          node-version: "20"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          python -m pip install -r requirements.txt
          python -m pip install -r infrastructure/requirements.txt
          npm install -g aws-cdk

      - name: CDK synth
        working-directory: infrastructure
        run: |
          cdk synth

      - name: CDK deploy
        working-directory: infrastructure
        run: |
          cdk deploy --require-approval never
```

## 9. Run Manual Deploy

In GitHub:

```txt
Repository -> Actions -> Deploy -> Run workflow
```

For the first test run, use:

```txt
pipeline_schedule_enabled: false
```

After the workflow succeeds, rerun with:

```txt
pipeline_schedule_enabled: true
```

if the scheduled production trigger should be enabled.

## 10. Troubleshooting

### Not authorized to perform sts:AssumeRoleWithWebIdentity

Usually caused by the trust policy not matching the GitHub repo or branch.

Check the trust policy:

```bash
aws iam get-role \
  --role-name github-actions-cdk-deploy-role \
  --query 'Role.AssumeRolePolicyDocument' \
  --output json
```

Verify:

- AWS account ID is correct.
- GitHub username or organization is correct.
- Repository name is correct.
- Branch name is correct.
- Workflow includes `permissions: id-token: write`.

### Deploy Workflow Does Not Appear In GitHub Actions

`workflow_dispatch` workflows appear when the workflow file exists on the default branch.

If the default branch is `main`, make sure `.github/workflows/deploy.yml` is pushed to `main`.

### CI Runs But Deploy Does Not Run Automatically

This is expected. The deploy workflow uses `workflow_dispatch`, so it only runs manually.
