#!/bin/bash
set -e

# --------------------------------------------------
# CONFIG — CHANGE ONLY THESE
# --------------------------------------------------
REGION="us-east-1"

STACK_NAME="inaas-emr-serverless"
EXECUTION_ROLE_NAME="EMRServerlessJobRole-INAAS"

JOB_S3_PATH="s3://inaas-raw-analytics-dev/emr/jobs/emr_test_job.py"
LOG_S3_PATH="s3://inaas-raw-analytics-dev/emr/logs/"

# --------------------------------------------------
# FETCH APPLICATION ID FROM CLOUDFORMATION
# --------------------------------------------------
echo "Fetching EMR Serverless Application ID..."

APPLICATION_ID=$(aws cloudformation describe-stacks \
  --stack-name ${STACK_NAME} \
  --region ${REGION} \
  --query "Stacks[0].Outputs[?OutputKey=='ApplicationId'].OutputValue" \
  --output text)

if [ -z "$APPLICATION_ID" ]; then
  echo "❌ Failed to get ApplicationId from CloudFormation"
  exit 1
fi

echo "✔ ApplicationId: $APPLICATION_ID"

# --------------------------------------------------
# FETCH EXECUTION ROLE ARN
# --------------------------------------------------
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

EXECUTION_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${EXECUTION_ROLE_NAME}"

echo "✔ Execution Role ARN: $EXECUTION_ROLE_ARN"

# --------------------------------------------------
# SUBMIT EMR SERVERLESS JOB
# --------------------------------------------------
echo "Submitting EMR Serverless Spark job..."

JOB_RUN_ID=$(aws emr-serverless start-job-run \
  --application-id "$APPLICATION_ID" \
  --execution-role-arn "$EXECUTION_ROLE_ARN" \
  --region "$REGION" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"$JOB_S3_PATH\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {
        \"logUri\": \"$LOG_S3_PATH\"
      }
    }
  }" \
  --query "jobRunId" \
  --output text)

echo "✔ Job submitted successfully"
echo "✔ JobRunId: $JOB_RUN_ID"

# --------------------------------------------------
# POLL JOB STATUS
# --------------------------------------------------
echo "Waiting for job to complete..."

while true; do
  STATUS=$(aws emr-serverless get-job-run \
    --application-id "$APPLICATION_ID" \
    --job-run-id "$JOB_RUN_ID" \
    --region "$REGION" \
    --query "jobRun.state" \
    --output text)

  echo "Job status: $STATUS"

  if [[ "$STATUS" == "SUCCESS" ]]; then
    echo "🎉 Job completed successfully!"
    break
  fi

  if [[ "$STATUS" == "FAILED" || "$STATUS" == "CANCELLED" ]]; then
    echo "❌ Job failed. Check logs at:"
    echo "   $LOG_S3_PATH"
    exit 1
  fi

  sleep 10
done
