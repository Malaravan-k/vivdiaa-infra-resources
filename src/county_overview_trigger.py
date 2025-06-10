import boto3
import os

# Initialize AWS Batch client with the specified region
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
batch_client = boto3.client("batch", region_name=AWS_REGION)

# Environment variables for Batch Job configuration
JOB_DEFINITION = os.getenv("JOB_DEFINITION", "vivid-dev-case-overview-definition")
JOB_QUEUE = os.getenv("JOB_QUEUE", "vivid-dev-case-overview-queue")
JOB_NAME = os.getenv("JOB_NAME", "Caseoverview_Batch_Job")

# Additional environment variables for application configuration
PASSWORD = os.getenv("PASSWORD", "ai1!e#TvNL*z#KGY7kwLrd)&")
CASE_TYPES = os.getenv("CASE_TYPES", '["CVFCV", "CVFM", "FORSP", "MISC"]')
CASE_CATEGORY = os.getenv("CASE_CATEGORY", "CV")
APP_CLIENT_ID = os.getenv("APP_CLIENT_ID", "f052f213-a46f-4e3c-8cbe-2c4f6025d23f")
TOKEN_SCOPE = os.getenv("TOKEN_SCOPE", "api://prd-rpa-web-services.nclea.gov/User.ReadAccess")
EMAIL = os.getenv("EMAIL", "vivdiaa.svc@nclea.gov")
BASE_URL = os.getenv("BASE_URL", "https://prdaws.nccourts.org/rpa_web_services/api/v1/partycases/")
BUCKET_NAME = os.getenv("BUCKET_NAME", "vivid-dev-county-details")

def handler(event, context):
    try:
        response = batch_client.submit_job(
            jobName=JOB_NAME,
            jobQueue=JOB_QUEUE,
            jobDefinition=JOB_DEFINITION,
            containerOverrides={
                "environment": [
                    {"name": "AWS_REGION", "value": AWS_REGION},
                    {"name": "PASSWORD", "value": PASSWORD},
                    {"name": "CASE_TYPES", "value": CASE_TYPES},
                    {"name": "CASE_CATEGORY", "value": CASE_CATEGORY},
                    {"name": "APP_CLIENT_ID", "value": APP_CLIENT_ID},
                    {"name": "TOKEN_SCOPE", "value": TOKEN_SCOPE},
                    {"name": "EMAIL", "value": EMAIL},
                    {"name": "BASE_URL", "value": BASE_URL},
                    {"name": "BUCKET_NAME", "value": BUCKET_NAME}
                ]
            }
        )
        
        print(f"Batch Job Submitted Successfully: {response['jobId']}")
        return {
            "statusCode": 200,
            "body": f"Batch Job {response['jobId']} submitted successfully!"
        }
    except Exception as e:
        print(f"Error submitting Batch Job: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }