import boto3
import os

# Initialize AWS Batch client
batch_client = boto3.client("batch")

# Environment variables for Batch Job configuration
JOB_DEFINITION = os.getenv("JOB_DEFINITION", "vivid-dev-case-overview-definition")
JOB_QUEUE = os.getenv("JOB_QUEUE","vivid-dev-case-overview-queue")
JOB_NAME = os.getenv("JOB_NAME", "")
BUCKET_NAME = os.getenv("BUCKET_NAME", "")
SECRET_ARN = os.getenv("SECRET_ARN", "")

def pdf_extraction_trigger(event, context):
    try:
        response = batch_client.submit_job(
            jobName=JOB_NAME,
            jobQueue=JOB_QUEUE,
            jobDefinition=JOB_DEFINITION,
            containerOverrides={
                "environment": [
                    {"name": "BUCKET_NAME", "value": BUCKET_NAME},
                    {"name": "SECRET_ARN", "value": SECRET_ARN}
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



