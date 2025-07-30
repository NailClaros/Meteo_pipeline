import os
import requests
import boto3
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, ClientError

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def list_files(bucket):
    """Lists all file names in the given S3 bucket."""
    response = s3.list_objects_v2(Bucket=bucket)
    if "Contents" in response:
        for obj in response["Contents"]:
            print("File: ", obj["Key"])
    else:
        print("Bucket is empty or doesn't exist.")

def file_exists_in_s3(bucket_name, key):
    """Checks if a file exists in the specified S3 bucket."""
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise  # Something else went wrong

def upload_file(bucket, filepath, key):
    """Uploads a file to the S3 bucket."""
    try:
        if not os.path.exists(filepath) or file_exists_in_s3(bucket, key):
            print(f"File '{filepath}' does not exist or already exists in S3 as '{key}'. Skipping upload.")
            print(f"File exists in S3: {file_exists_in_s3(bucket, key)}")
            print(f"File exists locally: {os.path.exists(filepath)}")
            return
        

        # Load AWS credentials from environment variables
        print(f"Uploading {filepath} to s3://{key}")
        s3.upload_file(filepath, bucket, key)

        # Delete local file only if upload succeeded
        print(f"upload successful: {filepath} -> s3://{key}")
        os.remove(filepath)

        print(f"Deleted local file: {filepath}")

    except (BotoCoreError, ClientError) as e:
        print(f"Upload failed: {e}")
        print(f"Keeping local file: {filepath}")

    except Exception as e:
        print(f"Unexpected error: {e}")
        print(f"Keeping local file: {filepath}")