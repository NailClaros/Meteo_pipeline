import os
import requests
import boto3
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, ClientError

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
LAKE_BUCKET = os.getenv("BUCKET_NAME")

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

def upload_file(bucket, filepath, key):
    """Uploads a file to the S3 bucket."""
    try:
        # Load AWS credentials from environment variables
        s3.upload_file(filepath, bucket, key)
        # Delete local file only if upload succeeded
        os.remove(filepath)

    except (BotoCoreError, ClientError) as e:
        print(f"Upload failed: {e}")
        print(f"Keeping local file: {filepath}")

    except Exception as e:
        print(f"Unexpected error: {e}")
        print(f"Keeping local file: {filepath}")