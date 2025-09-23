import os
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
    """Lists all file names in the given S3 bucket and returns them."""
    response = s3.list_objects_v2(Bucket=bucket)
    if "Contents" in response:
        keys = [obj["Key"] for obj in response["Contents"]]
        return keys
    else:
        print("Bucket is empty or doesn't exist.")
        return []

def file_exists_in_s3(bucket_name, key, s3_client=None):
    """Checks if a file exists in S3 using provided client."""
    if s3_client is None:
        s3_client = boto3.client("s3")
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise #Some other error occurred


def upload_file(bucket, filepath, key, s3_client=None):
    """Uploads a file to the S3 bucket using the given s3_client, or default global client."""
    if s3_client is None:
        s3_client = s3  # fallback to your global client

    try:
        exists_in_s3 = file_exists_in_s3(bucket, key, s3_client)
        exists_locally = os.path.exists(filepath)

        if not exists_locally or exists_in_s3:
            print(f"File '{filepath}' does not exist or already exists in S3 as '{key}'. Skipping upload.")
            print(f"File exists in S3: {exists_in_s3}")
            print(f"File exists locally: {exists_locally}")
            return

        print(f"Uploading {filepath} to s3://{key}")
        s3_client.upload_file(filepath, bucket, key)
        print(f"upload successful: {filepath} -> s3://{key}")
        os.remove(filepath)
        print(f"Deleted local file: {filepath}")

    except (BotoCoreError, ClientError) as e:
        print(f"Upload failed: {e}")
        print(f"Keeping local file: {filepath}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(f"Keeping local file: {filepath}")
