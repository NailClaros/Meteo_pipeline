import os
from weathercalls import fetch_and_save_weather_data, fetch_and_save_weather_data_test
from db import upload_weather_data_to_db
from awsfuncs import file_exists_in_s3, get_s3_client, upload_file
from dotenv import load_dotenv
from datetime import datetime
import psycopg2

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")


def run_pipeline():
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"weather_{today_str}.csv"
    local_path = os.path.join("data", filename)
    s3_key = f"{filename}"

    # 1. If local file already exists -> skip fetching
    if os.path.exists(local_path):
        print(f"Local file '{filename}' already exists. Skipping fetch.")
    else:
        fetch_and_save_weather_data()

    # 2. If already in S3 -> skip upload
    if file_exists_in_s3(BUCKET_NAME, s3_key):
        print(f"File '{filename}' already exists in S3. Skipping upload.")
    else:
        upload_file(BUCKET_NAME, local_path, s3_key)

    # 3. Upload weather data from S3 directly to database (skips duplicates in DB)
    upload_weather_data_to_db(
        bucket_name=BUCKET_NAME
    )

# run_pipeline()

def run_pipeline_test(
    bucket_name=None,
    conn=None,
    schema="aq_test_local",
    s3_client=None,
    output_dir="data",
    prefix="",
):
    """
    Run the weather data pipeline with test-friendly hooks.

    Returns status codes:
      0 = success
      1 = fetch failed
      2 = upload failed
      3 = DB upload failed
    """

    if bucket_name is None:
        bucket_name = BUCKET_NAME
    if not bucket_name:
        print("No bucket name provided or set in environment.")
        return 1

    if s3_client is None:
        s3_client = get_s3_client()

    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"weather_{today_str}.csv"
    local_path = os.path.join(output_dir, filename)
    s3_key = f"{prefix}{filename}"

    # Step 1. Fetch weather data if not present
    try:
        if os.path.exists(local_path):
            print(f"Local file '{filename}' already exists. Skipping fetch.")
        else:
            fetch_and_save_weather_data_test(
                date=today_str,
                bucket_name=bucket_name,
                output_dir=output_dir,
                s3_client=s3_client,
                prefix=prefix,
            )
    except Exception as e:
        print(f"Fetch failed: {e}")
        return 1

    # Step 2. Upload to S3 if not already present
    try:
        if file_exists_in_s3(bucket_name, s3_key, s3_client):
            print(f"File '{filename}' already exists in S3. Skipping upload.")
        else:
            upload_file(bucket_name, local_path, s3_key, s3_client)

        if not file_exists_in_s3(bucket_name, s3_key, s3_client):
            print(f"Upload to S3 failed for unknown reasons.")
            return 2
    except Exception as e:
        print(f"S3 upload failed: {e}")
        return 2

    # Step 3. Load from S3 into DB
    try:
        upload_weather_data_to_db(
            bucket_name=bucket_name,
            conn=conn,
            filename=filename,
            schema=schema,
            s3_client=s3_client,
        )
    except Exception as e:
        print(f"DB upload failed: {e}")
        return 3

    return 0
