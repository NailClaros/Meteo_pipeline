import os
from weathercalls import fetch_and_save_weather_data
from db import upload_weather_data_to_db
from awsfuncs import file_exists_in_s3, s3, upload_file
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
DB_URL = os.getenv("DB_URL")


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
        bucket_name=BUCKET_NAME,
        db_url=DB_URL
    )

run_pipeline()