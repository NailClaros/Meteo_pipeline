import os
import boto3
import psycopg2
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import datetime
from awsfuncs import file_exists_in_s3, get_s3_client
from io import StringIO
load_dotenv()

def file_already_uploaded(cursor, filename, schema: str | None = None) -> bool:
    """
    Check if the file has already been uploaded to the database.
    
    :param cursor: psycopg2 cursor
    :param filename: file name to check
    :param schema: optional schema name (defaults to "WeatherData")
    """
    schema = schema or "WeatherData"

    query = f"""
        SELECT EXISTS (
            SELECT 1 FROM "{schema}".formatted_weather_data WHERE file_name = %s
        );
    """

    cursor.execute(query, (filename,))
    return cursor.fetchone()[0]

def upload_weather_data_to_db(bucket_name=None, conn=None, filename=None, schema="WeatherData", s3_client=None):
    if bucket_name is None:
        bucket_name = os.getenv("BUCKET_NAME")
    
    # Determine connection
    close_conn = False
    if conn is None:
        conn = psycopg2.connect(os.getenv("DB_URL"))
        cursor = conn.cursor()
        close_conn = True
    else:
        cursor = conn.cursor()
    
    
    if filename is None:
        today_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"weather_{today_str}.csv"
    else:
        if not filename.startswith("weather_") or not filename.endswith(".csv"):
            raise ValueError("Filename must start with 'weather_' and end with '.csv'")

    # Check if file exists in S3
    if not file_exists_in_s3(bucket_name, filename):
        print(f"File {filename} does not exist in bucket {bucket_name}. Aborting.....")
        cursor.close()
        if close_conn:
            conn.close()
        return

    if file_already_uploaded(cursor, filename, schema):
        print(f"Data from file '{filename}' already exists in the database. Skipping insert.....")
        cursor.close()
        if close_conn:
            conn.close()
        return

    # Download file from S3
    if s3_client is None:
        s3_client = get_s3_client()
    obj = s3_client.get_object(Bucket=bucket_name, Key=filename)
    body = obj['Body'].read().decode("utf-8")
    df = pd.read_csv(StringIO(body))

    insert_query = f"""
        INSERT INTO "{schema}".formatted_weather_data (
            file_name, location_id, temp_f, cloud_cover_perc, surface_pressure, 
            wind_speed_80m_mph, wind_direction_80m_deg, time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        for _, row in df.iterrows():
            cursor.execute(
                insert_query,
                (
                    filename,
                    row["location_id"],
                    row["temperature (°F)"],
                    row["cloud cover (%)"],
                    row["surface pressure (hPa)"],
                    row["wind speed (80m elevation) (mph)"],
                    row["wind direction (80m elevation) (°)"],
                    row["time"]
                )
            )
        conn.commit()
        print(f"Inserted {len(df)} rows from {filename} into the database.....")
    except Exception as e:
        conn.rollback()
        print("Error inserting data:", e)
    finally:
        cursor.close()
        if close_conn:
            conn.close()

def upload_weather_data_to_s3_drain_bucket(bucket_name=os.getenv("BUCKET_NAME"), db_url=os.getenv("DB_URL")):
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' not in response:
            print(f"No files found in bucket '{bucket_name}'.")
            return

        for obj in response['Contents']:
            key = obj['Key']
            if not key.endswith(".csv"):
                continue

            filename = os.path.basename(key)

            if file_already_uploaded(cursor, filename):
                print(f"Skipping {filename} — already inserted.")
                continue

            print(f"Processing {filename}...")

            s3_obj = s3.get_object(Bucket=bucket_name, Key=key)
            body = s3_obj['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(body))

            insert_query = """
                INSERT INTO "WeatherData".formatted_weather_data (
                    file_name, "location_id", "temp_F", "cloud_cover_perc", "surface_pressure", 
                    "wind_speed_80m_mph", "wind_direction_80m_deg", "time"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            for _, row in df.iterrows():
                data_tuple = (
                    filename,
                    row['location_id'],
                    row['temperature (°F)'],
                    row['cloud cover (%)'],
                    row['surface pressure (hPa)'],
                    row['wind speed (80m elevation) (mph)'],
                    row['wind direction (80m elevation) (°)'],
                    row['time']
                )
                cursor.execute(insert_query, data_tuple)

            conn.commit()
            print(f"Inserted {len(df)} rows from {filename} into the database.")

    except Exception as e:
        conn.rollback()
        print("Error processing files:", e)

    finally:
        cursor.close()
        conn.close()

# upload_weather_data_to_db(
#     bucket_name=os.getenv("BUCKET_NAME"),
#     db_url=os.getenv("DB_URL"),
#     filename="weather_2025-07-29.csv"
# )

