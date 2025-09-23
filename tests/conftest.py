import pytest
import psycopg2
import boto3
import os
from dotenv import load_dotenv
load_dotenv()

@pytest.fixture(scope="session")
def db_conn():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )
    yield conn
    conn.close()

@pytest.fixture(autouse=True)
def prepare_schema(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            location_id TEXT,
            time TIMESTAMP,
            temperature REAL
        );
    """)
    db_conn.commit()
    cur.close()
    
    # Clean before each test
    cur = db_conn.cursor()
    cur.execute("TRUNCATE weather_data;")
    db_conn.commit()
    cur.close()

@pytest.fixture
def db_rows(db_conn):
    def _get_all():
        cur = db_conn.cursor()
        cur.execute("SELECT id, location, sensor_name_units, measurement, date_inserted FROM sensor_data ORDER BY id;")
        rows = cur.fetchall()
        cur.close()
        return rows
    return _get_all

@pytest.fixture(scope="session")
def s3_test_good_client():
    return boto3.client(
        "s3",
        region_name="us-east-1",  
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_T"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_T"),
    )

@pytest.fixture(scope="session")
def s3_test_bad_client():
    return boto3.client(
        "s3",
        region_name="us-east-1",  
        aws_access_key_id="bad_key",
        aws_secret_access_key="bad_secret",
    )

@pytest.fixture(scope="session")
def test_bucket():
    return os.getenv("TBUCKET")

@pytest.fixture(scope="session")
def test_prefix():
    return "meteos_test_folder/"

@pytest.fixture(autouse=True)
def cleanup_test_bucket(s3_test_good_client, test_bucket, test_prefix):
    yield  # run tests
    response = s3_test_good_client.list_objects_v2(Bucket=test_bucket, Prefix=test_prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3_test_good_client.delete_object(Bucket=test_bucket, Key=obj["Key"])
