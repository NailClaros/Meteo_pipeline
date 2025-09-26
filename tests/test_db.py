import pytest
import pandas as pd
from db import file_already_uploaded, upload_weather_data_to_db


@pytest.fixture
def sample_weather_df():
    return pd.DataFrame({
        "location_id": ["LOC1", "LOC2"],
        "temperature (°F)": [70.5, 75.2],
        "cloud cover (%)": [20.0, 50.0],
        "surface pressure (hPa)": [1015.0, 1012.0],
        "wind speed (80m elevation) (mph)": [5.0, 7.0],
        "wind direction (80m elevation) (°)": [180.0, 90.0],
        "time": ["2025-07-20 12:00:00", "2025-07-20 13:00:00"]
    })


# Helper to insert dummy weather rows into formatted_weather_data
def insert_dummy_weather_data(db_conn, df, filename="weather_test.csv"):
    cur = db_conn.cursor()
    insert_query = """
        INSERT INTO "aq_test_local".formatted_weather_data (
            file_name, location_id, temp_f, cloud_cover_perc, surface_pressure,
            wind_speed_80m_mph, wind_direction_80m_deg, time
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (file_name, location_id, time) DO NOTHING
        
    """
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            filename,
            row["location_id"],
            row["temperature (°F)"],
            row["cloud cover (%)"],
            row["surface pressure (hPa)"],
            row["wind speed (80m elevation) (mph)"],
            row["wind direction (80m elevation) (°)"],
            row["time"]
        ))
    db_conn.commit()
    cur.close()
    return filename


## Tests below


def test_file_already_uploaded_false(db_conn):
    cur = db_conn.cursor()
    result = file_already_uploaded(cur, "non_existent_file.csv", schema="aq_test_local")
    cur.close()
    assert result is False

def test_file_already_uploaded_true(db_conn, sample_weather_df):
    insert_dummy_weather_data(db_conn, sample_weather_df, filename="weather_exists.csv")

    cur = db_conn.cursor()
    result = file_already_uploaded(cur, "weather_exists.csv", schema="aq_test_local")
    cur.close()

    assert result is True

def test_insert_data_and_count(db_conn, sample_weather_df):
    filename = insert_dummy_weather_data(db_conn, sample_weather_df)

    cur = db_conn.cursor()
    cur.execute('SELECT COUNT(*) FROM "aq_test_local".formatted_weather_data WHERE File_name = %s;', (filename,))
    count = cur.fetchone()[0]
    cur.close()

    assert count == len(sample_weather_df)

def test_multiple_files_distinct(db_conn, sample_weather_df):
    insert_dummy_weather_data(db_conn, sample_weather_df, filename="file1.csv")
    insert_dummy_weather_data(db_conn, sample_weather_df, filename="file2.csv")

    cur = db_conn.cursor()
    cur.execute('SELECT COUNT(DISTINCT file_name) FROM "aq_test_local".formatted_weather_data;')
    count = cur.fetchone()[0]
    cur.close()

    assert count == 2

def test_upload_weather_data_to_db_with_test_db(
    db_conn, s3_test_good_client, test_bucket, tmp_path, sample_weather_df
):
    # Save CSV locally
    csv_file = tmp_path / "weather_test.csv"
    sample_weather_df.to_csv(csv_file, index=False)

    # Upload to test bucket
    s3_test_good_client.upload_file(str(csv_file), test_bucket, "weather_test.csv")

    # Call the function using fixture connection
    upload_weather_data_to_db(bucket_name=test_bucket, conn=db_conn, filename="weather_test.csv", schema="aq_test_local")

    # Verify inserted rows
    cur = db_conn.cursor()
    cur.execute('SELECT COUNT(*) FROM "aq_test_local".formatted_weather_data WHERE File_name = %s;', ("weather_test.csv",))
    count = cur.fetchone()[0]
    cur.close()

    assert count == len(sample_weather_df)

def test_unique_constraint(db_conn, sample_weather_df, db_rows):
    filename = "unique_test.csv"

    insert_dummy_weather_data(db_conn, sample_weather_df, filename=filename)
    
    rows_before = db_rows()

    assert len(rows_before) == len(sample_weather_df)


    insert_dummy_weather_data(db_conn, sample_weather_df, filename=filename)
    
    rows_after = db_rows()
    assert len(rows_after) == len(sample_weather_df)
    assert rows_before == rows_after

