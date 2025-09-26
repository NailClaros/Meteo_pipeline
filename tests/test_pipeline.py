from datetime import datetime
import os
from master import run_pipeline_test as run_pipeline
from awsfuncs import file_exists_in_s3, list_files


def test_pipeline_success(db_conn, s3_test_good_client, test_bucket, test_prefix, db_rows):
    """End-to-end success case: fetch -> upload -> DB insert."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"weather_{today_str}.csv"
    s3_key = f"{test_prefix}{filename}"

    before_rows = len(db_rows())
    before_files = list_files(test_bucket, s3_test_good_client)

    status = run_pipeline(
        bucket_name=test_bucket,
        conn=db_conn,
        schema="aq_test_local",
        s3_client=s3_test_good_client,
        prefix=test_prefix,
        output_dir="data",
    )

    after_rows = len(db_rows())
    after_files = list_files(test_bucket, s3_test_good_client)

    # Assertions
    assert status == 0
    assert after_rows > before_rows # rows inserted
    assert file_exists_in_s3(test_bucket, s3_key, s3_test_good_client)
    assert s3_key in after_files
    assert len(after_files) > len(before_files)  # exactly one new file


def test_pipeline_duplicate_run(db_conn, s3_test_good_client, test_bucket, test_prefix, db_rows):
    """Second run should skip upload and DB insert."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"weather_{today_str}.csv"
    s3_key = f"{test_prefix}{filename}"

    # First run (success)
    status1 = run_pipeline(
        bucket_name=test_bucket,
        conn=db_conn,
        schema="aq_test_local",
        s3_client=s3_test_good_client,
        prefix=test_prefix,
        output_dir="data",
    )
    row_count_1 = len(db_rows())
    file_count_1 = len(list_files(test_bucket, s3_test_good_client))
    assert status1 == 0

    # Second run (duplicate)
    status2 = run_pipeline(
        bucket_name=test_bucket,
        conn=db_conn,
        schema="aq_test_local",
        s3_client=s3_test_good_client,
        prefix=test_prefix,
        output_dir="data",
    )
    row_count_2 = len(db_rows())
    file_count_2 = len(list_files(test_bucket, s3_test_good_client))

    assert status2 == 0
    assert row_count_2 == row_count_1  # no new rows inserted
    assert file_count_2 == file_count_1  # no duplicate upload
    assert file_exists_in_s3(test_bucket, s3_key, s3_test_good_client)


def test_pipeline_bad_s3_client(db_conn, s3_test_bad_client, test_bucket, test_prefix, db_rows, tmp_path):
    """Pipeline should fail on bad S3 client but keep local file."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"weather_{today_str}.csv"
    local_path = tmp_path / filename

    status = run_pipeline(
        bucket_name=test_bucket,
        conn=db_conn,
        schema="aq_test_local",
        s3_client=s3_test_bad_client,  # bad client
        prefix=test_prefix,
        output_dir=tmp_path,
    )

    assert status == 1  # fetch failed
    assert not os.path.exists(local_path)  # local file should not exist as a bad client prevents fetch
    assert len(db_rows()) == 0  # no DB rows inserted

def test_pipeline_upload_failure(bad_db_conn, s3_test_good_client, test_prefix, db_rows, tmp_path):
    """Pipeline should fail on upload but keep local file."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"weather_{today_str}.csv"
    local_path = tmp_path / filename

    status = run_pipeline(
        bucket_name="definitely-not-a-real-bucket",  # wrong bucket
        conn=bad_db_conn,
        schema="aq_test_local",
        s3_client=s3_test_good_client,
        prefix=test_prefix,
        output_dir=tmp_path,
    )

    assert status == 2  # upload failed
    assert len(db_rows()) == 0  # no DB rows inserted
    assert os.path.exists(local_path)

def test_pipeline_bad_db_connection(s3_test_good_client, bad_db_conn, test_bucket, test_prefix, db_rows):
    """Pipeline should fail DB insert but leave file in S3."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"weather_{today_str}.csv"
    s3_key = f"{test_prefix}{filename}"

    before_files = list_files(test_bucket, s3_test_good_client)

    status = run_pipeline(
        bucket_name=test_bucket,
        conn=bad_db_conn,  # broken connection
        schema="aq_test_local",
        s3_client=s3_test_good_client,
        prefix=test_prefix,
        output_dir="data",
    )

    after_files = list_files(test_bucket, s3_test_good_client)

    assert status == 3  # DB upload failed
    assert file_exists_in_s3(test_bucket, s3_key, s3_test_good_client)
    assert len(after_files) == len(before_files) + 1  # file still uploaded
    assert len(db_rows()) == 0  # no DB rows inserted