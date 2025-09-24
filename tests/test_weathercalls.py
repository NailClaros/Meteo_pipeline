import os
from weathercalls import file_exists_in_s3, fetch_and_save_weather_data_test


def test_fetch_and_save_weather_data_uses_existing_file(
    make_test_file, 
    s3_test_good_client,
    test_bucket,
    test_prefix,
):
    # Get path from fixture (file already exists)
    existing_path = make_test_file

    # Run fetch — should reuse the existing file, not create a new one
    result_path = fetch_and_save_weather_data_test(
        bucket_name=test_bucket,
        s3_client=s3_test_good_client,
        output_dir=existing_path.parent,  # same tmp folder
        prefix=test_prefix,
    )

    # Verify same path returned, file exists
    assert result_path == str(existing_path)
    assert os.path.exists(result_path)
    filename = os.path.basename(result_path)
    s3_key = f"{test_prefix}{filename}"

    ## Verify not in S3 yet as it is only local
    assert file_exists_in_s3(test_bucket, s3_key, s3_test_good_client) is False


def test_fetch_and_save_weather_data_skips_if_in_s3(
    tmp_path,
    s3_test_good_client,
    test_bucket,
    test_prefix,
    make_test_file
):
    existing_path = make_test_file
    assert os.path.exists(existing_path), "Fixture file should exist locally"

    # Upload to S3
    s3_key = f"{test_prefix}{os.path.basename(existing_path)}"
    s3_test_good_client.upload_file(str(existing_path), test_bucket, s3_key)

    # Call function, should skip because file exists in S3
    result_path = fetch_and_save_weather_data_test(
        bucket_name=test_bucket,
        s3_client=s3_test_good_client,
        output_dir=tmp_path,
        prefix=test_prefix,
    )

    # The function returns the same filename but does NOT create a new local file
    assert os.path.basename(result_path) == os.path.basename(existing_path)
    assert not os.path.exists(tmp_path / os.path.basename(existing_path)), "No new local file should be created"

    # Only 1 object exists in S3
    response = s3_test_good_client.list_objects_v2(Bucket=test_bucket, Prefix=test_prefix)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    assert keys.count(s3_key) == 1, "No duplicate uploads should occur"

def test_fetch_and_save_weather_data_creates_file(
    tmp_path,
    s3_test_good_client,
    test_bucket,
    test_prefix
):

    result_path = fetch_and_save_weather_data_test(
        bucket_name=test_bucket,
        s3_client=s3_test_good_client,
        output_dir=tmp_path,
        prefix=test_prefix,
    )

    # File should exist locally
    assert os.path.exists(result_path), "Local CSV file should have been created"