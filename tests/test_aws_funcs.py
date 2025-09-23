import pytest
from botocore.exceptions import ClientError
from awsfuncs import upload_file, file_exists_in_s3, list_files



def test_upload_and_delete_file(s3_test_good_client, test_bucket, test_prefix, tmp_path):
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("hello test")
    key = f"{test_prefix}pytest/test_file.txt"

    
    upload_file(test_bucket, str(file_path), key)

    # verify exists
    assert file_exists_in_s3(test_bucket, key) is True

    s3_test_good_client.delete_object(Bucket=test_bucket, Key=key)
    assert file_exists_in_s3(test_bucket, key) is False


def test_list_files(s3_test_good_client, test_bucket, test_prefix, tmp_path):
    key = f"{test_prefix}pytest/another_file.txt"
    file_path = tmp_path / "another_file.txt"
    file_path.write_text("data")

    # upload directly so list_files can discover it
    s3_test_good_client.upload_file(str(file_path), test_bucket, key)

    # run your function (prints only, so we just check no exceptions are raised)
    list_files(test_bucket, s3_test_good_client)

    # cleanup
    s3_test_good_client.delete_object(Bucket=test_bucket, Key=key)


def test_file_exists_in_s3_true_false(s3_test_good_client, test_bucket, test_prefix, tmp_path):
    key = f"{test_prefix}pytest/exists_check.txt"
    file_path = tmp_path / "exists_check.txt"
    file_path.write_text("check me")

    s3_test_good_client.upload_file(str(file_path), test_bucket, key)

    assert file_exists_in_s3(test_bucket, key) is True

    # delete and verify false
    s3_test_good_client.delete_object(Bucket=test_bucket, Key=key)
    assert file_exists_in_s3(test_bucket, key) is False


def test_good_client_lists_files(s3_test_good_client, test_bucket, test_prefix, tmp_path):
    # Create a test file
    file_path = tmp_path / "list_test_file.txt"
    file_path.write_text("data for list test")
    key = f"{test_prefix}list_test_file.txt"

    # Upload directly via boto3
    s3_test_good_client.upload_file(str(file_path), test_bucket, key)

    # Use your list_files function to verify it "finds" the file
    files = list_files(test_bucket, s3_test_good_client)  # optionally, modify list_files to return keys
    assert any(key in f for f in files)

    # Cleanup
    s3_test_good_client.delete_object(Bucket=test_bucket, Key=key)



def test_bad_client_raises_on_list(s3_test_bad_client, test_bucket):
    # Expect a ClientError when calling list_files
    with pytest.raises(ClientError) as excinfo:
        files = list_files(test_bucket, s3_test_bad_client)

    # Check that the error code is a type of authentication failure
    assert excinfo.value.response["Error"]["Code"] in [
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "AccessDenied"
    ]



@pytest.fixture
def tmp_file_with_key(tmp_path, test_prefix):
    """
    Fixture that creates a temporary file for testing uploads.
    Returns a callable that recreates the file and gives the S3 key.
    """
    def _create(filename="tmp_file.txt", content="data"):
        file_path = tmp_path / filename
        file_path.write_text(content)
        key = f"{test_prefix}{filename}"
        return file_path, key  # return Path object, not string

    return _create

def test_file_not_exists_skips(s3_test_good_client, test_bucket):
    # Use a filename that does not exist
    non_existent_path = "does_not_exist.txt"
    key = f"test_prefix/{non_existent_path}"  

    upload_file(test_bucket, str("does_not_exist.txt"), key, s3_client=s3_test_good_client)

    # Nothing should be uploaded
    assert not file_exists_in_s3(test_bucket, key, s3_test_good_client)

def test_file_already_in_s3_skips(tmp_file_with_key, s3_test_good_client, test_bucket):
    # First upload
    tmp_file, key = tmp_file_with_key()
    upload_file(test_bucket, str(tmp_file), key, s3_client=s3_test_good_client)

    # Assert file exists in S3 and local file is deleted
    assert file_exists_in_s3(test_bucket, key, s3_test_good_client)
    assert not tmp_file.exists()

    # Re-create the file for the second upload attempt
    tmp_file, key = tmp_file_with_key()
    upload_file(test_bucket, str(tmp_file), key, s3_client=s3_test_good_client)

    # Check that the local file still exists
    assert tmp_file.exists()


def test_successful_upload_deletes_local(tmp_file_with_key, s3_test_good_client, test_bucket):
    tmp_file, key = tmp_file_with_key()
    upload_file(test_bucket, str(tmp_file), key, s3_client=s3_test_good_client)

    # Local file should be deleted
    assert not tmp_file.exists()
    # File should exist in S3
    assert file_exists_in_s3(test_bucket, key, s3_test_good_client)


def test_upload_failure_keeps_local(tmp_file_with_key, s3_test_bad_client, test_bucket):
    tmp_file, key = tmp_file_with_key()
    upload_file(test_bucket, str(tmp_file), key, s3_client=s3_test_bad_client)

    # Local file should still exist because upload failed
    assert tmp_file.exists()


