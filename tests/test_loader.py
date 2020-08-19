import json
import time

from moto import mock_s3
import boto3

from jsoncache import ThreadedObjectCache
from jsoncache.loader import BUCKET_RE, PATH_RE

TEST_S3_BUCKET = "TEST-BUCKET"
TEST_S3_PATH = "TEST_PATH/somefile.json"

FIXTURE_DATA = {"foo": 42}


def install_mock_data():
    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=TEST_S3_BUCKET)
    conn.Object(TEST_S3_BUCKET, TEST_S3_PATH).put(Body=json.dumps(FIXTURE_DATA))


def delete_mock_data():
    conn = boto3.resource("s3", region_name="us-west-2")
    conn.Object(TEST_S3_BUCKET, TEST_S3_PATH).delete()


def test_regex():
    """
    Test the regular expressions are setup properly to validate paths
    """
    assert BUCKET_RE.match("a-valid-bucket013-1") is not None
    assert BUCKET_RE.match("an_valid-bucket013-1") is None

    assert PATH_RE.match("/no/leading/slashes") is None
    assert PATH_RE.match("no/trailing/slashes/") is None
    assert PATH_RE.match("some/valid/path") is not None
    assert PATH_RE.match("some/valid/path/dots.are.ok") is not None
    assert PATH_RE.match("some/valid/path/dashes-are-ok") is not None
    assert PATH_RE.match("some/valid/path/underscore_is_ok") is not None


@mock_s3
def test_simple_s3_load():
    install_mock_data()
    t = ThreadedObjectCache("s3", TEST_S3_BUCKET, TEST_S3_PATH, 10)
    assert FIXTURE_DATA == t.get()


@mock_s3
def test_second_s3_load_fails():
    install_mock_data()
    t = ThreadedObjectCache("s3", TEST_S3_BUCKET, TEST_S3_PATH, 1)
    assert FIXTURE_DATA == t.get()

    delete_mock_data()
    time.sleep(2)
    assert FIXTURE_DATA == t.get()
