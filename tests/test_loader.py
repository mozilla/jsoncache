import boto3
import pytest
import json
from jsoncache.loader import BUCKET_RE, PATH_RE
from moto import mock_s3

S3_BUCKET = "TEST_BUCKET"
S3_PATH = "TEST_PATH"


def install_mock_data():
    conn = boto3.resource("s3", region_name="us-west-2")

    conn.create_bucket(Bucket=S3_BUCKET)
    conn.Object(S3_BUCKET, S3_PATH).put(Body=json.dumps({"foo": 42}))


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


@pytest.mark.skip(reason="needs a test")
@mock_s3
def test_second_s3_load_fails():
    install_mock_data()
    pass


@pytest.mark.skip(reason="needs a test")
@mock_s3
def test_simple_s3_load():
    install_mock_data()
    pass
