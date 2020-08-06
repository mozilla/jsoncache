from jsoncache.loader import BUCKET_RE, PATH_RE


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
