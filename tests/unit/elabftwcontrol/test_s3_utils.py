from elabftwcontrol.s3_utils import ParsedS3Path


def test_parsed_s3_path() -> None:
    test_path = "s3://mybucket/my/prefix/some/file"
    path = ParsedS3Path.from_path("s3://mybucket/my/prefix/some/file")
    assert path.bucket == "mybucket"
    assert path.prefix == "my/prefix/some/file"
    assert path.path == test_path
    assert path.stripped_path == "mybucket/my/prefix/some/file"
