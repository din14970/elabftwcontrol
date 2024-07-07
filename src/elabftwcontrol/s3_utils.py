from __future__ import annotations

from typing import Iterable, Iterator, List, Literal, NamedTuple, Optional

import pandas as pd

try:
    import awswrangler as wr
    from awswrangler.typing import _S3WriteDataReturnValue
    from s3fs import S3FileSystem

    S3_ENABLED = True
except ImportError:
    S3_ENABLED = False


from elabftwcontrol._logging import logger


class ParsedS3Path(NamedTuple):
    bucket: str
    prefix: str

    @classmethod
    def from_path(cls, path: str) -> ParsedS3Path:
        if not path.startswith("s3://"):
            raise ValueError("Path must start with s3://")

        stripped_path = path[5:]
        split_path = stripped_path.split("/")
        bucket = split_path[0]
        prefix = "".join(split_path[1:])
        return cls(
            bucket=bucket,
            prefix=prefix,
        )

    @property
    def path(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"

    @property
    def stripped_path(self) -> str:
        return f"{self.bucket}/{self.prefix}"


def _assert_s3_enabled() -> None:
    assert S3_ENABLED, "You have to install the optional S3 dependencies"


def write_df_to_glue_table(
    df: pd.DataFrame,
    path: str,
    database: str,
    table: str,
    partitions: Optional[List[str]] = None,
) -> _S3WriteDataReturnValue:
    _assert_s3_enabled()
    mode: Literal["overwrite", "overwrite_partitions"]
    if partitions is not None:
        mode = "overwrite_partitions"
    else:
        mode = "overwrite"

    return wr.s3.to_parquet(
        df=df,
        path=path,
        index=False,
        sanitize_columns=True,
        dataset=True,
        partition_cols=partitions,
        mode=mode,
        database=database,
        table=table,
    )


def write_df_to_s3_parquet(
    bucket: str,
    prefix: str,
    dataframe: pd.DataFrame,
) -> None:
    """Write a dataframe without registering it in Glue"""
    _assert_s3_enabled()
    wr.s3.to_parquet(
        df=dataframe,
        path=ParsedS3Path(bucket=bucket, prefix=prefix).path,
        compression="snappy",
    )


def write_lines_s3(
    bucket: str,
    prefix: str,
    lines: Iterable[str],
) -> None:
    _assert_s3_enabled()
    s3 = S3FileSystem()
    # hack to count items, because python doesn't have pointers to ints
    number_of_lines = [0]

    def process_substring(substr: str) -> bytes:
        number_of_lines[0] += 1
        return (substr + "\n").encode("UTF-8")

    with s3.open(f"{bucket}/{prefix}", mode="wb") as f:
        f.writelines(
            (process_substring(line) for line in lines),
        )

    logger.info(
        "Wrote %s lines to bucket %s on prefix %s"
        % (number_of_lines[0], bucket, prefix)
    )


def read_s3_jsonl_as_df(
    bucket: str,
    prefix: str,
) -> pd.DataFrame:
    _assert_s3_enabled()
    s3 = S3FileSystem()
    with s3.open(
        ParsedS3Path(bucket=bucket, prefix=prefix).stripped_path,
        mode="rb",
    ) as f:
        df = pd.read_json(f, lines=True)
    return df


def read_lines_s3(
    bucket: str,
    prefix: str,
    lazy: bool = True,
) -> Iterable[str]:
    _assert_s3_enabled()
    if lazy:
        return _read_lines_s3_lazy(bucket, prefix)
    else:
        return _read_lines_s3_greedy(bucket, prefix)


def _read_lines_s3_greedy(
    bucket: str,
    prefix: str,
) -> List[str]:
    s3 = S3FileSystem()
    with s3.open(
        ParsedS3Path(bucket=bucket, prefix=prefix).stripped_path,
        mode="rb",
    ) as f:
        lines = f.readlines()
    return lines


def _read_lines_s3_lazy(
    bucket: str,
    prefix: str,
) -> Iterator[str]:
    s3 = S3FileSystem()
    with s3.open(
        ParsedS3Path(bucket=bucket, prefix=prefix).stripped_path,
        mode="rb",
    ) as f:
        try:
            yield f.readline()
        except Exception:
            raise StopIteration
