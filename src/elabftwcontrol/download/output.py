from __future__ import annotations

import csv
import io
import sys
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Protocol, Sequence

import pandas as pd

from elabftwcontrol.core.interfaces import Pathlike
from elabftwcontrol.s3_utils import ParsedS3Path, write_lines_s3
from elabftwcontrol.utils import number_to_base


class OutputFormats(str, Enum):
    JSON = "json"
    CSV = "csv"
    EXCEL = "excel"
    PARQUET = "parquet"


class _LineWriterInterface(Protocol):
    def __call__(self, lines: Iterable[str]) -> None: ...


class LineWriter:
    """Callable class for writing lines to a text file.

    Instantiate using the `new` class method and pass a path.
    If no path is passed, the output will be directed to stdout.
    A path that is prefixed with s3:// will be written to AWS S3 if the
    extra `aws` dependencies are installed.
    """

    def __init__(self, line_writer: _LineWriterInterface) -> None:
        self.line_writer = line_writer

    def __call__(self, lines: Iterable[str]) -> None:
        self.line_writer(lines)

    @classmethod
    def new(cls, path: Optional[str] = None) -> LineWriter:
        writer = cls._select_writer_based_on_output(path)
        return cls(line_writer=writer)

    @classmethod
    def _select_writer_based_on_output(
        cls,
        path: Optional[str],
    ) -> _LineWriterInterface:
        writer: _LineWriterInterface
        if path is None:
            writer = _StdOutWriter()
        elif str(path).startswith("s3://"):
            writer = _S3TextWriter.new(path)
        else:
            writer = _TextFileWriter(path)
        return writer


class _StdOutWriter:
    def __call__(self, lines: Iterable[str]) -> None:
        for line in lines:
            print(line)


class _TextFileWriter:
    def __init__(
        self,
        path: Pathlike,
    ) -> None:
        self.path = path

    def __call__(self, lines: Iterable[str]) -> None:
        with open(self.path, "w") as f:
            for line in lines:
                f.write(line)
                f.write("\n")


class _S3TextWriter:
    def __init__(
        self,
        bucket: str,
        prefix: str,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix

    @classmethod
    def new(cls, path: str) -> _S3TextWriter:
        parsed_path = ParsedS3Path.from_path(path)
        return cls(
            bucket=parsed_path.bucket,
            prefix=parsed_path.prefix,
        )

    def __call__(self, lines: Iterable[str]) -> None:
        write_lines_s3(
            bucket=self.bucket,
            prefix=self.prefix,
            lines=lines,
        )


class _CSVWriterInterface(Protocol):
    def __call__(self, rows: Iterable[dict[str, Any]]) -> None: ...


class CSVWriter:
    """Callable class for rows to a CSV file.

    Instantiate using the `new` class method and pass a path and column names.
    If no path is passed, the output will be directed to stdout.
    A path that is prefixed with s3:// will be written to AWS S3 if the
    extra `aws` dependencies are installed.

    Row data is passed as dictionaries, with the keys being the column
    names and the values being the values in the row.
    """

    def __init__(
        self,
        header: Sequence[str],
        writer: _CSVWriterInterface,
    ) -> None:
        self.header = header
        self.writer = writer

    def __call__(self, rows: Iterable[dict[str, Any]]) -> None:
        self.writer(rows)

    @classmethod
    def new(
        cls,
        header: Sequence[str],
        path: Optional[Pathlike] = None,
    ) -> CSVWriter:
        writer = cls._select_writer_based_on_output(path, header=header)
        return cls(
            header=header,
            writer=writer,
        )

    @classmethod
    def _select_writer_based_on_output(
        cls,
        path: Optional[Pathlike],
        header: Sequence[str],
    ) -> _CSVWriterInterface:
        writer: _CSVWriterInterface
        if path is None:
            writer = _CSVToStdOutWriter(header=header)
        elif str(path).startswith("s3://"):
            raise NotImplementedError("Currently can't store CSVs directly on S3.")
        else:
            writer = _CSVToFileWriter(path=path, header=header)
        return writer

    @classmethod
    def write_rows(
        cls,
        file: Any,
        header: Sequence[str],
        rows: Iterable[dict[str, Any]],
    ) -> None:
        writer = csv.DictWriter(
            f=file,
            fieldnames=header,
            quoting=csv.QUOTE_NONNUMERIC,
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class _CSVToStdOutWriter:
    def __init__(
        self,
        header: Sequence[str],
    ) -> None:
        self.header = header

    def __call__(
        self,
        rows: Iterable[dict[str, Any]],
    ) -> None:
        CSVWriter.write_rows(
            file=sys.stdout,
            header=self.header,
            rows=rows,
        )


class _CSVToFileWriter:
    def __init__(
        self,
        path: Pathlike,
        header: Sequence[str],
    ) -> None:
        self.path = Path(path).expanduser()
        self.header = header

    def __call__(
        self,
        rows: Iterable[dict[str, Any]],
    ) -> None:
        with open(self.path, "w", newline="") as f:
            CSVWriter.write_rows(
                file=f,
                header=self.header,
                rows=rows,
            )


PandasOutputFormats = OutputFormats


class PandasWriter:
    """Write a pandas dataframe to a CSV or Parquet file"""

    def __init__(self, writer: Callable[[pd.DataFrame], None]) -> None:
        self.writer = writer

    def __call__(self, data: pd.DataFrame) -> None:
        self.writer(data)

    @classmethod
    def new(
        cls,
        path: Optional[Pathlike] = None,
        format: PandasOutputFormats = PandasOutputFormats.CSV,
        glue_info: Optional[str] = None,
    ) -> PandasWriter:
        writer: Callable[[pd.DataFrame], None]
        if path is None:
            writer = _PandasToStdOutWriter(format)
        elif str(path).startswith("s3://"):
            writer = _PandasToS3Writer.new(
                path=str(path),
                format=format,
                glue_info=glue_info,
            )
        else:
            writer = _PandasToFileWriter(path, format)
        return cls(writer=writer)

    @classmethod
    def write(
        cls,
        data: pd.DataFrame,
        path: Pathlike | io.BytesIO,
        format: PandasOutputFormats,
    ) -> None:
        if format == OutputFormats.CSV:
            data.to_csv(
                path,
                quoting=csv.QUOTE_NONNUMERIC,
                header=True,
                index=False,
                escapechar="\\",
            )
        elif format == OutputFormats.PARQUET:
            data.to_parquet(path=path)
        else:
            raise ValueError(f"Format '{format}' not saveable as pandas dataframe.")


class _PandasToS3Writer:
    """Very basic pandas DF to S3, no partitioning"""

    def __init__(self, write_method: Callable[[pd.DataFrame], None]) -> None:
        self.write_method = write_method

    def __call__(self, df: pd.DataFrame) -> None:
        self.write_method(df)

    @classmethod
    def new(
        cls,
        path: str,
        format: PandasOutputFormats,
        glue_info: Optional[str] = None,
    ) -> _PandasToS3Writer:
        try:
            from elabftwcontrol.s3_utils import wr
        except ImportError:
            raise RuntimeError(
                "You must install optional dependencies for interacting with AWS."
            )

        common_arguments: dict[str, Any] = {
            "path": path,
            "index": False,
        }

        if glue_info is not None:
            glue_db, glue_table = glue_info.split(":")
            common_arguments.update(
                {
                    "sanitize_columns": True,
                    "dataset": True,
                    "mode": "overwrite",
                    "database": glue_db,
                    "table": glue_table,
                }
            )

        if format == OutputFormats.CSV:

            def write_method(df: pd.DataFrame) -> None:
                wr.s3.to_csv(
                    df=df,
                    **common_arguments,
                )

        elif format == OutputFormats.PARQUET:

            def write_method(df: pd.DataFrame) -> None:
                wr.s3.to_parquet(
                    df=df,
                    compression="snappy",
                    **common_arguments,
                )

        else:
            raise ValueError(f"Output format '{format}' is not supported")

        return cls(write_method)


class _PandasToFileWriter:
    def __init__(
        self,
        path: Pathlike,
        format: PandasOutputFormats,
    ) -> None:
        self.path = Path(path).expanduser()
        self.format = format

    def __call__(
        self,
        data: pd.DataFrame,
    ) -> None:
        PandasWriter.write(
            data=data,
            path=self.path,
            format=self.format,
        )


class _PandasToStdOutWriter:
    def __init__(
        self,
        format: PandasOutputFormats,
    ) -> None:
        self.format = format

    def __call__(
        self,
        data: pd.DataFrame,
    ) -> None:
        buffer = io.BytesIO()
        PandasWriter.write(
            data=data,
            path=buffer,
            format=self.format,
        )
        sys.stdout.buffer.write(buffer.getvalue())


class _SplitDataFrame(Protocol):
    key: str
    data: pd.DataFrame


class ExcelWriter:
    """Writes multiple dataframes to different sheets in one Excel file"""

    def __init__(self, writer: Callable[[Iterable[_SplitDataFrame]], None]) -> None:
        self.writer = writer

    def __call__(
        self,
        sheets: Iterable[_SplitDataFrame],
    ) -> None:
        self.writer(sheets)

    @classmethod
    def new(cls, path: Optional[Pathlike] = None) -> ExcelWriter:
        writer: Callable[[Iterable[_SplitDataFrame]], None]
        if path is None:
            writer = _ExcelToStdOutWriter()
        elif str(path).startswith("s3://"):
            raise NotImplementedError("Currently can't store Excels directly on S3.")
        else:
            writer = _ExcelToFileWriter(path=path)
        return cls(writer=writer)

    @classmethod
    def write(
        cls,
        excelwriter: pd.ExcelWriter,
        sheets: Iterable[_SplitDataFrame],
    ) -> None:
        with excelwriter:
            for sheet in sheets:
                df = sheet.data
                df.to_excel(
                    excelwriter,
                    sheet_name=sheet.key,
                    index=False,
                )
                work_sheet = excelwriter.sheets[sheet.key]
                table_range = cls.get_table_range(df)
                work_sheet.add_table(table_range, cls.get_table_options(df))

    @classmethod
    def get_table_range(cls, df: pd.DataFrame) -> str:
        """Get the data range in Excel based on the dataframe shape"""
        n_rows, n_cols = df.shape
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        digits = [digit - 1 for digit in number_to_base(n_cols - 1, 26)]
        digits[-1] += 1
        last_col = "".join((alphabet[i] for i in digits))
        return f"A1:{last_col}{n_rows + 1}"

    @classmethod
    def get_table_options(cls, df: pd.DataFrame) -> dict[str, Any]:
        return {"columns": [{"header": column} for column in df.columns]}


class _ExcelToStdOutWriter:
    def __call__(
        self,
        sheets: Iterable[_SplitDataFrame],
    ) -> None:
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(
            buffer,
            engine="xlsxwriter",
        )
        ExcelWriter.write(writer, sheets)
        sys.stdout.buffer.write(buffer.getvalue())


class _ExcelToFileWriter:
    def __init__(
        self,
        path: Pathlike,
    ) -> None:
        self.path = Path(path).expanduser()

    def __call__(
        self,
        sheets: Iterable[_SplitDataFrame],
    ) -> None:
        writer = pd.ExcelWriter(
            self.path,
            engine="xlsxwriter",
        )
        ExcelWriter.write(writer, sheets)
