from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from elabftwcontrol.download.output import (
    CSVWriter,
    ExcelWriter,
    LineWriter,
)


def test_text_to_file_writer() -> None:
    with TemporaryDirectory() as tmpdir:
        data = [
            "line1",
            "line2\nline3",
            """\
line4
line5""",
        ]
        filepath = Path(tmpdir) / "test_file.csv"

        writer = LineWriter.new(filepath)
        writer(data)

        with open(filepath, "r") as f:
            content = f.read()

        expected = """\
line1
line2
line3
line4
line5
"""
        assert expected == content


def test_text_to_stdout_writer() -> None:
    with patch("sys.stdout", new=StringIO()) as fake_out:
        data = [
            "line1",
            "line2\nline3",
            """\
line4
line5""",
        ]

        writer = LineWriter.new()
        writer(data)

        expected = """\
line1
line2
line3
line4
line5
"""
        assert expected == fake_out.getvalue()


def test_csv_writer_write_rows() -> None:
    with TemporaryDirectory() as tmpdir:
        header = ["col1", "col2"]
        data = [
            {"col1": 1, "col2": 2},
            {"col1": 4, "col2": "test"},
            {"col2": 4, "col3": 5},
        ]
        filepath = Path(tmpdir) / "test_file.csv"

        with open(filepath, "w") as f:
            CSVWriter.write_rows(file=f, header=header, rows=data)

        with open(filepath, "r") as f:
            content = f.read()

        expected = """\
\"col1\",\"col2\"
1,2
4,"test"
\"\",4
"""
        assert expected == content


def test_csv_to_file_writer() -> None:
    with TemporaryDirectory() as tmpdir:
        data = [
            {"col1": 1},
            {"col1": 1, "col2": 2},
            {"col1": 4, "col2": "test"},
            {"col2": 4, "col3": 5},
        ]
        filepath = Path(tmpdir) / "test_file.csv"

        writer = CSVWriter.new(
            path=filepath,
            header=["col1", "col2"],
        )
        writer(data)

        with open(filepath, "r") as f:
            content = f.read()

        expected = """\
\"col1\",\"col2\"
1,\"\"
1,2
4,"test"
\"\",4
"""
        assert expected == content


def test_csv_to_stdout_writer() -> None:
    with patch("sys.stdout", new=StringIO()) as fake_out:
        data = [
            {"col1": 1, "col2": 2},
            {"col1": 4, "col2": "test"},
            {"col2": 4, "col3": 5},
        ]

        writer = CSVWriter.new(header=["col1", "col2"])
        writer(data)

        expected = """\
\"col1\",\"col2\"\r
1,2\r
4,"test"\r
\"\",4\r
"""
        assert expected == fake_out.getvalue()


class TestExcelWriter:
    def test_write(self) -> None:
        pass

    @pytest.mark.parametrize(
        ("shape", "expected"),
        (
            ((0, 5), "A1:E1"),
            ((4, 26), "A1:Z5"),
            ((4, 27), "A1:AA5"),
            ((4, 28), "A1:AB5"),
            ((4, 703), "A1:AAA5"),
            ((4, 3487), "A1:EDC5"),
            ((4, 16377), "A1:XEW5"),
        ),
    )
    def test_get_table_range(
        self,
        shape: tuple[int, int],
        expected: str,
    ) -> None:
        mockdf = Mock()
        mockdf.shape = shape
        assert ExcelWriter.get_table_range(mockdf) == expected

    def test_write_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            df_1 = pd.DataFrame(
                {
                    "a": [1, 2],
                    "b": ["x", "y"],
                }
            )
            df_2 = pd.DataFrame(
                {
                    "a": [1, 2],
                    "b": ["x", "y"],
                }
            )

            output = [
                Mock(
                    key="sheet_1",
                    data=df_1,
                ),
                Mock(
                    key="sheet_2",
                    data=df_2,
                ),
            ]

            test_file = Path(tmpdir) / "test_file.xlsx"
            writer = ExcelWriter.new(test_file)
            writer(output)

            for sheet, df in zip(("sheet_1", "sheet_2"), (df_1, df_2)):
                test = pd.read_excel(test_file, sheet_name=sheet)
                pd.testing.assert_frame_equal(test, df)
