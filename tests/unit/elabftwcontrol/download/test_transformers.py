from typing import Any, Callable, Sequence, TypeVar

import pandas as pd
import pytest

from elabftwcontrol.download.metadata import MetadataField
from elabftwcontrol.download.transformers import (
    CSVElabDataTableTransformer,
    CSVLongMetadataTableTransformer,
    ExcelTransformer,
    JSONTransformer,
    LazyWideTableUtils,
    WideObjectTableData,
)

T = TypeVar("T")


class MockDictable:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data

    def to_dict(self) -> dict[str, Any]:
        return self.data


def test_obj_json_transform() -> None:
    data = {"a": 1, "b": {"c": [1, 2, 3], "d": "test"}}
    obj = MockDictable(data)
    result = JSONTransformer.transform(obj, indent=None)
    expected = '{"a": 1, "b": {"c": [1, 2, 3], "d": "test"}}'
    assert result == expected


def test_json_transformer() -> None:
    n_obj = 4

    def create_data(a: int) -> dict[str, Any]:
        return {"a": a, "b": {"c": [1, 2, 3], "d": "test"}}

    objs = [MockDictable(create_data(a)) for a in range(n_obj)]

    transformer = JSONTransformer(indent=None)
    transformed = transformer(objs)

    for i, result in enumerate(transformed):
        expected = '{"a": ' + f"{i}" + ', "b": {"c": [1, 2, 3], "d": "test"}}'
        assert expected == result


@pytest.fixture
def dummy_fields() -> list[MetadataField]:
    return [
        MetadataField(
            name="field1",
        ),
        MetadataField(
            name="field2",
            type="sometype",
            group="somegroup",
            position=10,
            value="1234",
            unit="kg",
        ),
        MetadataField(
            name="field1",
            type="sometype2",
            group="somegroup2",
            position=5,
            value="blabla",
            unit="",
        ),
    ]


class MockIdAndMetadata:
    def __init__(self, fields: list[MetadataField]) -> None:
        self.id = 1
        self.metadata = fields


def test_csv_long_metadata_transformer(dummy_fields: list[MetadataField]) -> None:
    def get_fields(fields: Any) -> list[MetadataField]:
        return fields

    def do_nothing(x: T) -> T:
        return x

    obj = MockIdAndMetadata(dummy_fields)
    expected = [
        {
            "_id": 1,
            "field_name": "field1",
            "field_type": "",
            "field_group": "",
            "field_position": -1,
            "field_description": "",
            "field_value": "",
            "field_unit": "",
        },
        {
            "_id": 1,
            "field_name": "field2",
            "field_type": "sometype",
            "field_group": "somegroup",
            "field_position": 10,
            "field_description": "",
            "field_value": "1234",
            "field_unit": "kg",
        },
        {
            "_id": 1,
            "field_name": "field1",
            "field_type": "sometype2",
            "field_group": "somegroup2",
            "field_position": 5,
            "field_description": "",
            "field_value": "blabla",
            "field_unit": "",
        },
    ]

    transformer = CSVLongMetadataTableTransformer()
    result = transformer.transform(
        obj,
        metadata_parser=get_fields,
        field_sanitizer=do_nothing,
    )
    for e, r in zip(expected, result):
        assert e == r


class MockGenericObject:
    def __init__(self, a: int, b: str, c: bool) -> None:
        self.a = a
        self.b = b
        self.c = c


def test_csv_elab_data_transformer() -> None:
    def column_name_sanitizer(col: str) -> str:
        if col == "a":
            return "z"
        else:
            return col

    obj = MockGenericObject(a=1, b="2", c=True)
    columns = ["a", "b", "d"]

    result = CSVElabDataTableTransformer.transform(
        obj,
        columns=columns,
        column_name_sanitizer=column_name_sanitizer,
    )
    expected = {"z": 1, "b": "2"}

    assert expected == result


class MockGenericObjectIdAndMetadata:
    def __init__(
        self,
        a: int,
        b: str,
        c: bool,
        fields: list[MetadataField],
    ) -> None:
        self.id = 1
        self.a = a
        self.b = b
        self.c = c
        self.metadata = fields
        self.fields = fields


def dummy_field_to_cell_value(field: MetadataField) -> str:
    return str(field.value) + "_test"


def dummy_column_name_sanitizer(col: str) -> str:
    if col == "a":
        return "z"
    elif col == "f1":
        return "zz1"
    else:
        return col


class TestWideTableUtils:
    obj_columns = ["a", "b", "d"]
    meta_columns = ["f1", "f2", "f4"]

    @pytest.mark.parametrize(
        ("sanitizer", "expected"),
        (
            (dummy_column_name_sanitizer, ["_a", "_b", "_d", "zz1", "f2", "f4"]),
            (None, ["_a", "_b", "_d", "f1", "f2", "f4"]),
        ),
    )
    def test_create_header(
        self,
        sanitizer: Callable[[str], str],
        expected: Sequence[str],
    ) -> None:
        result = LazyWideTableUtils.create_header(
            object_columns=self.obj_columns,
            metadata_columns=self.meta_columns,
            column_name_sanitizer=sanitizer,
        )
        assert result == expected

    @pytest.mark.parametrize(
        ("cell_content", "field", "expected"),
        (
            ("unit", MetadataField(value=1, unit="kg"), "kg"),
            ("value", MetadataField(value=1, unit="kg"), 1),
            ("value", MetadataField(value='\n"a', unit="kg"), "\\na"),
            ("combined", MetadataField(value=1, unit="kg"), "1 kg"),
        ),
    )
    def test_get_field_to_cell_value_converter(
        self,
        cell_content: str,
        field: MetadataField,
        expected: str,
    ) -> None:
        converter = LazyWideTableUtils.get_field_to_cell_value_converter(cell_content)
        assert converter(field) == expected

    @pytest.mark.parametrize(
        ("sanitizer", "expected"),
        (
            (
                dummy_column_name_sanitizer,
                {
                    "_a": 23,
                    "_b": "abc",
                    "_d": None,
                    "zz1": "_test",
                    "f2": "1234_test",
                    "f4": None,
                },
            ),
            (
                None,
                {
                    "_a": 23,
                    "_b": "abc",
                    "_d": None,
                    "f1": "_test",
                    "f2": "1234_test",
                    "f4": None,
                },
            ),
        ),
    )
    def test_transform_object_to_row(
        self,
        sanitizer: Callable[[str], str],
        expected: Sequence[str],
    ) -> None:
        fields = [
            MetadataField(
                name="f1",
            ),
            MetadataField(
                name="f2",
                type="sometype",
                group="somegroup",
                position=10,
                value="1234",
                unit="kg",
            ),
            MetadataField(
                name="f3",
                type="sometype2",
                group="somegroup2",
                position=5,
                value="blabla",
                unit="",
            ),
        ]
        obj = MockGenericObjectIdAndMetadata(
            a=23,
            b="abc",
            c=True,
            fields=fields,
        )

        def get_fields(metadata: Any) -> list[MetadataField]:
            return metadata

        result = LazyWideTableUtils.transform_object_to_row_wide(
            obj=obj,
            object_columns=self.obj_columns,
            metadata_columns=self.meta_columns,
            field_to_cell_value=dummy_field_to_cell_value,
            column_name_sanitizer=sanitizer,
            metadata_parser=get_fields,
        )
        assert result == expected


class TestExcelTransformer:
    obj_columns = ["a", "b", "d"]
    meta_columns = ["f1", "f2", "f4"]

    @staticmethod
    def get_fields(obj: Any) -> list[MetadataField]:
        return obj

    transformer = ExcelTransformer(
        field_to_cell_value=dummy_field_to_cell_value,
        column_name_sanitizer=dummy_column_name_sanitizer,
        metadata_parser=get_fields,
    )

    def test_transform_sheet_to_dataframe(self) -> None:
        fields_1 = [
            MetadataField(
                name="f1",
            ),
            MetadataField(
                name="f2",
                type="sometype",
                group="somegroup",
                position=10,
                value="1234",
                unit="kg",
            ),
            MetadataField(
                name="f3",
                type="sometype2",
                group="somegroup2",
                position=5,
                value="blabla",
                unit=None,
            ),
        ]
        obj_1 = MockGenericObjectIdAndMetadata(
            a=23,
            b="abc",
            c=True,
            fields=fields_1,
        )
        fields_2 = [
            MetadataField(
                name="f1",
                value="hello",
            ),
            MetadataField(
                name="f2",
                type="sometype",
                group="somegroup",
                position=10,
                value="9999",
                unit="kg",
            ),
            MetadataField(
                name="f4",
                type="sometype2",
                group="somegroup2",
                position=5,
                value="blabla2",
                unit=None,
            ),
        ]
        obj_2 = MockGenericObjectIdAndMetadata(
            a=55,
            b="dfg",
            c=False,
            fields=fields_2,
        )

        sheet = WideObjectTableData(
            table_name="sheet1",
            object_columns=self.obj_columns,
            metadata_columns=self.meta_columns,
            objects=[obj_1, obj_2],
        )

        result = self.transformer.transform_sheet_to_dataframe(sheet)
        expected = pd.DataFrame(
            {
                "_a": [23, 55],
                "_b": ["abc", "dfg"],
                "_d": [None, None],
                "zz1": ["_test", "hello_test"],
                "f2": ["1234_test", "9999_test"],
                "f4": [None, "blabla2_test"],
            }
        )

        assert result.key == "sheet1"
        pd.testing.assert_frame_equal(expected, result.data)
