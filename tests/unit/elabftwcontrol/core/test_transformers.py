from contextlib import nullcontext
from typing import Any, Callable, ContextManager, Iterable, Iterator, Sequence, TypeVar

import pandas as pd
import pytest

from elabftwcontrol.core.interfaces import HasIDAndMetadata
from elabftwcontrol.core.metadata import MetadataField
from elabftwcontrol.core.transformers import (
    CSVElabDataTableTransformer,
    CSVLongMetadataTableTransformer,
    JSONTransformer,
    LazyWideTableUtils,
    MultiPandasDataFrameTransformer,
    ObjectTypes,
    SplitDataFrame,
)

T = TypeVar("T")


class MockDictable:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data

    def to_dict(self) -> dict[str, Any]:
        return self.data


class TestJSONTransformer:
    def test_obj_json_transform(self) -> None:
        data = {"a": 1, "b": {"c": [1, 2, 3], "d": "test"}}
        objs = [MockDictable(data)]
        result = list(JSONTransformer.new(indent=None)(objs))
        expected = ['{"a": 1, "b": {"c": [1, 2, 3], "d": "test"}}']
        assert result == expected

    def test_json_transformer(self) -> None:
        n_obj = 4

        def create_data(a: int) -> dict[str, Any]:
            return {"a": a, "b": {"c": [1, 2, 3], "d": "test"}}

        objs = [MockDictable(create_data(a)) for a in range(n_obj)]

        transformer = JSONTransformer.new(indent=None)
        transformed = transformer(objs)

        for i, result in enumerate(transformed):
            expected = '{"a": ' + f"{i}" + ', "b": {"c": [1, 2, 3], "d": "test"}}'
            assert expected == result


class MockIdAndMetadata(HasIDAndMetadata):
    def __init__(self, metadata: str) -> None:
        self.id = 1
        self.metadata = metadata


class TestMultiPandasDataFrameTransformer:
    def verify_result(
        self,
        result: Iterable[SplitDataFrame],
        expected: Iterable[SplitDataFrame],
    ) -> None:
        for r, e in zip(result, expected):
            assert r.key == e.key
            pd.testing.assert_frame_equal(r.data, e.data)

    def test_transformer(self) -> None:
        def splitter(raw_data: pd.DataFrame) -> Iterator[SplitDataFrame]:
            yield SplitDataFrame(
                key="key1",
                data=pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}),
            )
            yield SplitDataFrame(
                key="key2",
                data=pd.DataFrame({"a": [4, 5, 6], "b": ["a", "b", "c"]}),
            )

        def df_transform_getter(key: str) -> Callable[[pd.DataFrame], pd.DataFrame]:
            if key == "key1":

                def transformer(data: pd.DataFrame) -> pd.DataFrame:
                    return data[data["a"] < 3].reset_index(drop=True)

            elif key == "key2":

                def transformer(data: pd.DataFrame) -> pd.DataFrame:
                    return data[data["b"] > "a"].reset_index(drop=True)

            else:
                raise ValueError

            return transformer

        transformer = MultiPandasDataFrameTransformer(
            splitter=splitter,
            df_transform_getter=df_transform_getter,
        )
        result = transformer(pd.DataFrame())
        expected = (
            SplitDataFrame(
                key="key1",
                data=pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}),
            ),
            SplitDataFrame(
                key="key2",
                data=pd.DataFrame({"a": [5, 6], "b": ["b", "c"]}),
            ),
        )
        self.verify_result(result, expected)

    @pytest.mark.parametrize(
        ["input", "obj_type", "categories", "expected", "context"],
        (
            (
                pd.DataFrame(
                    {
                        "category_title": ["a", "b", "a", "a"],
                        "other_column": [1, 2, 3, 4],
                    }
                ),
                ObjectTypes.ITEM,
                None,
                (
                    SplitDataFrame(
                        key="a",
                        data=pd.DataFrame(
                            {
                                "category_title": ["a", "a", "a"],
                                "other_column": [1, 3, 4],
                            }
                        ),
                    ),
                    SplitDataFrame(
                        key="b",
                        data=pd.DataFrame(
                            {
                                "category_title": ["b"],
                                "other_column": [2],
                            }
                        ),
                    ),
                ),
                nullcontext(),
            ),
            (
                pd.DataFrame(
                    {
                        "title": ["a 1", "b 2", "a 3", "a 4"],
                        "other_column": [1, 2, 3, 4],
                    }
                ),
                ObjectTypes.EXPERIMENT,
                ("a", "b"),
                (
                    SplitDataFrame(
                        key="a",
                        data=pd.DataFrame(
                            {
                                "title": ["a 1", "a 3", "a 4"],
                                "other_column": [1, 3, 4],
                            }
                        ),
                    ),
                    SplitDataFrame(
                        key="b",
                        data=pd.DataFrame(
                            {
                                "title": ["b 2"],
                                "other_column": [2],
                            }
                        ),
                    ),
                ),
                nullcontext(),
            ),
            (
                pd.DataFrame(
                    {
                        "title": ["a 1", "b 2", "a 3", "a 4"],
                        "other_column": [1, 2, 3, 4],
                    }
                ),
                ObjectTypes.EXPERIMENT,
                None,
                None,
                pytest.raises(ValueError),
            ),
            (
                pd.DataFrame(
                    {
                        "title": ["a 1", "b 2", "a 3", "a 4"],
                        "other_column": [1, 2, 3, 4],
                    }
                ),
                ObjectTypes.EXPERIMENTS_TEMPLATE,
                None,
                (
                    SplitDataFrame(
                        key="experiments_template",
                        data=pd.DataFrame(
                            {
                                "title": ["a 1", "b 2", "a 3", "a 4"],
                                "other_column": [1, 2, 3, 4],
                            }
                        ),
                    ),
                ),
                nullcontext(),
            ),
        ),
    )
    def test_for_raw_tables(
        self,
        input: pd.DataFrame,
        obj_type: ObjectTypes,
        categories: Sequence[str] | None,
        expected: Iterable[SplitDataFrame],
        context: ContextManager,
    ) -> None:
        with context:
            transformer = MultiPandasDataFrameTransformer.for_raw_tables(
                object_type=obj_type,
                categories=categories,
            )
            result = transformer(input)
            self.verify_result(result, expected)


class TestCSVLongMetadataTableTransformer:
    def test_csv_long_metadata_transformer(self) -> None:
        metadata = """\
{
    "elabftw": {
        "extra_fields_groups": [
            {"id": 2, "name": "somegroup"}
        ]
    },
    "extra_fields": {
        "field1": {},
        "field2": {
            "group_id": "1",
            "position": 10,
            "value": "blabla",
            "unit": "kg"
        },
        "field3": {
            "type": "number",
            "group_id": "2",
            "position": 5,
            "value": "1234",
            "unit": ""
        }
    }
}
"""
        obj = MockIdAndMetadata(metadata)
        expected = [
            {
                "_id": 1,
                "field_name": "field1",
                "field_type": "text",
                "field_group": "",
                "field_position": -1,
                "field_description": "",
                "field_value": "",
                "field_unit": "",
            },
            {
                "_id": 1,
                "field_name": "field2",
                "field_type": "text",
                "field_group": "",
                "field_position": 10,
                "field_description": "",
                "field_value": "blabla",
                "field_unit": "kg",
            },
            {
                "_id": 1,
                "field_name": "field3",
                "field_type": "number",
                "field_group": "somegroup",
                "field_position": 5,
                "field_description": "",
                "field_value": "1234",
                "field_unit": "",
            },
        ]

        transformer = CSVLongMetadataTableTransformer.new()
        result = transformer([obj, obj])
        for e, r in zip(result, expected * 2):
            assert e == r


class MockGenericObject:
    def __init__(self, a: int, b: str, c: bool) -> None:
        self.a = a
        self.b = b
        self.c = c


class TestCSVElabDAtaTableTransformer:
    def test_csv_elab_data_transformer(self) -> None:
        def column_name_sanitizer(col: str) -> str:
            if col == "a":
                return "z"
            else:
                return col

        obj = MockGenericObject(a=1, b="2", c=True)
        columns = ["a", "b", "d"]

        transformer = CSVElabDataTableTransformer(
            columns=columns,
            column_name_sanitizer=column_name_sanitizer,
        )
        result = transformer.transform(obj)
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
