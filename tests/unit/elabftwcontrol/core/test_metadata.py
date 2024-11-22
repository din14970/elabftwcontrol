import math
from datetime import date, datetime, time
from typing import Any, Callable, Optional

import pytest

from elabftwcontrol.core.metadata import (
    MetadataField,
    MetadataParser,
    ParsedMetadataToMetadataFieldList,
    ParsedMetadataToOrderedFieldnames,
    ParsedMetadataToPandasDtype,
    ParsedMetadataToSimpleDict,
)


class TestMetadataField:
    @pytest.mark.parametrize(
        ("data", "expected"),
        (
            (
                {"type": "test_type", "position": 3, "value": 5, "unit": "kg"},
                MetadataField(
                    name="test",
                    type="test_type",
                    group="test_group",
                    position=3,
                    value="5",
                    unit="kg",
                ),
            ),
            (
                {},
                MetadataField(
                    name="test",
                    type="",
                    group="test_group",
                    position=-1,
                    value="",
                    unit="",
                ),
            ),
            (
                {"type": 5, "position": "x", "value": 5, "unit": "kg"},
                MetadataField(
                    name="test",
                    type="5",
                    group="test_group",
                    position=-1,
                    value="5",
                    unit="kg",
                ),
            ),
        ),
    )
    def test_from_raw_field_data(
        self, data: dict[str, Any], expected: MetadataField
    ) -> None:
        assert (
            MetadataField.from_raw_data(
                data=data,
                name="test",
                group="test_group",
            )
            == expected
        )

    @pytest.mark.parametrize(
        ("value", "field_type", "expected"),
        (
            ("1234", "number", 1234.0),
            ("1234-01-23", "date", datetime(1234, 1, 23)),
            ("1234-01-23T12:34", "datetime-local", datetime(1234, 1, 23, 12, 34)),
            ("12:34", "time", datetime(1900, 1, 1, 12, 34)),
            ("12 - x - x", "items", 12),
            ("12 - x - x", "experiments", 12),
            ("x", "date", None),
            ("x", "datetime-local", None),
            ("x", "time", None),
            ("x", "items", None),
            ("x", "experiments", None),
            ("x", "anything_else", "x"),
            ("1234", "anything_else", "1234"),
        ),
    )
    def test_get_field_value(self, value: str, field_type: str, expected: Any) -> None:
        field = MetadataField(
            name="test",
            type=field_type,
            group="test_group",
            position=0,
            value=value,
            unit="kg",
        )
        assert field.parsed_value == expected

    @pytest.mark.parametrize(
        ("value", "expected", "test"),
        (
            ("1234", 1234.0, lambda result, expected: abs(result - expected) < 1e-6),
            ("x", float("nan"), lambda result, expected: math.isnan(result)),
        ),
    )
    def test_parse_number(
        self,
        value: str,
        expected: float,
        test: Callable[[float, float], bool],
    ) -> None:
        assert test(MetadataField.parse_number(value), expected)

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("2024-05-20", datetime(2024, 5, 20)),
            ("x", None),
        ),
    )
    def test_parse_date(self, value: str, expected: Optional[date]) -> None:
        assert MetadataField.parse_date(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("2024-05-20T19:05", datetime(2024, 5, 20, 19, 5)),
            ("x", None),
        ),
    )
    def test_parse_datetime(self, value: str, expected: Optional[datetime]) -> None:
        assert MetadataField.parse_datetime(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("19:05", datetime(1900, 1, 1, 19, 5)),
            ("x", None),
        ),
    )
    def test_parse_time(self, value: str, expected: Optional[time]) -> None:
        assert MetadataField.parse_time(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("19 - bla - bla", 19),
            ("x", None),
        ),
    )
    def test_parse_item_link(self, value: str, expected: Optional[int]) -> None:
        assert MetadataField.parse_item_link(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("19 - bla - bla", 19),
            ("x", None),
        ),
    )
    def test_parse_experiment_link(self, value: str, expected: Optional[int]) -> None:
        assert MetadataField.parse_experiment_link(value) == expected

    @pytest.mark.parametrize(
        ("value", "unit", "expected"),
        (
            ("1234", "kg", "kg"),
            (None, "kg", ""),
            ("1234", None, ""),
            (None, None, ""),
        ),
    )
    def test_get_field_unit(
        self,
        value: Any,
        unit: Optional[str],
        expected: str,
    ) -> None:
        field = MetadataField(
            name="test",
            type="anything",
            group="test_group",
            position=0,
            value=value,
            unit=unit,
        )
        assert field.corrected_unit == expected

    @pytest.mark.parametrize(
        ("value", "unit", "expected"),
        (
            ("1234", "kg", "1234 kg"),
            (None, "kg", ""),
            ("1234", None, "1234"),
            (None, None, ""),
        ),
    )
    def test_get_field_combined(
        self,
        value: Any,
        unit: Optional[str],
        expected: str,
    ) -> None:
        field = MetadataField(
            name="test",
            type="anything",
            group="test_group",
            position=0,
            value=value,
            unit=unit,
        )
        assert field.value_and_unit == expected


class TestMetadataParser:
    def test_parse_empty(self) -> None:
        parser = MetadataParser()
        result = parser(metadata="{}")
        assert result == {}

    def test_parse_bad_json(self) -> None:
        parser = MetadataParser()
        result = parser(metadata="wrong json")
        assert result == {}

    def test_parse_ok(self) -> None:
        parser = MetadataParser()
        result = parser(metadata='{"a": "b"}')
        assert result == {"a": "b"}


METADATA_WITH_GROUPS = """\
{
  "elabftw": {
    "extra_fields_groups": [
      {
        "id": 1,
        "name": "group 1"
      },
      {
        "id": 2,
        "name": "group 2"
      }
    ]
  },
  "extra_fields": {
    "field 1": {
      "type": "number",
      "unit": "h",
      "units": [
        "h"
      ],
      "group_id": "2",
      "position": "8",
      "description": "something",
      "non existing": "something"
    },
    "field 2": {
      "type": "date",
      "value": "x",
      "group_id": "1",
      "position": "x"
    },
    "field 3": {
      "type": "something",
      "value": "anything",
      "group_id": "x",
      "position": "10"
    }
  }
}
"""

METADATA_WITHOUT_GROUPS = """\
{
  "extra_fields": {
    "field 1": {
      "type": "number",
      "unit": "h",
      "units": [
        "h"
      ],
      "group_id": "2",
      "position": "8",
      "description": "something",
      "non existing": "something"
    },
    "field 2": {
      "type": "date",
      "value": "x",
      "group_id": "1",
      "position": "x"
    },
    "field 3": {
      "type": "something",
      "value": "anything",
      "group_id": "x",
      "position": "10"
    }
  }
}
"""


class TestParsedMetadataToMetadataFieldList:
    def test_full_metadata_with_groups(self) -> None:
        parsed_metadata = MetadataParser()(METADATA_WITH_GROUPS)
        fields = ParsedMetadataToMetadataFieldList()(parsed_metadata)
        expected: list[MetadataField] = [
            MetadataField(
                name="field 1",
                type="number",
                group="group 2",
                position=8,
                description="something",
                value="",
                unit="h",
            ),
            MetadataField(
                name="field 2",
                type="date",
                group="group 1",
                position=-1,
                value="x",
                unit="",
            ),
            MetadataField(
                name="field 3",
                type="something",
                group="",
                position=10,
                value="anything",
                unit="",
            ),
        ]
        assert len(fields) == len(expected)
        assert set(fields) == set(expected)

    def test_full_metadata_no_groups(self) -> None:
        parsed_metadata = MetadataParser()(METADATA_WITHOUT_GROUPS)
        fields = ParsedMetadataToMetadataFieldList()(parsed_metadata)
        expected: list[MetadataField] = [
            MetadataField(
                name="field 1",
                type="number",
                group="",
                description="something",
                position=8,
                value="",
                unit="h",
            ),
            MetadataField(
                name="field 2",
                type="date",
                group="",
                position=-1,
                value="x",
                unit="",
            ),
            MetadataField(
                name="field 3",
                type="something",
                group="",
                position=10,
                value="anything",
                unit="",
            ),
        ]
        assert len(fields) == len(expected)
        assert set(fields) == set(expected)


class TestParsedMetadataToOrderedFields:
    def test_get_extra_field_names_in_order(self) -> None:
        metadata = """\
{
  "extra_fields": {
    "field 3": {
      "position": "10"
    },
    "field 1": {
      "position": "8"
    },
    "field 2": {
      "position": "x"
    }
  }
}
"""
        parsed_metadata = MetadataParser()(metadata)
        fields = ParsedMetadataToOrderedFieldnames()(parsed_metadata)
        expected = ["field 2", "field 1", "field 3"]
        assert fields == expected


class TestParsedMetadataToSimpleDict:
    metadata = {
        "extra_fields": {
            "field 3": {
                "position": "10",
                "value": "20",
                "type": "number",
                "unit": "kg",
            },
            "field 1": {
                "position": "8",
                "type": "number",
                "unit": "kg",
            },
            "field 2": {
                "position": "x",
                "value": "20",
                "type": "number",
            },
            "field 6": {
                "position": "13",
                "value": "20",
                "type": "something",
                "unit": "kg",
            },
            "field 5": {
                "position": "12",
                "value": "1999-09-09T19:19",
                "type": "datetime-local",
                "unit": "kg",
            },
            "field 4": {
                "position": "11",
                "value": "notanumber",
                "type": "number",
                "unit": "kg",
            },
            "field 7": {
                "position": "99",
                "value": "blabla",
                "type": "text",
            },
        },
    }

    @pytest.mark.parametrize(
        ("cell_content", "ordered", "expected"),
        (
            (
                "value",
                True,
                {
                    "field 2": 20,
                    "field 1": float("nan"),
                    "field 3": 20,
                    "field 4": float("nan"),
                    "field 5": datetime(1999, 9, 9, 19, 19),
                    "field 6": "20",
                    "field 7": "blabla",
                },
            ),
            (
                "unit",
                False,
                {
                    "field 3": "kg",
                    "field 1": "",
                    "field 2": "",
                    "field 6": "kg",
                    "field 5": "kg",
                    "field 4": "kg",
                    "field 7": "",
                },
            ),
            (
                "combined",
                False,
                {
                    "field 3": "20 kg",
                    "field 1": "",
                    "field 2": "20",
                    "field 6": "20 kg",
                    "field 5": "1999-09-09T19:19 kg",
                    "field 4": "notanumber kg",
                    "field 7": "blabla",
                },
            ),
        ),
    )
    def test_get_simple_dict(
        self,
        cell_content: str,
        ordered: bool,
        expected: dict[str, Any],
    ) -> None:
        transformer = ParsedMetadataToSimpleDict.new(
            cell_content=cell_content,
            order_fields=ordered,
        )
        transformed = transformer(self.metadata)
        for (field_r, value_r), (field_e, value_e) in zip(
            transformed.items(), expected.items()
        ):
            assert field_r == field_e
            if isinstance(value_r, float) and math.isnan(value_r):
                assert math.isnan(value_e)
            else:
                assert value_r == value_e


class TestParsedMetadataToPandasDtype:
    metadata = {
        "extra_fields": {
            "field 1": {
                "type": "number",
            },
            "field 2": {
                "type": "text",
            },
            "field 3": {
                "type": "time",
            },
            "field 4": {
                "type": "experiments",
            },
        },
    }

    @pytest.mark.parametrize(
        ("cell_content", "expected"),
        (
            (
                "value",
                {
                    "field 1": "Float64",
                    "field 2": "string",
                    "field 3": "datetime64[ns]",
                    "field 4": "Int64",
                },
            ),
            (
                "unit",
                {
                    "field 1": "string",
                    "field 2": "string",
                    "field 3": "string",
                    "field 4": "string",
                },
            ),
            (
                "combined",
                {
                    "field 1": "string",
                    "field 2": "string",
                    "field 3": "datetime64[ns]",
                    "field 4": "Int64",
                },
            ),
        ),
    )
    def test_get_pandas_dtype(
        self,
        cell_content: str,
        expected: dict[str, Any],
    ) -> None:
        transformer = ParsedMetadataToPandasDtype.new(
            cell_content=cell_content,
        )
        transformed = transformer(self.metadata)
        assert transformed == expected
