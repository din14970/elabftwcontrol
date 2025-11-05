import math
from datetime import date, datetime, time
from typing import Any, Callable, Optional

import pytest

from elabftwcontrol.core.models import (
    ConfigMetadata,
    FieldTypeEnum,
    GroupModel,
    MetadataModel,
    SingleFieldModel,
)
from elabftwcontrol.testing_utils import assert_dicts_equal


class TestSingleFieldModel:
    @pytest.mark.parametrize(
        ("input", "expected"),
        (
            ({}, SingleFieldModel()),
            (
                {
                    "type": "number",
                    "value": "2.3",
                    "unit": "kg",
                    "group_id": "1",
                    "position": "3",
                },
                SingleFieldModel(
                    type=FieldTypeEnum.number,
                    value="2.3",
                    unit="kg",
                    group_id=1,
                    position=3,
                ),
            ),
            (
                {
                    "type": "number",
                    "value": 2.3,
                    "unit": "kg",
                    "group_id": "1",
                    "position": "3",
                },
                SingleFieldModel(
                    type=FieldTypeEnum.number,
                    value=2.3,
                    unit="kg",
                    group_id=1,
                    position=3,
                ),
            ),
            (
                {
                    "value": ["select 1", "select 2"],
                    "type": "select",
                    "options": ["select 1", "select 2", "select 3"],
                    "group_id": "1",
                    "allow_multi_values": True,
                    "position": "3",
                    "blank_value_on_duplicate": True,
                    "readonly": True,
                },
                SingleFieldModel(
                    value=["select 1", "select 2"],
                    type=FieldTypeEnum.select,
                    options=["select 1", "select 2", "select 3"],
                    group_id=1,
                    allow_multi_values=True,
                    position=3,
                    blank_value_on_duplicate=True,
                    readonly=True,
                ),
            ),
        ),
    )
    def test_creation(self, input: dict[str, Any], expected: SingleFieldModel) -> None:
        assert SingleFieldModel(**input) == expected

    @pytest.mark.parametrize(
        ("value", "field_type", "expected"),
        (
            ("1234", FieldTypeEnum.number, 1234.0),
            ("1234-01-23", FieldTypeEnum.date, datetime(1234, 1, 23)),
            (
                "1234-01-23T12:34",
                FieldTypeEnum.datetime_local,
                datetime(1234, 1, 23, 12, 34),
            ),
            ("12:34", FieldTypeEnum.time, datetime(1900, 1, 1, 12, 34)),
            ("12", FieldTypeEnum.items, 12),
            ("12", FieldTypeEnum.experiments, 12),
            ("x", FieldTypeEnum.date, None),
            ("x", FieldTypeEnum.datetime_local, None),
            ("x", FieldTypeEnum.time, None),
            ("x", FieldTypeEnum.items, None),
            ("x", FieldTypeEnum.experiments, None),
            ("x", None, "x"),
            ("1234", None, "1234"),
        ),
    )
    def test_get_field_value(
        self,
        value: str,
        field_type: FieldTypeEnum,
        expected: Any,
    ) -> None:
        if field_type is None:
            field = SingleFieldModel(value=value)
        else:
            field = SingleFieldModel(value=value, type=field_type)
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
        assert test(SingleFieldModel.parse_number(value), expected)

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("2024-05-20", datetime(2024, 5, 20)),
            ("x", None),
        ),
    )
    def test_parse_date(self, value: str, expected: Optional[date]) -> None:
        assert SingleFieldModel.parse_date(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("2024-05-20T19:05", datetime(2024, 5, 20, 19, 5)),
            ("x", None),
        ),
    )
    def test_parse_datetime(self, value: str, expected: Optional[datetime]) -> None:
        assert SingleFieldModel.parse_datetime(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("19:05", datetime(1900, 1, 1, 19, 5)),
            ("x", None),
        ),
    )
    def test_parse_time(self, value: str, expected: Optional[time]) -> None:
        assert SingleFieldModel.parse_time(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("19", 19),
            ("x", None),
        ),
    )
    def test_parse_item_link(self, value: str, expected: Optional[int]) -> None:
        assert SingleFieldModel.parse_item_link(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            ("19", 19),
            ("x", None),
        ),
    )
    def test_parse_experiment_link(self, value: str, expected: Optional[int]) -> None:
        assert SingleFieldModel.parse_experiment_link(value) == expected

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
        field = SingleFieldModel(value=value, unit=unit)
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
        field = SingleFieldModel(value=value, unit=unit)
        assert field.value_and_unit == expected


class TestMetadataModel:
    def test_creation(self) -> None:
        dummy_extra_data: dict[str, Any] = {
            "elabftw": {
                "extra_fields_groups": [
                    {"id": 1, "name": "g1"},
                    {"id": 2, "name": "g2"},
                    {"id": 3, "name": "g3"},
                ],
            },
            "extra_fields": {
                "f1": {
                    "type": "number",
                    "value": "2.3",
                    "unit": "kg",
                    "group_id": "1",
                    "position": "3",
                },
                "k3": {
                    "type": "date",
                    "value": "2023-09-12",
                    "group_id": "1",
                    "position": "2",
                },
            },
        }
        expected = MetadataModel(
            elabftw=ConfigMetadata(
                extra_fields_groups=[
                    GroupModel(id=1, name="g1"),
                    GroupModel(id=2, name="g2"),
                    GroupModel(id=3, name="g3"),
                ]
            ),
            extra_fields={
                "f1": SingleFieldModel(
                    type=FieldTypeEnum.number,
                    value="2.3",
                    unit="kg",
                    group_id=1,
                    position=3,
                ),
                "k3": SingleFieldModel(
                    type=FieldTypeEnum.date,
                    value="2023-09-12",
                    group_id=1,
                    position=2,
                ),
            },
        )
        model = MetadataModel(**dummy_extra_data)
        assert expected == model

    def test_create_empty(self) -> None:
        expected = MetadataModel()
        model = MetadataModel(**{})
        assert expected == model

    @pytest.mark.parametrize(
        ("wrong_input",),
        (
            (
                {
                    "extra_fields": {
                        "f2": {
                            "type": "non_existing_type",
                        },
                    },
                },
            ),
            (
                {
                    "extra_fields": {
                        "f2": {
                            "group_id": "not_integer",
                        },
                    },
                },
            ),
            (
                {
                    "extra_fields": {
                        "f2": {
                            "position": "not_integer",
                        },
                    },
                },
            ),
        ),
    )
    def test_metadatamodel_failure(self, wrong_input: dict[str, Any]) -> None:
        with pytest.raises(Exception):
            _ = MetadataModel(**wrong_input)

    def test_ordered_fieldnames(self) -> None:
        wrong_field: dict[str, Any] = {}
        model = MetadataModel(
            extra_fields={
                "f1": SingleFieldModel(position=3),
                "f2": SingleFieldModel(position=1),
                "f3": SingleFieldModel(position=2),
                "f4": SingleFieldModel(position=None),
                "f5": SingleFieldModel(**wrong_field),
            },
        )
        expected = ["f4", "f5", "f2", "f3", "f1"]
        assert model.ordered_fieldnames == expected

    def test_fieldnames(self) -> None:
        wrong_field: dict[str, Any] = {}
        model = MetadataModel(
            extra_fields={
                "f1": SingleFieldModel(position=3),
                "f2": SingleFieldModel(position=1),
                "f3": SingleFieldModel(position=2),
                "f4": SingleFieldModel(position=None),
                "f5": SingleFieldModel(**wrong_field),
            },
        )
        expected = ["f1", "f2", "f3", "f4", "f5"]
        assert model.fieldnames == expected

    def test_field_values(self) -> None:
        wrong_field: dict[str, Any] = {
            "value": "123",
            "type": "number",
        }
        wrong_field_2: dict[str, Any] = {
            "value": "not a number",
            "type": "number",
            "position": "-1",
        }
        model = MetadataModel(
            extra_fields={
                "f1": SingleFieldModel(
                    value="1993-06-18",
                    type=FieldTypeEnum.date,
                    position=3,
                ),
                "f2": SingleFieldModel(value="1", position=1),
                "f3": SingleFieldModel(
                    value="234",
                    type=FieldTypeEnum.items,
                    position=2,
                ),
                "f4": SingleFieldModel(value="value4", position=None),
                "f5": SingleFieldModel(**wrong_field),
                "f6": SingleFieldModel(**wrong_field_2),
            },
        )
        expected = {
            "f4": "value4",
            "f5": 123,
            "f6": float("nan"),
            "f2": "1",
            "f3": 234,
            "f1": datetime(1993, 6, 18),
        }
        assert_dicts_equal(model.field_values, expected)

    def test_field_units(self) -> None:
        wrong_field: dict[str, Any] = {
            "value": "123",
            "unit": "unit_5",
            "type": "number",
        }
        wrong_field_2: dict[str, Any] = {
            "value": "not a number",
            "unit": None,
            "type": "number",
            "position": "-1",
        }
        model = MetadataModel(
            extra_fields={
                "f1": SingleFieldModel(
                    value="1993-06-18",
                    unit="unit_1",
                    type=FieldTypeEnum.date,
                    position=3,
                ),
                "f2": SingleFieldModel(
                    value="1",
                    position=1,
                ),
                "f3": SingleFieldModel(
                    value="234 - something - something",
                    type=FieldTypeEnum.items,
                    position=2,
                ),
                "f4": SingleFieldModel(
                    value="value4",
                    unit="unit_4",
                    position=None,
                ),
                "f5": SingleFieldModel(**wrong_field),
                "f6": SingleFieldModel(**wrong_field_2),
            },
        )
        expected = {
            "f4": "unit_4",
            "f5": "unit_5",
            "f6": "",
            "f2": "",
            "f3": "",
            "f1": "unit_1",
        }
        assert_dicts_equal(model.field_units, expected)

    def test_field_value_and_units(self) -> None:
        wrong_field: dict[str, Any] = {
            "value": "123",
            "unit": "unit_5",
            "type": "number",
        }
        wrong_field_2: dict[str, Any] = {
            "value": "not a number",
            "unit": None,
            "type": "number",
            "position": "-1",
        }
        model = MetadataModel(
            extra_fields={
                "f1": SingleFieldModel(
                    value="1993-06-18",
                    unit="unit_1",
                    type=FieldTypeEnum.date,
                    position=3,
                ),
                "f2": SingleFieldModel(
                    value="1",
                    position=1,
                ),
                "f3": SingleFieldModel(
                    value="234 - something - something",
                    type=FieldTypeEnum.items,
                    position=2,
                ),
                "f4": SingleFieldModel(
                    value="value4",
                    unit="unit_4",
                    position=None,
                ),
                "f5": SingleFieldModel(**wrong_field),
                "f6": SingleFieldModel(**wrong_field_2),
            },
        )
        expected = {
            "f4": "value4 unit_4",
            "f5": "123 unit_5",
            "f6": "not a number",
            "f2": "1",
            "f3": "234 - something - something",
            "f1": "1993-06-18 unit_1",
        }
        assert model.field_values_and_units == expected

    def test_get_extra_field_names_in_order(self) -> None:
        metadata: dict[str, Any] = {
            "extra_fields": {
                "field 3": {"position": "10"},
                "field 1": {"position": "8"},
                "field 2": {},
            }
        }
        parsed_metadata = MetadataModel(**metadata)
        expected = ["field 2", "field 1", "field 3"]
        assert parsed_metadata.ordered_fieldnames == expected
