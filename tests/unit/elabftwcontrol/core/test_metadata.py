from datetime import datetime
from typing import Any

import pytest

from elabftwcontrol.core.metadata import (
    MetadataField,
    ParsedMetadataToMetadataFieldList,
    ParsedMetadataToPandasDtype,
    ParsedMetadataToSimpleDict,
    TableCellContentType,
)
from elabftwcontrol.core.models import (
    ConfigMetadata,
    FieldTypeEnum,
    GroupModel,
    MetadataModel,
    SingleFieldModel,
)
from elabftwcontrol.testing_utils import assert_dicts_equal


class TestMetadataField:
    @pytest.mark.parametrize(
        ("data", "expected"),
        (
            (
                SingleFieldModel(
                    type=FieldTypeEnum.text,
                    position=3,
                    value="5",
                    unit="kg",
                ),
                MetadataField(
                    name="test",
                    type="text",
                    group="test_group",
                    position=3,
                    value="5",
                    unit="kg",
                ),
            ),
            (
                SingleFieldModel(),
                MetadataField(
                    name="test",
                    type="text",
                    group="test_group",
                    position=-1,
                    value="",
                    unit="",
                ),
            ),
            (
                SingleFieldModel(
                    type=FieldTypeEnum.number,
                    position=None,
                    value=None,
                ),
                MetadataField(
                    name="test",
                    type="number",
                    group="test_group",
                    position=-1,
                    value="",
                ),
            ),
        ),
    )
    def test_from_parsed_field(
        self,
        data: SingleFieldModel,
        expected: MetadataField,
    ) -> None:
        assert (
            MetadataField.from_parsed_field(
                data=data,
                name="test",
                group="test_group",
            )
            == expected
        )


class TestParsedMetadataToMetadataFieldList:
    def test_full_metadata_with_groups(self) -> None:
        parsed_metadata = MetadataModel(
            elabftw=ConfigMetadata(
                extra_fields_groups=[
                    GroupModel(id=1, name="group 1"),
                    GroupModel(id=2, name="group 2"),
                ]
            ),
            extra_fields={
                "field 1": SingleFieldModel(
                    type=FieldTypeEnum("number"),
                    unit="h",
                    units=["h"],
                    group_id=2,
                    position=8,
                    description="something",
                ),
                "field 2": SingleFieldModel(
                    type=FieldTypeEnum("date"),
                    value="x",
                    group_id=1,
                    position=None,
                ),
                "field 3": SingleFieldModel(
                    value="anything",
                    group_id=None,
                    position=10,
                ),
            },
        )
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
            ),
            MetadataField(
                name="field 3",
                type="text",
                position=10,
                value="anything",
            ),
        ]
        assert len(fields) == len(expected)
        assert set(fields) == set(expected)

    def test_full_metadata_no_groups(self) -> None:
        parsed_metadata = MetadataModel(
            extra_fields={
                "field 1": SingleFieldModel(
                    type=FieldTypeEnum("number"),
                    unit="h",
                    units=["h"],
                    group_id=2,
                    position=8,
                    description="something",
                ),
                "field 2": SingleFieldModel(
                    type=FieldTypeEnum("date"),
                    value="x",
                    group_id=1,
                    position=None,
                ),
                "field 3": SingleFieldModel(
                    value="anything",
                    group_id=None,
                    position=10,
                ),
            }
        )
        fields = ParsedMetadataToMetadataFieldList()(parsed_metadata)
        expected: list[MetadataField] = [
            MetadataField(
                name="field 1",
                type="number",
                description="something",
                position=8,
                unit="h",
            ),
            MetadataField(
                name="field 2",
                type="date",
                position=-1,
                value="x",
            ),
            MetadataField(
                name="field 3",
                type="text",
                position=10,
                value="anything",
            ),
        ]
        assert len(fields) == len(expected)
        assert set(fields) == set(expected)


class TestParsedMetadataToSimpleDict:
    metadata = MetadataModel(
        extra_fields={
            "field 3": SingleFieldModel(
                position=10,
                value="20",
                type=FieldTypeEnum("number"),
                unit="kg",
            ),
            "field 1": SingleFieldModel(
                position=8,
                type=FieldTypeEnum("number"),
                unit="kg",
            ),
            "field 2": SingleFieldModel(
                value="20",
                type=FieldTypeEnum("number"),
            ),
            "field 6": SingleFieldModel(
                position=13,
                value="20",
                unit="kg",
            ),
            "field 5": SingleFieldModel(
                position=12,
                value="1999-09-09T19:19",
                type=FieldTypeEnum("datetime-local"),
                unit="kg",
            ),
            "field 4": SingleFieldModel(
                position=11,
                value="notanumber",
                type=FieldTypeEnum("number"),
                unit="kg",
            ),
            "field 7": SingleFieldModel(
                position=99,
                value="blabla",
                type=FieldTypeEnum("text"),
            ),
        },
    )

    @pytest.mark.parametrize(
        ("cell_content", "expected"),
        (
            (
                "value",
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
                {
                    "field 2": "",
                    "field 1": "",
                    "field 3": "kg",
                    "field 4": "kg",
                    "field 5": "kg",
                    "field 6": "kg",
                    "field 7": "",
                },
            ),
            (
                "combined",
                {
                    "field 2": "20",
                    "field 1": "",
                    "field 3": "20 kg",
                    "field 4": "notanumber kg",
                    "field 5": "1999-09-09T19:19 kg",
                    "field 6": "20 kg",
                    "field 7": "blabla",
                },
            ),
        ),
    )
    def test_get_simple_dict(
        self,
        cell_content: TableCellContentType,
        expected: dict[str, Any],
    ) -> None:
        transformer = ParsedMetadataToSimpleDict.new(
            cell_content=cell_content,
        )
        transformed = transformer(self.metadata)
        assert_dicts_equal(transformed, expected, order_is_important=True)


class TestParsedMetadataToPandasDtype:
    metadata = MetadataModel(
        extra_fields={
            "field 1": SingleFieldModel(
                type=FieldTypeEnum("number"),
            ),
            "field 2": SingleFieldModel(
                type=FieldTypeEnum("text"),
            ),
            "field 3": SingleFieldModel(
                type=FieldTypeEnum("time"),
            ),
            "field 4": SingleFieldModel(
                type=FieldTypeEnum("experiments"),
            ),
        },
    )

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
        cell_content: TableCellContentType,
        expected: dict[str, Any],
    ) -> None:
        transformer = ParsedMetadataToPandasDtype.new(
            cell_content=cell_content,
        )
        transformed = transformer(self.metadata)
        assert transformed == expected
