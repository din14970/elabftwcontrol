from __future__ import annotations

from enum import Enum
from typing import Any, Callable, NamedTuple, Optional, TypeVar

from elabftwcontrol._logging import logger
from elabftwcontrol.core.models import FieldTypeEnum, MetadataModel, SingleFieldModel

T = TypeVar("T")
V = TypeVar("V")


class TableCellContentType(str, Enum):
    """When downloading as a table, what should be in the cells"""

    VALUE = "value"
    UNIT = "unit"
    COMBINED = "combined"


class MetadataField(NamedTuple):
    """Named tuple representation for a field for easy conversion to pandas table"""

    name: str = ""
    type: str = ""
    group: str = ""
    description: str = ""
    position: int = -1
    value: str = ""
    unit: str = ""

    @classmethod
    def from_parsed_field(
        cls,
        data: SingleFieldModel,
        name: Optional[str] = None,
        group: Optional[str] = None,
    ) -> MetadataField:
        field_position = cls.safe_get_field_position(data.position)
        return cls(
            name=str(name or ""),
            type=data.type.value,
            group=str(group or ""),
            description=str(data.description or ""),
            position=field_position,
            value=str(data.value or ""),
            unit=str(data.unit or ""),
        )

    @classmethod
    def safe_get_field_position(cls, position: Any) -> int:
        try:
            return int(position)
        except Exception:
            return -1


class ParsedMetadataToMetadataFieldList:
    """Convert parsed object metadata to a list of named tuples"""

    def __call__(self, parsed_metadata: MetadataModel) -> list[MetadataField]:
        return self.transform_api_object_metadata(parsed_metadata)

    @classmethod
    def transform_api_object_metadata(
        cls,
        parsed_metadata: MetadataModel,
    ) -> list[MetadataField]:
        groups = parsed_metadata.elabftw.extra_fields_groups
        group_map = {group.id: group.name for group in groups}

        fields: list[MetadataField] = []
        for fieldname, field in parsed_metadata.extra_fields.items():
            group_id = field.group_id
            if group_id is None:
                field_group = None
            else:
                field_group = group_map.get(group_id)
            if field_group is None:
                logger.debug(f"Field {fieldname} does not have a valid group.")
                field_group = None

            new_field = MetadataField.from_parsed_field(
                data=field,
                name=fieldname,
                group=field_group,
            )
            fields.append(new_field)

        return fields


class ParsedMetadataToSimpleDict:
    """Convert parsed object metadata to a simplified key: value dictionary"""

    def __init__(
        self,
        parser: Callable[[MetadataModel], dict[str, Any]],
    ) -> None:
        self.parser = parser

    def __call__(self, parsed_metadata: MetadataModel) -> dict[str, Any]:
        return self.parser(parsed_metadata)

    @classmethod
    def new(
        cls,
        cell_content: TableCellContentType,
    ) -> ParsedMetadataToSimpleDict:
        match cell_content:
            case TableCellContentType.COMBINED:

                def parser(parsed_metadata: MetadataModel) -> dict[str, Any]:
                    return parsed_metadata.field_values_and_units

            case TableCellContentType.UNIT:

                def parser(parsed_metadata: MetadataModel) -> dict[str, Any]:
                    return parsed_metadata.field_units

            case TableCellContentType.VALUE:

                def parser(parsed_metadata: MetadataModel) -> dict[str, Any]:
                    return parsed_metadata.field_values

            case _:
                raise ValueError(f"Cell content '{cell_content}' not recognized.")

        return cls(parser=parser)


class ParsedMetadataToPandasDtype:
    def __init__(self, converter: Callable[[SingleFieldModel], str]) -> None:
        self.converter = converter

    @classmethod
    def new(cls, cell_content: TableCellContentType) -> ParsedMetadataToPandasDtype:
        match cell_content:
            case TableCellContentType.VALUE:
                converter = cls.get_pandas_value_dtype
            case TableCellContentType.COMBINED:
                converter = cls.get_pandas_combined_dtype
            case TableCellContentType.UNIT:
                converter = cls.get_pandas_unit_dtype
            case _:
                raise ValueError(f"Cell content type '{cell_content}' not recognized.")
        return cls(converter)

    def __call__(self, parsed_metadata: MetadataModel) -> dict[str, str]:
        return self.transform_api_object_metadata(
            parsed_metadata,
            converter=self.converter,
        )

    @classmethod
    def transform_api_object_metadata(
        cls,
        parsed_metadata: MetadataModel,
        converter: Callable[[SingleFieldModel], str],
    ) -> dict[str, str]:
        return parsed_metadata.map_fields(converter)

    @classmethod
    def get_pandas_unit_dtype(cls, field: SingleFieldModel) -> str:
        return "string"

    @classmethod
    def get_pandas_value_dtype(cls, field: SingleFieldModel) -> str:
        match field.type:
            case FieldTypeEnum.number:
                return "Float64"

            case FieldTypeEnum.date:
                return "datetime64[ns]"

            case FieldTypeEnum.datetime_local:
                return "datetime64[ns]"

            case FieldTypeEnum.time:
                return "datetime64[ns]"

            case FieldTypeEnum.items:
                return "Int64"

            case FieldTypeEnum.experiments:
                return "Int64"

            case _:
                return "string"

    @classmethod
    def get_pandas_combined_dtype(cls, field: SingleFieldModel) -> str:
        match field.type:
            case FieldTypeEnum.number:
                return "string"

            case FieldTypeEnum.date:
                return "datetime64[ns]"

            case FieldTypeEnum.datetime_local:
                return "datetime64[ns]"

            case FieldTypeEnum.time:
                return "datetime64[ns]"

            case FieldTypeEnum.items:
                return "Int64"

            case FieldTypeEnum.experiments:
                return "Int64"

            case _:
                return "string"
