from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from typing import Any, Callable, NamedTuple, Optional, TypeVar, Union

from elabftwcontrol._logging import logger

T = TypeVar("T")
V = TypeVar("V")


class TableCellContentType(str, Enum):
    """When downloading as a table, what should be in the cells"""

    VALUE = "value"
    UNIT = "unit"
    COMBINED = "combined"


def _safe_parse(
    return_type: str,
    fail_value: V,
) -> Callable[[Callable[[Any, str], T]], Callable[[Any, str], T | V]]:
    """Decorator to ensure all parsing failures are handled"""

    def decorator(parser: Callable[[Any, str], T]) -> Callable[[Any, str], T | V]:
        def wrapper(cls: Any, value: str) -> Union[T, V]:
            try:
                return parser(cls, value)
            except Exception:
                logger.debug(f"Could not parse {value} as a {return_type}")
                return fail_value

        return wrapper

    return decorator


class MetadataField(NamedTuple):
    name: str = ""
    type: str = ""
    group: str = ""
    description: str = ""
    position: int = -1
    value: str = ""
    unit: str = ""

    @classmethod
    def from_raw_data(
        cls,
        data: dict[str, Any],
        name: Optional[str] = None,
        group: Optional[str] = None,
    ) -> MetadataField:
        field_position = cls.safe_get_field_position(data.get("position"))
        return cls(
            name=str(name or ""),
            type=str(data.get("type", "")),
            group=str(group or ""),
            description=str(data.get("description", "")),
            position=field_position,
            value=str(data.get("value", "")),
            unit=str(data.get("unit", "")),
        )

    @classmethod
    def safe_get_field_position(cls, position: Any) -> int:
        try:
            return int(position)
        except Exception:
            return -1

    @property
    def parsed_value(self) -> Any:
        return self.get_parsed_value(self.value, self.type)

    @property
    def value_and_unit(self) -> str:
        return self.get_field_combined(self.value, self.unit)

    @property
    def corrected_unit(self) -> str:
        return self.get_field_unit(self.value, self.unit)

    @classmethod
    def get_parsed_value(
        cls,
        value: str,
        type: str,
    ) -> Any:
        match type:
            case "number":
                return cls.parse_number(value)

            case "date":
                return cls.parse_date(value)

            case "datetime-local":
                return cls.parse_datetime(value)

            case "time":
                return cls.parse_time(value)

            case "items":
                return cls.parse_item_link(value)

            case "experiments":
                return cls.parse_experiment_link(value)

            case _:
                return value

    @classmethod
    def get_pandas_value_dtype(
        cls,
        type: Optional[str],
    ) -> str:
        match type:
            case "number":
                return "Float64"

            case "date":
                return "datetime64[ns]"

            case "datetime-local":
                return "datetime64[ns]"

            case "time":
                return "datetime64[ns]"

            case "items":
                return "Int64"

            case "experiments":
                return "Int64"

            case _:
                return "string"

    @classmethod
    def get_pandas_combined_dtype(
        cls,
        type: Optional[str],
    ) -> str:
        match type:
            case "number":
                return "string"

            case "date":
                return "datetime64[ns]"

            case "datetime-local":
                return "datetime64[ns]"

            case "time":
                return "datetime64[ns]"

            case "items":
                return "Int64"

            case "experiments":
                return "Int64"

            case _:
                return "string"

    @classmethod
    def get_pandas_unit_dtype(
        cls,
        type: Optional[str],
    ) -> str:
        return "string"

    @classmethod
    def get_field_unit(
        cls,
        field_value: str,
        field_unit: str,
    ) -> str:
        if not field_value or not field_unit:
            return ""
        else:
            return field_unit

    @classmethod
    def get_field_combined(
        cls,
        field_value: str,
        field_unit: str,
    ) -> str:
        if not field_value:
            return ""
        elif not field_unit:
            return field_value
        else:
            return f"{field_value} {field_unit}"

    @classmethod
    @_safe_parse(return_type="number", fail_value=float("nan"))
    def parse_number(cls, value: str) -> float:
        return float(value)

    @classmethod
    @_safe_parse(return_type="date", fail_value=None)
    def parse_date(cls, value: str) -> Optional[datetime]:
        parsed = datetime.strptime(value, "%Y-%m-%d")
        return parsed

    @classmethod
    @_safe_parse(return_type="datetime", fail_value=None)
    def parse_datetime(cls, value: str) -> Optional[datetime]:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M")

    @classmethod
    @_safe_parse(return_type="time", fail_value=None)
    def parse_time(cls, value: str) -> Optional[datetime]:
        return datetime.strptime(value, "%H:%M")

    @classmethod
    @_safe_parse(return_type="link to an item", fail_value=None)
    def parse_item_link(cls, value: str) -> Optional[int]:
        return cls.parse_link(value)

    @classmethod
    @_safe_parse(return_type="link to an experiment", fail_value=None)
    def parse_experiment_link(cls, value: str) -> Optional[int]:
        return cls.parse_link(value)

    @classmethod
    def parse_link(cls, value: str) -> int:
        split = value.split(" - ")
        return int(split[0])


class MetadataParser:
    """Converts an object metadata string to a nested dictionary"""

    def __call__(self, metadata: Optional[str]) -> dict[str, Any]:
        return self.safe_parse_metadata(metadata)

    @classmethod
    def safe_parse_metadata(
        cls,
        obj_metadata: Optional[str],
    ) -> dict[str, Any]:
        if not obj_metadata:
            return {}

        try:
            data = json.loads(obj_metadata)
        except (TypeError, json.JSONDecodeError) as e:
            logger.warn(f"Metadata obj object could not be parsed as valid JSON:\n{e}")
            return {}

        return data


class ParsedMetadataToOrderedFieldnames:
    """Converts a parsed object metadata to field names ordered by position"""

    def __call__(self, parsed_metadata: dict[str, Any]) -> list[str]:
        return self.get_field_names_ordered_by_position(parsed_metadata)

    @classmethod
    def get_field_names_ordered_by_position(
        cls,
        parsed_metadata: dict[str, Any],
    ) -> list[str]:
        if "extra_fields" not in parsed_metadata:
            return []

        fields: list[tuple[int, str]] = []
        for fieldname, field in parsed_metadata["extra_fields"].items():
            try:
                position = int(field["position"])
            except (KeyError, ValueError):
                logger.debug("No valid position defined on field %s" % fieldname)
                position = -1

            fields.append((position, fieldname))

        fields.sort()

        return [field[1] for field in fields]


class ParsedMetadataToMetadataFieldList:
    """Convert parsed object metadata to a list of named tuples"""

    def __call__(self, parsed_metadata: dict[str, Any]) -> list[MetadataField]:
        return self.transform_api_object_metadata(parsed_metadata)

    @classmethod
    def transform_api_object_metadata(
        cls,
        parsed_metadata: dict[str, Any],
    ) -> list[MetadataField]:
        if "extra_fields" not in parsed_metadata:
            return []

        groups = parsed_metadata.get("elabftw", {}).get("extra_fields_groups", {})
        group_map = {int(group["id"]): group["name"] for group in groups}

        fields: list[MetadataField] = []
        for fieldname, field in parsed_metadata["extra_fields"].items():
            try:
                group_id = int(field["group_id"])
                field_group = group_map.get(group_id)
            except (KeyError, ValueError):
                logger.debug(f"Field {fieldname} does not have a valid group.")
                field_group = None

            new_field = MetadataField.from_raw_data(
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
        parser: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        self.parser = parser

    def __call__(self, parsed_metadata: dict[str, Any]) -> dict[str, Any]:
        return self.parser(parsed_metadata)

    @classmethod
    def new(
        cls,
        cell_content: TableCellContentType,
        order_fields: bool,
    ) -> ParsedMetadataToSimpleDict:
        cell_content_getter = cls.get_cell_content_getter(cell_content)

        def parser(parsed_metadata: dict[str, Any]) -> dict[str, Any]:
            return cls.transform_api_object_metadata(
                parsed_metadata,
                cell_content_getter,
                order_fields=order_fields,
            )

        return cls(parser=parser)

    @classmethod
    def get_cell_content_getter(
        cls,
        cell_content: TableCellContentType,
    ) -> Callable[[MetadataField], Any]:
        cell_content_getter: Callable[[MetadataField], Any]
        match cell_content:
            case TableCellContentType.COMBINED:

                def cell_content_getter(field: MetadataField) -> str:
                    return field.value_and_unit

            case TableCellContentType.UNIT:

                def cell_content_getter(field: MetadataField) -> str:
                    return field.corrected_unit

            case TableCellContentType.VALUE:

                def cell_content_getter(field: MetadataField) -> Any:
                    return field.parsed_value

            case _:
                raise ValueError(f"Cell content '{cell_content}' not recognized.")

        return cell_content_getter

    @classmethod
    def transform_api_object_metadata(
        cls,
        parsed_metadata: dict[str, Any],
        cell_content_getter: Callable[[MetadataField], Any],
        order_fields: bool,
    ) -> dict[str, Any]:
        if "extra_fields" not in parsed_metadata:
            return {}

        extra_fields = parsed_metadata["extra_fields"]

        if order_fields:
            fieldnames = ParsedMetadataToOrderedFieldnames()(parsed_metadata)
        else:
            fieldnames = extra_fields.keys()

        field_values: dict[str, Any] = {}
        for fieldname in fieldnames:
            field = extra_fields[fieldname]
            parsed_field = MetadataField.from_raw_data(field)
            field_values[fieldname] = cell_content_getter(parsed_field)

        return field_values


class ParsedMetadataToPandasDtype:
    def __init__(self, converter: Callable[[Optional[str]], str]) -> None:
        self.converter = converter

    @classmethod
    def new(cls, cell_content: TableCellContentType) -> ParsedMetadataToPandasDtype:
        match cell_content:
            case TableCellContentType.VALUE:
                converter = MetadataField.get_pandas_value_dtype
            case TableCellContentType.COMBINED:
                converter = MetadataField.get_pandas_combined_dtype
            case TableCellContentType.UNIT:
                converter = MetadataField.get_pandas_unit_dtype
            case _:
                raise ValueError(f"Cell content type '{cell_content}' not recognized.")
        return cls(converter)

    def __call__(self, parsed_metadata: dict[str, Any]) -> dict[str, str]:
        return self.transform_api_object_metadata(
            parsed_metadata,
            converter=self.converter,
        )

    @classmethod
    def transform_api_object_metadata(
        cls,
        parsed_metadata: dict[str, Any],
        converter: Callable[[Optional[str]], str],
    ) -> dict[str, str]:
        if "extra_fields" not in parsed_metadata:
            return {}

        extra_fields = parsed_metadata["extra_fields"]

        field_types: dict[str, str] = {}
        for fieldname, field in extra_fields.items():
            field_types[fieldname] = converter(field.get("type"))
        return field_types
