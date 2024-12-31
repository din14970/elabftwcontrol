from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping, NamedTuple, Optional, Type, TypeVar, Union

from elabapi_python import Experiment, ExperimentTemplate, Item, ItemsType
from pydantic import BaseModel, Field

from elabftwcontrol._logging import logger
from elabftwcontrol.core.interfaces import HasIDAndMetadataAndDictable

Pathlike = Union[str, Path]


T = TypeVar("T")
V = TypeVar("V")


class IdNode(NamedTuple):
    """Minimum piece of information to identify an object"""

    kind: ObjectTypes
    id: int

    def __str__(self) -> str:
        return f"{self.kind}: {self.id}"


class NameNode(NamedTuple):
    """Minimum piece of information to identify a definition"""

    kind: ObjectTypes
    name: str

    def __str__(self) -> str:
        return f"{self.kind}: {self.name}"


class Auth(BaseModel):
    base: int = 30
    teams: list[int] = Field(default_factory=list)
    users: list[int] = Field(default_factory=list)
    teamgroups: list[int] = Field(default_factory=list)


class ObjectTypes(str, Enum):
    ITEM = "item"
    ITEMS_TYPE = "items_type"
    EXPERIMENT = "experiment"
    EXPERIMENTS_TEMPLATE = "experiments_template"

    def is_template(self) -> bool:
        return self == self.ITEMS_TYPE or self == self.EXPERIMENTS_TEMPLATE

    def is_individual(self) -> bool:
        return self == self.ITEM or self == self.EXPERIMENT


OBJTYPE_TO_CLASS: Mapping[ObjectTypes, Type[HasIDAndMetadataAndDictable]] = {
    ObjectTypes.ITEM: Item,
    ObjectTypes.EXPERIMENT: Experiment,
    ObjectTypes.ITEMS_TYPE: ItemsType,
    ObjectTypes.EXPERIMENTS_TEMPLATE: ExperimentTemplate,
}

CLASS_TO_OBJTYPE: Mapping[Type[HasIDAndMetadataAndDictable], ObjectTypes] = {
    Item: ObjectTypes.ITEM,
    Experiment: ObjectTypes.EXPERIMENT,
    ItemsType: ObjectTypes.ITEMS_TYPE,
    ExperimentTemplate: ObjectTypes.EXPERIMENTS_TEMPLATE,
}


class GroupModel(BaseModel):
    id: int
    name: str


class ConfigMetadata(BaseModel):
    display_main_text: Optional[bool] = None
    extra_fields_groups: list[GroupModel] = []


class FieldTypeEnum(str, Enum):
    checkbox = "checkbox"
    radio = "radio"
    select = "select"
    text = "text"
    number = "number"
    email = "email"
    url = "url"
    date = "date"
    datetime_local = "datetime-local"
    time = "time"
    items = "items"
    experiments = "experiments"


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


class SingleFieldModel(BaseModel):
    value: Optional[str | list[str]] = None
    type: FieldTypeEnum = FieldTypeEnum.text
    options: Optional[list[str]] = None
    allow_multi_values: Optional[bool] = None
    required: Optional[bool] = None
    description: Optional[str] = None
    units: Optional[list[str]] = None
    unit: Optional[str] = None
    position: Optional[int] = None
    blank_value_on_duplicate: Optional[bool] = None
    group_id: Optional[int] = None
    readonly: Optional[bool] = None

    @property
    def parsed_value(self) -> Any:
        return self._parse_field_value(self.value, self.type)

    @property
    def value_and_unit(self) -> str:
        """Value and unit concatenated"""
        if not self.value:
            return ""
        elif not self.unit:
            return str(self.value)
        else:
            return f"{self.value} {self.unit}"

    @property
    def corrected_unit(self) -> str:
        """If there is no unit, empty string"""
        if not self.value or not self.unit:
            return ""
        else:
            return self.unit

    @classmethod
    def _parse_field_value(cls, value: Any, field_type: FieldTypeEnum) -> Any:
        """Parse the value from a field based on the field type"""
        match field_type:
            case FieldTypeEnum.number:
                return cls.parse_number(value)

            case FieldTypeEnum.date:
                return cls.parse_date(value)

            case FieldTypeEnum.datetime_local:
                return cls.parse_datetime(value)

            case FieldTypeEnum.time:
                return cls.parse_time(value)

            case FieldTypeEnum.items:
                return cls.parse_item_link(value)

            case FieldTypeEnum.experiments:
                return cls.parse_experiment_link(value)

            case _:
                return value

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


class ElabftwControlConfig(BaseModel):
    template_name: str | None = None
    name: str | None = None
    version: str | None = None


class MetadataModel(BaseModel):
    elabftw: ConfigMetadata = ConfigMetadata()
    extra_fields: dict[str, SingleFieldModel] = {}
    elabftwcontrol: ElabftwControlConfig | None = None

    @property
    def ordered_fieldnames(self) -> list[str]:
        """Get the names of fields in order of position"""
        fields: list[tuple[int, str]] = []
        for fieldname, field in self.extra_fields.items():
            if field.position is None:
                position = -1
            else:
                try:
                    position = int(field.position)
                except ValueError:
                    logger.debug("Invalid position defined on field %s" % fieldname)
                    position = -1

            fields.append((position, fieldname))

        fields.sort()

        return [field[1] for field in fields]

    @property
    def fieldnames(self) -> list[str]:
        """Names of fields in arbitrary order"""
        return list(self.extra_fields.keys())

    def map_fields(self, accessor: Callable[[SingleFieldModel], T]) -> dict[str, T]:
        """Apply a function to all fields and return a dictionary with the results"""
        return {
            fieldname: accessor(self.extra_fields[fieldname])
            for fieldname in self.ordered_fieldnames
        }

    @property
    def field_values(self) -> dict[str, Any]:
        """Name: values of fields ordered by field position"""

        def accessor(field: SingleFieldModel) -> Any:
            return field.parsed_value

        return self.map_fields(accessor)

    @property
    def field_units(self) -> dict[str, str]:
        """Name: unit of fields ordered by field position"""

        def accessor(field: SingleFieldModel) -> str:
            return field.corrected_unit

        return self.map_fields(accessor)

    @property
    def field_values_and_units(self) -> dict[str, str]:
        """Name: value + unit as strings of fields ordered by field position"""

        def accessor(field: SingleFieldModel) -> str:
            return field.value_and_unit

        return self.map_fields(accessor)
