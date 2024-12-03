from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, List, Optional, Union

from pydantic import BaseModel

Pathlike = Union[str, Path]


def parse_tag_str(data: Any) -> Optional[List[str]]:
    if isinstance(data, str):
        data = data.split("|")
    return data


def parse_tag_id_str(data: Any) -> Optional[List[int]]:
    if isinstance(data, str):
        data = [int(tag_id) for tag_id in data.split(",")]
    return data


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


class SingleFieldModel(BaseModel):
    value: Optional[Any] = None
    type: FieldTypeEnum = FieldTypeEnum.text
    options: Optional[List[str]] = None
    allow_multi_values: Optional[bool] = None
    required: Optional[bool] = None
    description: Optional[str] = None
    units: Optional[List[str]] = None
    unit: Optional[str] = None
    position: Optional[int] = None
    blank_value_on_duplicate: Optional[bool] = None
    group_id: Optional[int] = None
    readonly: Optional[bool] = None


class MetadataModel(BaseModel):
    elabftw: ConfigMetadata = ConfigMetadata()
    extra_fields: dict[str, SingleFieldModel] = {}
