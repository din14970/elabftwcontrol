from __future__ import annotations

import json
import math
from collections import OrderedDict
from datetime import date, datetime
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pandas as pd
from elabapi_python import Experiment, ExperimentTemplate, Item, ItemsType, Link
from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    ValidationError,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)

from elabftwcontrol.client import ElabftwApi, ObjectSyncer
from elabftwcontrol._logging import logger
from elabftwcontrol.types import EntityTypes, SingleObjectTypes, StringAble
from elabftwcontrol.utils import (
    parse_optional_date,
    parse_optional_float,
    parse_optional_int,
)

ExtraFieldParser = Callable[[Dict[str, Any]], Dict[str, Any]]

Pathlike = Union[str, Path]


class MetadataLink(BaseModel):
    """Link from one elab object to another defined in metadata"""

    destination_type: SingleObjectTypes
    destination_id: int
    destination_cat_name: Optional[str] = None
    destination_title: Optional[str] = None

    @classmethod
    def from_metadata(
        cls,
        destination_type: SingleObjectTypes,
        value: str,
    ) -> MetadataLink:
        obj_id, obj_category_title, obj_title = tuple(
            map(lambda x: x.strip(), value.split("-"))
        )
        return cls(
            destination_type=destination_type,
            destination_id=int(obj_id),
            destination_cat_name=obj_category_title,
            destination_title=obj_title,
        )

    def __str__(self) -> str:
        return f"{self.destination_id} - {self.destination_cat_name} - {self.destination_title}"


def parse_optional_object_link(
    destination_type: SingleObjectTypes,
    value: Optional[str],
) -> Optional[MetadataLink]:
    if value is None:
        metalink = None
    else:
        try:
            metalink = MetadataLink.from_metadata(
                destination_type=destination_type,
                value=value,
            )
        except ValueError:
            metalink = None

    return metalink


_parse_map: Mapping[str, Callable[[Optional[str]], Any]] = {
    "number": parse_optional_float,
    "date": parse_optional_date,
    "items": lambda x: parse_optional_object_link(
        destination_type="items",
        value=x,
    ),
    "experiments": lambda x: parse_optional_object_link(
        destination_type="experiments",
        value=x,
    ),
}

NO_CATEGORY_NAME = "unknown"


class FlexibleBaseModel(BaseModel):
    def __init__(
        self,
        _model_data: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        to_merge: Dict[str, Any]
        if _model_data is None:
            to_merge = {}
        elif isinstance(_model_data, str):
            to_merge = self.__class__.parse_string_data(_model_data)
        elif isinstance(_model_data, dict):
            to_merge = _model_data
        else:
            raise ValidationError(f"Could not understand type: {type(_model_data)}")
        kwargs.update(to_merge)
        super().__init__(**kwargs)

    @classmethod
    def parse_string_data(cls, data: str) -> Dict[str, Any]:
        raise NotImplementedError("Is not implemented on class")

    @model_validator(mode="before")
    @classmethod
    def validate_input(cls, data: Any) -> Dict[str, Any]:
        if isinstance(data, str):
            to_return = cls.parse_string_data(data)
        else:
            to_return = data
        return to_return


class Auth(FlexibleBaseModel):
    base: int = 30
    teams: List[int] = Field(default_factory=list)
    users: List[int] = Field(default_factory=list)
    teamgroups: List[int] = Field(default_factory=list)

    @classmethod
    def parse_string_data(cls, data: str) -> Dict[str, Any]:
        return json.loads(data)

    def __str__(self) -> str:
        return self.model_dump_json().replace(":", ": ").replace(",", ", ")


def parse_tag_str(data: Any) -> Optional[List[str]]:
    if isinstance(data, str):
        data = data.split("|")
    return data


def parse_tag_id_str(data: Any) -> Optional[List[int]]:
    if isinstance(data, str):
        data = [int(tag_id) for tag_id in data.split(",")]
    return data


class LinkData(BaseModel):
    """A wrapper over a link"""

    source_entity: SingleObjectTypes
    source_id: int
    destination_entity: SingleObjectTypes
    destination_id: int

    @classmethod
    def from_api_data(
        cls,
        source_item: Experiment | Item,
        destination_entity: Literal["experiments", "items"],
        link: Link,
    ) -> LinkData:
        source_entity: SingleObjectTypes
        if isinstance(source_item, Experiment):
            source_entity = "experiments"
        else:
            source_entity = "items"
        return cls(
            source_entity=source_entity,
            source_id=source_item.id,
            destination_entity=destination_entity,
            destination_id=link.itemid,
        )


class WrappedLink:
    """A wrapper over a link"""

    def __init__(
        self,
        label: str,
        data: LinkData,
    ) -> None:
        self.label = label
        self.data = data

    def to_dict(self) -> Dict[str, Any]:
        return self.data.model_dump(by_alias=True)

    @classmethod
    def from_dict(
        cls,
        label: str,
        data: Dict[str, Any],
    ) -> WrappedLink:
        parsed_data = LinkData(**data)
        return cls(label=label, data=parsed_data)

    @classmethod
    def from_api_data(
        cls,
        label: str,
        source_item: Experiment | Item,
        destination_entity: Literal["experiments", "items"],
        link: Link,
    ) -> WrappedLink:
        data = LinkData.from_api_data(
            source_item=source_item,
            destination_entity=destination_entity,
            link=link,
        )
        return cls(
            label=label,
            data=data,
        )

    def create(
        self,
        client: ElabftwApi,
    ) -> None:
        data = self.data
        client.links.create(
            source_type=data.source_entity,
            source_id=data.source_id,
            destination_type=data.destination_entity,
            destination_id=data.destination_id,
        )

    def exists(
        self,
        client: ElabftwApi,
    ) -> bool:
        data = self.data
        return client.links.exists(
            source_type=data.source_entity,
            source_id=data.source_id,
            destination_type=data.destination_entity,
            destination_id=data.destination_id,
        )

    def delete(
        self,
        client: ElabftwApi,
    ) -> None:
        data = self.data
        client.links.delete(
            source_type=data.source_entity,
            source_id=data.source_id,
            destination_type=data.destination_entity,
            destination_id=data.destination_id,
        )


class ExperimentTemplateData(BaseModel):
    """A wrapper over Experiment Templates"""

    id: Optional[int] = None
    body: Optional[str] = None
    canread: Optional[Auth] = None
    canwrite: Optional[Auth] = None
    category: Optional[str] = None
    category_color: Optional[str] = None
    category_title: Optional[str] = None
    fullname: Optional[str] = None
    is_pinned: Optional[bool] = None
    locked: Optional[bool] = None
    locked_at: Optional[datetime] = None
    lockedby: Optional[int] = None
    metadata: Optional[ExtraFieldData] = None
    status: Optional[Any] = None
    status_color: Optional[str] = None
    status_title: Optional[str] = None
    tags: Annotated[Optional[List[str]], BeforeValidator(parse_tag_str)] = None
    tags_id: Annotated[Optional[List[int]], BeforeValidator(parse_tag_id_str)] = None
    teams_id: Optional[int] = None
    title: Optional[str] = None
    userid: Optional[int] = None

    @field_serializer(
        "canread",
        "canwrite",
        "metadata",
        "locked_at",
    )
    def serialize_complex_fields(
        self,
        field: Optional[StringAble],
        _info: Any,
    ) -> Optional[str]:
        if field is None:
            return None
        return str(field)

    @field_serializer("tags")
    def serialize_tags(
        self,
        tags: Optional[List[str]],
        _info: Any,
    ) -> Optional[str]:
        if tags is None:
            return tags
        return "|".join(tags)

    @field_serializer("tags_id")
    def serialize_tags_id(
        self,
        tags_id: Optional[List[int]],
        _info: Any,
    ) -> Optional[str]:
        if tags_id is None:
            return tags_id
        return ",".join(map(str, tags_id))

    @classmethod
    def from_api_data(
        cls,
        data: ExperimentTemplate,
    ) -> ExperimentTemplateData:
        base_data = data.to_dict()
        return cls(**base_data)


class WrappedExperimentTemplate:
    """A wrapper over Experiment Templates"""

    obj_type: EntityTypes = "experiments_templates"
    updatable_fields: Sequence[str] = (
        "body",
        "canread",
        "canwrite",
        "metadata",
        "title",
    )

    def __init__(
        self,
        label: str,
        data: ExperimentTemplateData,
    ) -> None:
        self.label = label
        self.data = data

    @property
    def id(self) -> Optional[int]:
        return self.data.id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        self.data.id = value

    def __str__(self) -> str:
        return f"{self.obj_type}: {self.label}"

    def get(self, property: str) -> Any:
        return getattr(self.data, property)

    @classmethod
    def from_api_data(
        cls,
        label: str,
        data: ExperimentTemplate,
    ) -> WrappedExperimentTemplate:
        data = ExperimentTemplateData.from_api_data(data)
        return cls(
            label=label,
            data=data,
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.data.model_dump(by_alias=True)

    @classmethod
    def iter(cls, syncer: ObjectSyncer) -> Iterator[WrappedExperimentTemplate]:
        return syncer.iter(cls)

    def exists(self, syncer: ObjectSyncer) -> bool:
        return syncer.exists(self)

    def push(self, syncer: ObjectSyncer) -> None:
        client = syncer.api
        syncer.push(
            self,
            create_method=client.experiments_templates.create,
        )
        syncer.patch_tags(self)

    def pull(self, syncer: ObjectSyncer) -> None:
        syncer.pull(self)

    def delete(self, syncer: ObjectSyncer) -> None:
        syncer.delete(self)


class ItemTypeData(BaseModel):
    """A wrapper over DB item types"""

    id: Optional[int] = None
    body: Optional[str] = None
    canread: Optional[Auth] = None
    canwrite: Optional[Auth] = None
    color: Optional[str] = None
    metadata: Optional[ExtraFieldData] = None
    ordering: Optional[Any] = None
    status: Optional[Any] = None
    title: Optional[str] = None

    @classmethod
    def from_api_data(
        cls,
        data: ItemsType,
    ) -> ItemTypeData:
        base_data = data.to_dict()
        return cls(**base_data)

    @field_serializer(
        "canread",
        "canwrite",
        "metadata",
    )
    def serialize_complex_fields(
        self,
        field: Optional[StringAble],
        _info: Any,
    ) -> Optional[str]:
        if field is None:
            return None
        return str(field)

    @field_validator("color")
    @classmethod
    def prepend_hash_to_color(cls, v: str) -> str:
        if not v.startswith("#"):
            v = f"#{v}"
        return v


class WrappedItemType:
    """A wrapper over DB item types"""

    obj_type: EntityTypes = "items_types"
    updatable_fields: Sequence[str] = (
        "body",
        "canread",
        "canwrite",
        "color",
        "metadata",
        "title",
    )

    def __init__(
        self,
        label: str,
        data: ItemTypeData,
    ) -> None:
        self.label = label
        self.data = data

    @property
    def id(self) -> Optional[int]:
        return self.data.id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        self.data.id = value

    def __str__(self) -> str:
        return f"{self.obj_type}: {self.label}"

    def get(self, property: str) -> Any:
        return getattr(self.data, property)

    @classmethod
    def from_api_data(
        cls,
        label: str,
        data: ItemsType,
    ) -> WrappedItemType:
        data = ItemTypeData.from_api_data(data)
        return cls(
            label=label,
            data=data,
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.data.model_dump(by_alias=True)

    @classmethod
    def iter(cls, syncer: ObjectSyncer) -> Iterator[WrappedItemType]:
        return syncer.iter(cls)

    def exists(self, syncer: ObjectSyncer) -> bool:
        return syncer.exists(self)

    def push(self, syncer: ObjectSyncer) -> None:
        client = syncer.api
        syncer.push(
            self,
            create_method=client.items_types.create,
        )

    def pull(self, syncer: ObjectSyncer) -> None:
        syncer.pull(self)

    def delete(self, syncer: ObjectSyncer) -> None:
        syncer.delete(self)


class ExperimentData(BaseModel):
    """A wrapper over experiments"""

    id: Optional[int] = None
    create_date: Optional[date] = Field(default=None, alias="_date")
    access_key: Optional[str] = None
    body: Optional[str] = None
    body_html: Optional[str] = None
    canread: Optional[Auth] = None
    canwrite: Optional[Auth] = None
    category: Optional[int] = None
    category_color: Optional[str] = None
    category_title: Optional[str] = None
    color: Optional[str] = None
    comments: Optional[List[Dict[str, Any]]] = None
    content_type: Optional[int] = None
    created_at: Optional[datetime] = None
    elabid: Optional[str] = None
    experiments_links: Optional[Any] = None
    firstname: Optional[str] = None
    fullname: Optional[str] = None
    has_attachement: Optional[Any] = None
    has_comment: Optional[int] = None
    items_links: Optional[Any] = None
    lastchangeby: Optional[int] = None
    lastname: Optional[str] = None
    locked: Optional[bool] = None
    locked_at: Optional[datetime] = None
    lockedby: Optional[int] = None
    mainattr_title: Optional[str] = None
    metadata: Optional[ExtraFieldData] = None
    modified_at: Optional[datetime] = None
    next_step: Optional[str] = None
    orcid: Optional[str] = None
    page: Optional[str] = None
    rating: Optional[int] = None
    recent_comment: Optional[datetime] = None
    sharelink: Optional[str] = None
    state: Optional[int] = None
    status: Optional[Any] = None
    status_color: Optional[str] = None
    status_title: Optional[str] = None
    steps: Optional[Any] = None
    tags: Annotated[Optional[List[str]], BeforeValidator(parse_tag_str)] = None
    tags_id: Annotated[Optional[List[int]], BeforeValidator(parse_tag_id_str)] = None
    timestamped: Optional[bool] = None
    timestamped_at: Optional[datetime] = None
    timestampedby: Optional[int] = None
    title: Optional[str] = None
    type: Optional[str] = None
    up_item_id: Optional[int] = None
    uploads: Optional[Any] = None
    userid: Optional[int] = None

    @field_validator("color")
    @classmethod
    def prepend_hash_to_color(cls, v: str) -> str:
        if not v.startswith("#"):
            v = f"#{v}"
        return v

    @field_serializer("tags")
    def serialize_tags(
        self,
        tags: Optional[List[str]],
        _info: Any,
    ) -> Optional[str]:
        if tags is None:
            return tags
        return "|".join(tags)

    @field_serializer("tags_id")
    def serialize_tags_id(
        self,
        tags_id: Optional[List[int]],
        _info: Any,
    ) -> Optional[str]:
        if tags_id is None:
            return tags_id
        return ",".join(map(str, tags_id))

    @field_serializer(
        "canread",
        "canwrite",
        "metadata",
        "modified_at",
        "locked_at",
        "timestamped_at",
        "recent_comment",
        "created_at",
        "create_date",
    )
    def serialize_complex_fields(
        self,
        field: Optional[StringAble],
        _info: Any,
    ) -> Optional[str]:
        if field is None:
            return None
        return str(field)

    @classmethod
    def from_api_data(
        cls,
        data: Experiment,
    ) -> ExperimentData:
        base_data = data.to_dict()
        return cls(**base_data)


class WrappedExperiment:
    obj_type: EntityTypes = "experiments"
    updatable_fields: Sequence[str] = (
        "body",
        "canread",
        "canwrite",
        "metadata",
        "title",
        "rating",
    )

    def __init__(
        self,
        label: str,
        data: ExperimentData,
    ) -> None:
        self.label = label
        self.data = data

    @property
    def id(self) -> Optional[int]:
        return self.data.id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        self.data.id = value

    def __str__(self) -> str:
        return f"{self.obj_type}: {self.label}"

    def get(self, property: str) -> Any:
        return getattr(self.data, property)

    @classmethod
    def from_api_data(
        cls,
        label: str,
        data: Experiment,
    ) -> WrappedExperiment:
        data = ExperimentData.from_api_data(data)
        return cls(
            label=label,
            data=data,
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.data.model_dump(by_alias=True)

    @classmethod
    def iter(cls, syncer: ObjectSyncer) -> Iterator[WrappedExperiment]:
        return syncer.iter(cls)

    def exists(self, syncer: ObjectSyncer) -> bool:
        return syncer.exists(self)

    def push(self, syncer: ObjectSyncer) -> None:
        client = syncer.api
        category = self.data.category or -1
        syncer.push(
            self,
            create_method=lambda: client.experiments.create(category_id=category),
        )
        syncer.patch_tags(self)

    def pull(self, syncer: ObjectSyncer) -> None:
        syncer.pull(self)

    def delete(self, syncer: ObjectSyncer) -> None:
        syncer.delete(self)


class ItemData(BaseModel):
    id: Optional[int] = None
    create_date: Optional[date] = Field(default=None, alias="_date")
    access_key: Optional[str] = None
    body: Optional[str] = None
    body_html: Optional[str] = None
    book_can_overlap: Optional[bool] = None
    book_cancel_minutes: Optional[int] = None
    book_is_cancellable: Optional[bool] = None
    book_max_minutes: Optional[int] = None
    book_max_slots: Optional[int] = None
    canbook: Optional[Auth] = None
    canread: Optional[Auth] = None
    canwrite: Optional[Auth] = None
    category: Optional[int] = None
    category_color: Optional[str] = None
    category_title: Optional[str] = None
    color: Optional[str] = None
    comments: Optional[Any] = None
    content_type: Optional[int] = None
    created_at: Optional[datetime] = None
    elabid: Optional[str] = None
    experiments_links: Optional[Any] = None
    firstname: Optional[str] = None
    fullname: Optional[str] = None
    has_attachement: Optional[Any] = None
    has_comment: Optional[int] = None
    is_bookable: bool = False
    items_links: Optional[Any] = None
    lastchangeby: Optional[int] = None
    lastname: Optional[str] = None
    locked: Optional[bool] = None
    locked_at: Optional[datetime] = None
    lockedby: Optional[int] = None
    mainattr_title: Optional[str] = None
    metadata: Optional[ExtraFieldData] = None
    modified_at: Optional[datetime] = None
    next_step: Optional[str] = None
    orcid: Optional[str] = None
    page: Optional[str] = None
    rating: Optional[int] = None
    recent_comment: Optional[datetime] = None
    sharelink: Optional[str] = None
    state: Optional[int] = None
    status: Optional[Any] = None
    status_color: Optional[str] = None
    status_title: Optional[str] = None
    steps: Optional[Any] = None
    tags: Annotated[Optional[List[str]], BeforeValidator(parse_tag_str)] = None
    tags_id: Annotated[Optional[List[int]], BeforeValidator(parse_tag_id_str)] = None
    timestamped: Optional[bool] = None
    timestamped_at: Optional[datetime] = None
    timestampedby: Optional[int] = None
    title: Optional[str] = None
    type: Optional[str] = None
    up_item_id: Optional[int] = None
    uploads: Optional[Any] = None
    userid: Optional[int] = None

    @field_validator("color")
    @classmethod
    def prepend_hash_to_color(cls, v: str) -> str:
        if not v.startswith("#"):
            v = f"#{v}"
        return v

    @field_serializer("tags")
    def serialize_tags(
        self,
        tags: Optional[List[str]],
        _info: Any,
    ) -> Optional[str]:
        if tags is None:
            return tags
        return "|".join(tags)

    @field_serializer("tags_id")
    def serialize_tags_id(
        self,
        tags_id: Optional[List[int]],
        _info: Any,
    ) -> Optional[str]:
        if tags_id is None:
            return tags_id
        return ",".join(map(str, tags_id))

    @field_serializer(
        "canbook",
        "canread",
        "canwrite",
        "metadata",
        "modified_at",
        "locked_at",
        "timestamped_at",
        "recent_comment",
        "created_at",
        "create_date",
    )
    def serialize_complex_fields(
        self,
        field: Optional[StringAble],
        _info: Any,
    ) -> Optional[str]:
        if field is None:
            return None
        return str(field)

    @classmethod
    def new_from_item_type(
        cls,
        label: str,
        item_type: ItemTypeData,
    ) -> ItemData:
        if item_type.id is None:
            raise ValueError("Item type must have an assigned ID.")
        data: Dict[str, Any] = {
            "body": item_type.body,
            "canread": item_type.canread,
            "metadata": item_type.metadata,
            "category": item_type.id,
        }
        new_item = cls(**data)
        return new_item

    @classmethod
    def from_api_data(
        cls,
        data: Item,
    ) -> ItemData:
        base_data = data.to_dict()
        return cls(**base_data)


class WrappedItem:
    obj_type: EntityTypes = "items"
    updatable_fields: Sequence[str] = (
        "body",
        "book_can_overlap",
        "book_cancel_minutes",
        "book_is_cancellable",
        "book_max_minutes",
        "book_max_slots",
        "canbook",
        "canread",
        "canwrite",
        "is_bookable",
        "metadata",
        "title",
        "rating",
    )

    def __init__(
        self,
        label: str,
        data: ItemData,
    ) -> None:
        self.label = label
        self.data = data

    @property
    def id(self) -> Optional[int]:
        return self.data.id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        self.data.id = value

    def __str__(self) -> str:
        return f"{self.obj_type}: {self.label}"

    def get(self, property: str) -> Any:
        return getattr(self.data, property)

    @classmethod
    def from_api_data(
        cls,
        label: str,
        data: Item,
    ) -> WrappedItem:
        data = ItemData.from_api_data(data)
        return cls(
            label=label,
            data=data,
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.data.model_dump(by_alias=True)

    @classmethod
    def iter(cls, syncer: ObjectSyncer) -> Iterator[WrappedItem]:
        return syncer.iter(cls)

    def exists(self, syncer: ObjectSyncer) -> bool:
        return syncer.exists(self)

    def push(self, syncer: ObjectSyncer) -> None:
        client = syncer.api
        category = self.data.category
        if category is None:
            raise ValueError("Could not create item with unknown category")
        syncer.push(
            self,
            create_method=lambda: client.items.create(category_id=category),
        )
        syncer.patch_tags(self)

    def pull(self, syncer: ObjectSyncer) -> None:
        syncer.pull(self)

    def delete(self, syncer: ObjectSyncer) -> None:
        syncer.delete(self)


class ExtraFieldData(FlexibleBaseModel):
    fields: OrderedDict[str, SingleFieldData]
    category_map: Dict[int, str]

    @classmethod
    def parse_string_data(cls, data: str) -> Dict[str, Any]:
        loaded = json.loads(data)
        field_dict, category_map = cls._parse_extra_data(loaded)
        return {
            "fields": field_dict,
            "category_map": category_map,
        }

    def get_field_names(self) -> Iterator[str]:
        return (name for name in self.fields.keys())

    def get_field_values(self) -> Iterator[Any]:
        return (field.value for field in self.fields.values())

    def get_field_categories(self) -> Iterator[Optional[str]]:
        for field in self.fields.values():
            yield self.get_category_name(field)

    def get_field_units(self) -> Iterator[Optional[str]]:
        return (field.unit for field in self.fields.values())

    def get_category_name(self, field: SingleFieldData) -> Optional[str]:
        """Get the name of a field category"""
        return self.category_map.get(field.group_id) if field.group_id else None

    def set_value(
        self,
        field_name: str,
        value: Any,
    ) -> None:
        """Change the value of a single field"""
        self.fields[field_name].set_value(value)

    def set_unit(
        self,
        field_name: str,
        unit: str,
    ) -> None:
        """Change the unit of a single field"""
        field = self.fields[field_name]
        field.set_unit(unit)

    def set_values(
        self,
        new_values: Dict[str, Any],
    ) -> None:
        """Change the values of the fields by name. Each failed field setting is logged; no exceptions are raised."""
        for name, value in new_values.items():
            if name not in self.fields:
                logger.warn(f"Field '{name}' does not exist and could not be set.")
                continue
            try:
                self.set_value(name, value)
            except Exception as e:
                logger.warn(f"Field '{name}' could not be set: {e}")

    @classmethod
    def from_file(
        cls,
        filepath: Pathlike,
    ) -> ExtraFieldData:
        with open(filepath) as f:
            extra_data = json.load(f)
        return cls.from_extra_data(extra_data)

    @classmethod
    def from_string(
        cls,
        data_str: str,
    ) -> ExtraFieldData:
        extra_data = json.loads(data_str)
        return cls.from_extra_data(extra_data)

    def __str__(self) -> str:
        extra_data = self.to_extra_data()
        return self.extra_data_to_str(extra_data)

    def to_file(self, filepath: Pathlike) -> None:
        extra_data = self.to_extra_data()
        str_data = self.extra_data_to_str(extra_data)
        with open(filepath, "w") as f:
            f.write(str_data)

    @classmethod
    def extra_data_to_str(cls, extra_data: Dict[str, Any]) -> str:
        return json.dumps(extra_data)

    @classmethod
    def _parse_extra_data(
        cls,
        data: Dict[str, Any],
    ) -> Tuple[OrderedDict[str, SingleFieldData], Dict[int, str]]:
        extra_fields_groups = data.get("elabftw", {}).get("extra_fields_groups", {})
        category_map: dict[int, str] = {
            int(field_group["id"]): field_group["name"]
            for field_group in extra_fields_groups
        }

        extra_fields = data.get("extra_fields", {})
        fields: List[SingleFieldData] = []

        for name, info in extra_fields.items():
            raw_data = RawFieldData(name=name, info=info)
            fields.append(SingleFieldData.from_raw_field_data(raw_data))

        fields = cls._sort_fields_by_position(fields)
        field_dict = cls._field_list_to_ordered_dict(fields)
        return field_dict, category_map

    @classmethod
    def from_extra_data(
        cls,
        extra_data: Dict[str, Any],
    ) -> ExtraFieldData:
        field_dict, category_map = cls._parse_extra_data(extra_data)
        return cls(
            fields=field_dict,
            category_map=category_map,
        )

    @classmethod
    def _field_list_to_ordered_dict(
        cls,
        fields: Sequence[SingleFieldData],
    ) -> OrderedDict[str, SingleFieldData]:
        return OrderedDict((field.name, field) for field in fields)

    @model_serializer
    def to_extra_data(self) -> Dict[str, Any]:
        if not self.fields:
            return {}

        extra_data: Dict[str, Any] = {
            "elabftw": {
                "extra_fields_groups": [],
            },
            "extra_fields": {},
        }
        for id, category in self.category_map.items():
            group_data = {"id": id, "name": category}
            extra_data["elabftw"]["extra_fields_groups"].append(group_data)

        for field in self.fields.values():
            raw_field = field.to_raw_field_data()
            extra_data["extra_fields"][raw_field.name] = raw_field.info

        return extra_data

    @classmethod
    def from_lists(
        cls,
        field_names: Sequence[str],
        field_values: Sequence[Any],
        field_categories: Sequence[Optional[str]],
        field_units: Sequence[Optional[str]],
    ) -> ExtraFieldData:
        fields = []
        reverse_category_map: Dict[str, int] = {}
        category_counter = 1
        for category in field_categories:
            if category not in reverse_category_map and category is not None:
                reverse_category_map[category] = category_counter
                category_counter += 1

        for i, (name, value, category, unit) in enumerate(
            zip(
                field_names,
                field_values,
                field_categories,
                field_units,
            )
        ):
            group_id = reverse_category_map.get(category) if category else None

            field_type = "text"
            if isinstance(value, float) or isinstance(value, int):
                field_type = "number"
            if isinstance(value, date):
                field_type = "date"

            new_field = SingleFieldData(
                name=name,
                value=value,
                group_id=group_id,
                unit=unit,
                type=field_type,
                position=i + 1,
            )
            fields.append(new_field)

        category_map = {v: k for k, v in reverse_category_map.items()}

        fields = cls._sort_fields_by_position(fields)
        field_dict = cls._field_list_to_ordered_dict(fields)

        return cls(
            fields=field_dict,
            category_map=category_map,
        )

    def to_lists(
        self,
    ) -> Tuple[List[str], List[Any], List[Optional[str]], List[Optional[str]]]:
        """Returns names, values, categories and units as lists"""
        return (
            list(self.get_field_names()),
            list(self.get_field_values()),
            list(self.get_field_categories()),
            list(self.get_field_units()),
        )

    @classmethod
    def empty(cls) -> ExtraFieldData:
        return cls(fields=OrderedDict(), category_map={})

    def iter_fields(self) -> Iterator[SingleFieldData]:
        return iter(self.fields.values())

    def select_categories(self, categories: Sequence[str]) -> ExtraFieldData:
        fields: OrderedDict[str, SingleFieldData] = OrderedDict()
        category_map: Dict[int, str] = {}

        for field in self.iter_fields():
            category = self.get_category_name(field)
            if category in categories and category is not None:
                fields[field.name] = field
                if field.group_id is not None:
                    category_map[field.group_id] = category

        return ExtraFieldData(
            fields=fields,
            category_map=category_map,
        )

    def select_fields(self, field_names: Sequence[str]) -> ExtraFieldData:
        fields: OrderedDict[str, SingleFieldData] = OrderedDict()
        category_map: Dict[int, str] = {}

        for field in self.iter_fields():
            category = self.get_category_name(field)
            if field.name in field_names:
                fields[field.name] = field
                if category is not None and field.group_id is not None:
                    category_map[field.group_id] = category

        return ExtraFieldData(
            fields=fields,
            category_map=category_map,
        )

    def get_fields_in_category(self, category: str) -> List[SingleFieldData]:
        """Return the fields corresponding to a specific category"""
        return [
            field
            for field in self.fields.values()
            if self.get_category_name(field) == category
        ]

    def get_category_to_field_map(self) -> Dict[str, List[SingleFieldData]]:
        """Mapping <group name> -> <list of field names in that group>"""
        category_field_map: Dict[str, List[SingleFieldData]] = {}
        for field in self.fields.values():
            field_category = self.get_category_name(field) or NO_CATEGORY_NAME
            if field_category not in category_field_map:
                category_field_map[field_category] = []
            category_field_map[field_category].append(field)
        return category_field_map

    @classmethod
    def _sort_fields_by_position(
        cls,
        fields: List[SingleFieldData],
    ) -> List[SingleFieldData]:
        """Get a list of the names of extra fields in the order declared."""
        labeled_fields = [(field.position or -1, field) for field in fields]
        labeled_fields.sort()
        return [labeled_field[1] for labeled_field in labeled_fields]


class SingleFieldData(BaseModel):
    name: str
    value: Optional[Any] = None
    group_id: Optional[int] = None
    unit: Optional[str] = None
    type: Optional[str] = None
    options: Optional[List[str]] = None
    required: Optional[bool] = None
    description: Optional[str] = None
    units: Optional[List[str]] = None
    position: Optional[int] = None
    allow_multi_values: Optional[bool] = None

    def __lt__(self, other: SingleFieldData) -> bool:
        # this is for ordering in case templates duplicate position
        return (self.position, self.name) < (other.position, other.name)

    def __gt__(self, other: SingleFieldData) -> bool:
        # this is for ordering in case templates duplicate position
        return (self.position, self.name) > (other.position, other.name)

    def set_value(self, new_value: Any) -> None:
        if new_value is None:
            self.value = None
            return
        # the field is a selection box of some kind
        if self.options is not None:
            if new_value not in self.options:
                raise ValueError(f"{new_value} not in options {self.options}")
        # the field is numeric
        if self.type == "number":
            try:
                value = float(new_value)
                self.value = value if not math.isnan(value) else None
                return
            except TypeError:
                raise ValueError(f"{new_value} can not be cast to a number")
        # the field is a date
        if self.type == "date":
            if not isinstance(new_value, date):
                raise ValueError(f"{new_value} is not a date")
            # check for NaT
            if pd.isnull(new_value):
                new_value = None
        # the field is a link to an item or experiment
        if self.type == "items" or self.type == "experiments":
            if not isinstance(new_value, MetadataLink):
                raise ValueError("Value is not a MetadataLink")
        # the field is anything else
        self.value = new_value

    def set_unit(self, new_unit: str) -> None:
        if self.units is None:
            raise ValueError(f"Field {self.name} does not have units.")
        if new_unit not in self.units:
            raise ValueError(
                f"Invalid unit {new_unit} for field {self.name}. Options: {self.units}."
            )
        self.unit = new_unit

    @classmethod
    def from_raw_field_data(
        cls,
        data: RawFieldData,
    ) -> SingleFieldData:
        name = data.name
        info = data.info

        value = info.get("value")
        if value is not None:
            parser = _parse_map.get(info.get("type", "text"), str)
            value = parser(value)

        return SingleFieldData(
            name=name,
            value=value,
            group_id=parse_optional_int(info.get("group_id")),
            unit=info.get("unit"),
            type=info.get("type"),
            options=info.get("options"),
            required=info.get("required"),
            description=info.get("description"),
            units=info.get("units"),
            position=parse_optional_int(info.get("position")),
            allow_multi_values=info.get("allow_multi_values"),
        )

    def to_raw_field_data(self) -> RawFieldData:
        field_dict: Dict[str, Any] = {}
        if self.value:
            field_dict["value"] = str(self.value)
        if self.group_id:
            field_dict["group_id"] = str(self.group_id)
        if self.unit:
            field_dict["unit"] = self.unit
        if self.type:
            field_dict["type"] = self.type
        if self.options:
            field_dict["options"] = self.options
        if self.required:
            field_dict["required"] = self.required
        if self.description:
            field_dict["description"] = self.description
        if self.units:
            field_dict["units"] = self.units
        if self.position:
            field_dict["position"] = str(self.position)
        if self.allow_multi_values:
            field_dict["allow_multi_values"] = self.allow_multi_values
        return RawFieldData(name=self.name, info=field_dict)


class RawFieldData(BaseModel):
    name: str
    info: Dict[str, Any]
