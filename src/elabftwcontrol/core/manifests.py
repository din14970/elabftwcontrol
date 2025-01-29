from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    Type,
    Union,
    cast,
    get_args,
)

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self

from elabftwcontrol.client import ElabftwApi
from elabftwcontrol.core.models import (
    ConfigMetadata,
    ElabftwControlConfig,
    FieldTypeEnum,
    GroupModel,
    IdNode,
    MetadataModel,
    NameNode,
    ObjectTypes,
    SingleFieldModel,
)
from elabftwcontrol.types import EntityTypes
from elabftwcontrol.upload.diff import Diff
from elabftwcontrol.upload.graph import DependencyGraph

# from elabftwcontrol.upload.state import EnrichedObj

Pathlike = Union[str, Path]


IdResolver = Callable[[NameNode], IdNode | None]
NameResolver = Callable[[IdNode], NameNode]


class MetadataManifestConfig(BaseModel):
    display_main_text: bool = True


class BaseMetaField(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        coerce_numbers_to_str=True,
    )
    name: str
    group: Optional[str] = None
    description: Optional[str] = None
    required: Optional[bool] = None
    readonly: Optional[bool] = None

    @classmethod
    def from_single_field_model(
        cls,
        name: str,
        group: Optional[str],
        model: SingleFieldModel,
        name_resolver: NameResolver,
    ) -> Self:
        """Create a field manifest from a SingleFieldModel parsed from real metadata"""
        extracted_field_data = cls.get_creation_data(
            name=name,
            group=group,
            model=model,
            name_resolver=name_resolver,
        )
        return cls(**extracted_field_data)

    @classmethod
    def get_creation_data(
        cls,
        name: str,
        group: Optional[str],
        model: SingleFieldModel,
        name_resolver: NameResolver,
    ) -> dict[str, Any]:
        """The data needed for creating a manifest
        May need to be overridden by base classes, particularly linked fields
        """
        field_data = model.model_dump(exclude_none=True)
        field_members = cast(dict[str, Any], cls.__fields__).keys()
        extracted_field_data: dict[str, Any] = {
            "name": name,
            "group": group,
        }
        for field_member in field_members:
            if field_member not in extracted_field_data:
                member_value = field_data.get(field_member)
                if member_value is not None:
                    extracted_field_data[field_member] = member_value
        return extracted_field_data

    def to_single_field_model(
        self,
        position: Optional[int],
        group_id: Optional[int],
        id_resolver: IdResolver,
    ) -> SingleFieldModel:
        """Convert to a single field model for real metadata"""
        data = self.get_single_field_data(
            position=position,
            group_id=group_id,
            id_resolver=id_resolver,
        )
        return SingleFieldModel(**data)

    def get_single_field_data(
        self,
        position: Optional[int],
        group_id: Optional[int],
        id_resolver: IdResolver,
    ) -> dict[str, Any]:
        """The data needed for creating a SingleFieldModel
        May need to be overridden by base classes, particularly linked fields
        """
        data = self.model_dump(exclude_none=True)
        del data["name"]
        if "group" in data:
            del data["group"]
        data["group_id"] = group_id
        data["position"] = position
        return data


class MetadataCheckboxFieldOptions(str, Enum):
    ON = "on"
    OFF = "off"


class MetadataCheckboxFieldManifest(BaseMetaField):
    type: Literal[FieldTypeEnum.checkbox] = FieldTypeEnum.checkbox
    value: MetadataCheckboxFieldOptions = MetadataCheckboxFieldOptions.OFF

    def turn_on(self) -> None:
        self.value = MetadataCheckboxFieldOptions.ON

    def turn_off(self) -> None:
        self.value = MetadataCheckboxFieldOptions.OFF

    def toggle(self) -> None:
        if self.value == MetadataCheckboxFieldOptions.OFF:
            self.value = MetadataCheckboxFieldOptions.ON
        else:
            self.value = MetadataCheckboxFieldOptions.OFF


class MetadataRadioFieldManifest(BaseMetaField):
    options: List[str]
    value: str = ""
    type: Literal[FieldTypeEnum.radio] = FieldTypeEnum.radio

    def model_post_init(self, __context: Any) -> None:
        if not self.value:
            self.value = ""

    @model_validator(mode="after")
    def check_consistency(self) -> MetadataRadioFieldManifest:
        if self.value not in self.options and self.value != "":
            raise ValueError(
                f"Field '{self.name}' has value '{self.value}' which is not part of the "
                f"options: '{self.options}'."
            )
        return self


class MetadataSelectFieldManifest(BaseMetaField):
    options: List[str]
    value: str | List[str] = ""
    type: Literal[FieldTypeEnum.select] = FieldTypeEnum.select
    allow_multi_values: Literal[None, False, True] = None

    def model_post_init(self, __context: Any) -> None:
        if not self.value:
            if self.allow_multi_values:
                self.value = []
            else:
                self.value = ""

    @model_validator(mode="after")
    def check_consistency(self) -> MetadataSelectFieldManifest:
        if self.allow_multi_values:
            if not isinstance(self.value, list):
                raise ValueError(f"Field '{self.name}' has only one value, use list.")
            not_in_options = set(self.value) - set(self.options)
            if not_in_options:
                raise ValueError(
                    f"Field '{self.name}' has option(s) `{not_in_options}`, which "
                    f"are not part of the options: '{self.options}'"
                )

        else:
            if isinstance(self.value, list):
                raise ValueError(
                    f"Field '{self.name}' has multiple values which is not enabled."
                )
            if self.value not in self.options and self.value != "":
                raise ValueError(
                    f"Field '{self.name}' has value '{self.value}' which is not part of the "
                    f"options: '{self.options}'."
                )

        return self


class MetadataTextFieldManifest(BaseMetaField):
    type: Literal[FieldTypeEnum.text] = FieldTypeEnum.text
    value: str = ""


class MetadataNumberFieldManifest(BaseMetaField):
    type: Literal[FieldTypeEnum.number] = FieldTypeEnum.number
    value: str = ""
    units: Optional[List[str]] = None
    unit: Optional[str] = None

    @model_validator(mode="after")
    def check_consistency(self) -> MetadataNumberFieldManifest:
        if self.value:
            try:
                float(self.value)
            except ValueError:
                raise ValueError(
                    f"Field '{self.name}' has value '{self.value}' which is not a number"
                )

        if self.units is None and self.unit is not None:
            raise ValueError(
                f"Field '{self.name}' has a unit '{self.unit}' but no units defined"
            )

        if (
            self.units is not None
            and self.unit is not None
            and self.unit not in self.units
        ):
            raise ValueError(
                f"Field '{self.name}' has unit '{self.unit}' which is not part of the "
                f"provided units: '{self.units}'"
            )

        return self


class MetadataEmailFieldManifest(BaseMetaField):
    type: Literal[FieldTypeEnum.email] = FieldTypeEnum.email
    value: str = ""

    @model_validator(mode="after")
    def check_consistency(self) -> MetadataEmailFieldManifest:
        if self.value:
            email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")
            if not email_regex.match(self.value):
                raise ValueError(
                    f"Field '{self.name}' has value '{self.value}' which is not a valid email"
                )
        return self


class MetadataURLFieldManifest(BaseMetaField):
    type: Literal[FieldTypeEnum.url] = FieldTypeEnum.url
    value: str = ""

    @model_validator(mode="after")
    def check_consistency(self) -> MetadataURLFieldManifest:
        if self.value:
            url_regex = re.compile(
                r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
            )
            if not url_regex.match(self.value):
                raise ValueError(
                    f"Field '{self.name}' has value '{self.value}' which is not a valid url"
                )
        return self


class MetadataDateFieldManifest(BaseMetaField):
    _FMT = "%Y-%m-%d"
    type: Literal[FieldTypeEnum.date] = FieldTypeEnum.date
    value: str = ""

    @model_validator(mode="after")
    def check_consistency(self) -> MetadataDateFieldManifest:
        if self.value:
            try:
                datetime.strptime(self.value, self._FMT)
            except ValueError:
                raise ValueError(
                    f"Field '{self.name}' has value '{self.value}' which is not a valid date "
                    "in the format YYYY-MM-DD."
                )
        return self

    def set_value(self, value: date) -> None:
        self.value = value.strftime(self._FMT)


class MetadataDatetimeFieldManifest(BaseMetaField):
    _FMT = "%Y-%m-%dT%H:%M"
    type: Literal[FieldTypeEnum.datetime_local] = FieldTypeEnum.datetime_local
    value: str = ""

    @model_validator(mode="after")
    def check_consistency(self) -> MetadataDatetimeFieldManifest:
        if self.value:
            try:
                datetime.strptime(self.value, self._FMT)
            except ValueError:
                raise ValueError(
                    f"Field '{self.name}' has value '{self.value}' which is not a valid "
                    "datetime in the format YYYY-MM-DDTHH:MM"
                )
        return self

    def set_value(self, value: datetime) -> None:
        self.value = value.strftime(self._FMT)


class MetadataTimeFieldManifest(BaseMetaField):
    _FMT = "%H:%M"
    type: Literal[FieldTypeEnum.time] = FieldTypeEnum.time
    value: str = ""

    @model_validator(mode="after")
    def check_consistency(self) -> MetadataTimeFieldManifest:
        if self.value:
            try:
                datetime.strptime(self.value, self._FMT)
            except ValueError:
                raise ValueError(
                    f"Field '{self.name}' has value '{self.value}' which is not a valid "
                    "time in the format HH:MM"
                )
        return self

    def set_value(self, value: time) -> None:
        self.value = value.strftime(self._FMT)


class _LinkFieldManifest(BaseMetaField):
    @classmethod
    def get_creation_data(
        cls,
        name: str,
        group: Optional[str],
        model: SingleFieldModel,
        name_resolver: NameResolver,
    ) -> dict[str, Any]:
        """Data needed to convert to create the manifest

        A manifest stores a name-id to another manifest
        """
        data = super().get_creation_data(name, group, model, name_resolver)
        if data["value"]:
            id_node = cls.get_id_node(data["value"])
            name_node = name_resolver(id_node)
            data["value"] = name_node.name
        return data

    def get_single_field_data(
        self,
        position: Optional[int],
        group_id: Optional[int],
        id_resolver: IdResolver,
    ) -> dict[str, Any]:
        """Data needed to convert to a SingleFieldModel

        A SingleFieldModel stores a real id
        """
        data = super().get_single_field_data(position, group_id, id_resolver)
        if data["value"]:
            name_node = self.to_name_node()
            id_node = id_resolver(name_node)
            if id_node is None:
                data["value"] = "Unknown"
            else:
                data["value"] = id_node.id
        return data

    def to_name_node(self) -> NameNode:
        raise NotImplementedError

    @classmethod
    def get_id_node(cls, id: int) -> IdNode:
        raise NotImplementedError


class MetadataItemsLinkFieldManifest(_LinkFieldManifest):
    type: Literal[FieldTypeEnum.items] = FieldTypeEnum.items
    value: str = ""

    def to_name_node(self) -> NameNode:
        return NameNode(
            kind=ObjectTypes.ITEM,
            name=self.value,
        )

    @classmethod
    def get_id_node(cls, id: int) -> IdNode:
        return IdNode(
            kind=ObjectTypes.ITEM,
            id=id,
        )


class MetadataExperimentsLinkFieldManifest(_LinkFieldManifest):
    type: Literal[FieldTypeEnum.experiments] = FieldTypeEnum.experiments
    value: str = ""

    def to_name_node(self) -> NameNode:
        return NameNode(
            kind=ObjectTypes.EXPERIMENT,
            name=self.value,
        )

    @classmethod
    def get_id_node(cls, id: int) -> IdNode:
        return IdNode(
            kind=ObjectTypes.EXPERIMENT,
            id=id,
        )


_MetadataFieldType = Union[
    MetadataCheckboxFieldManifest,
    MetadataRadioFieldManifest,
    MetadataSelectFieldManifest,
    MetadataNumberFieldManifest,
    MetadataEmailFieldManifest,
    MetadataURLFieldManifest,
    MetadataDateFieldManifest,
    MetadataDatetimeFieldManifest,
    MetadataTimeFieldManifest,
    MetadataItemsLinkFieldManifest,
    MetadataExperimentsLinkFieldManifest,
    MetadataTextFieldManifest,
]
MetadataFieldManifest = Annotated[
    _MetadataFieldType,
    Field(discriminator="type"),
]

_metadata_field_name_to_manifest_map: dict[str, Type[_MetadataFieldType]] = {
    # some hackery to extract the type name directly from the type definition
    get_args(field_type.__fields__["type"].annotation)[0]: field_type
    for field_type in get_args(_MetadataFieldType)
}

LinkedField = MetadataItemsLinkFieldManifest | MetadataExperimentsLinkFieldManifest


class MetadataGroupManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    group_name: str
    sub_fields: List[MetadataFieldManifest] = []


class ExtraFieldsManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    config: MetadataManifestConfig = MetadataManifestConfig()
    fields: Sequence[MetadataFieldManifest] = []

    @model_validator(mode="after")
    def check_consistency(self) -> ExtraFieldsManifest:
        self.field_map
        return self

    @cached_property
    def field_map(self) -> dict[str, MetadataFieldManifest]:
        field_map = {}
        for field in self.fields:
            if field.name in field_map:
                raise ValueError(f"Field name '{field.name}' is duplicated!")
            field_map[field.name] = field
        return field_map

    def to_full_representation(self) -> ExtraFieldsManifest:
        return self

    def to_metadata_str(
        self,
        id_resolver: IdResolver,
        extra_metadata: ElabftwControlConfig | None,
    ) -> str:
        """Final render of what goes into a metadata field

        Parameters
        ----------
        extra_metadata
            The metadata injected by elabftwcontrol to determine the link to the
            manifest definitions
        id_resolver
            Mechanism for converting manifest names to real Ids
        """
        model = self.to_model(extra_metadata=extra_metadata, id_resolver=id_resolver)
        return model.model_dump_json(exclude_none=True)

    @classmethod
    def from_model(
        cls,
        model: MetadataModel,
        name_resolver: NameResolver,
    ) -> Self:
        """Convert a parsed metadata JSON to a manifest

        Model metadata contain numeric links to other items.
        The name resolver converts these to names of other manifests.
        """
        if model.elabftw.display_main_text is not None:
            config = MetadataManifestConfig(
                display_main_text=model.elabftw.display_main_text,
            )
        else:
            config = MetadataManifestConfig()

        groups = model.elabftw.extra_fields_groups
        group_map = {group.id: group.name for group in groups}

        fields = []
        positions = []
        for i, (name, field) in enumerate(model.extra_fields.items()):
            if field.group_id:
                group = group_map.get(field.group_id)
            else:
                group = None
            parsed_field = cls._parse_single_field_model(
                name=name,
                group=group,
                field=field,
                name_resolver=name_resolver,
            )
            if field.position is None:
                position = -1
            else:
                position = field.position
            fields.append(parsed_field)
            positions.append((position, i))

        # sort by position
        positions.sort()
        fields = [fields[i] for _, i in positions]

        return cls(
            config=config,
            fields=fields,
        )

    def to_field_dict(
        self,
        id_resolver: IdResolver,
    ) -> dict[str, dict[str, Any]]:
        """Convert manifest to nested dictionary for Diff"""
        model = self.to_model(id_resolver=id_resolver, extra_metadata=None)
        return model.field_dict

    def to_model(
        self,
        id_resolver: IdResolver,
        extra_metadata: ElabftwControlConfig | None,
    ) -> MetadataModel:
        """Convert manifest to the expected JSON format in eLabFTW

        Some fields link to another entity. For this purpose, you must pass
        an IdResolver, which converts names of other manifests to ids of
        existing items.
        """
        extra_fields: dict[str, SingleFieldModel] = {}
        extra_fields_groups: list[GroupModel] = []

        n_group = 1
        group_map: dict[str, int] = {}

        for i, (field_name, field) in enumerate(self.field_map.items()):
            group = field.group
            if group is None:
                group_id = None
            else:
                if group not in group_map:
                    group_map[group] = n_group
                    extra_fields_groups.append(GroupModel(id=n_group, name=group))
                    n_group += 1
                group_id = group_map[group]

            new_field = field.to_single_field_model(
                position=i,
                group_id=group_id,
                id_resolver=id_resolver,
            )
            extra_fields[field_name] = new_field

        return MetadataModel(
            elabftw=ConfigMetadata(extra_fields_groups=extra_fields_groups),
            extra_fields=extra_fields,
            elabftwcontrol=extra_metadata,
        )

    @classmethod
    def _parse_single_field_model(
        cls,
        name: str,
        group: Optional[str],
        field: SingleFieldModel,
        name_resolver: NameResolver,
    ) -> _MetadataFieldType:
        field_type = field.type or "text"
        manifest_type = _metadata_field_name_to_manifest_map[field_type]
        return manifest_type.from_single_field_model(
            name=name,
            group=group,
            model=field,
            name_resolver=name_resolver,
        )

    def get_dependencies(self) -> set[NameNode]:
        """Get items and experiments that depend on this metadata"""
        dependencies: List[NameNode] = []
        for field in self._iter_linkable():
            if field.value:
                dependencies.append(field.to_name_node())

        return set(dependencies)

    def _iter_linkable(self) -> Iterator[LinkedField]:
        """Iterate only over linkable fields"""
        for field in self.fields:
            if isinstance(
                field,
                MetadataExperimentsLinkFieldManifest,
            ) or isinstance(
                field,
                MetadataItemsLinkFieldManifest,
            ):
                yield field


class ExtraFieldsManifestComplex(BaseModel):
    model_config = ConfigDict(extra="forbid")
    config: MetadataManifestConfig = MetadataManifestConfig()
    fields: Sequence[Union[MetadataGroupManifest, MetadataFieldManifest]] = []

    def to_full_representation(self) -> ExtraFieldsManifest:
        expanded_fields: list[MetadataFieldManifest] = []

        for data in self.fields:
            if isinstance(data, MetadataGroupManifest):
                for subfield in data.sub_fields:
                    if subfield.group is not None and subfield.group != data.group_name:
                        raise ValueError(
                            f"Subfield '{subfield.name}' in group '{data.group_name}' has an "
                            f"incompatible group name: '{subfield.group}'"
                        )
                    field_copy = subfield.model_copy(
                        update={"group": data.group_name},
                        deep=True,
                    )
                    expanded_fields.append(field_copy)
            else:
                field_copy = data.model_copy(deep=True)
                expanded_fields.append(field_copy)

        return ExtraFieldsManifest(
            config=self.config.model_copy(),
            fields=expanded_fields,
        )


class _ValueAndUnit(BaseModel):
    model_config = ConfigDict(extra="forbid", coerce_numbers_to_str=True)

    value: str | List[str]
    unit: Optional[str] = None


class SimpleExtraFieldsManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    config: Optional[MetadataManifestConfig] = None
    field_values: Dict[str, str | List[str] | _ValueAndUnit] = {}

    def to_full_representation(
        self,
        parent_extra_fields: ExtraFieldsManifest | ExtraFieldsManifestComplex,
    ) -> ExtraFieldsManifest:
        extra_fields = parent_extra_fields.to_full_representation().model_copy(
            deep=True
        )

        if self.config is not None:
            extra_fields.config = self.config

        for field_name, field_value in self.field_values.items():
            if field_name not in extra_fields.field_map:
                raise ValueError(f"Field '{field_name}' not in parent metadata.")

            if isinstance(field_value, _ValueAndUnit):
                value = field_value.value
                unit = field_value.unit
            else:
                value = field_value
                unit = None

            field = extra_fields.field_map[field_name]
            field.value = value
            if unit is not None and isinstance(field, MetadataNumberFieldManifest):
                field.unit = unit

        return extra_fields


class BaseElabObjManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = 1
    id: str


class HasTitleAndBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    body: Optional[str] = None


# TODO
# implement HasAuth
# canread and canwrite: Auth | None
# For this a mapping between users/teams/groups need to be made as well


class HasTags(BaseModel):
    tags: Optional[Sequence[str]] = None

    def get_tags(self) -> Sequence[str] | None:
        return self.tags

    def render_tags(self) -> str | None:
        if self.tags is not None:
            return "|".join(self.tags)
        else:
            return None


class HasExtraFields(BaseModel):
    extra_fields: ExtraFieldsManifest | ExtraFieldsManifestComplex | None = None

    def get_extra_fields(self) -> ExtraFieldsManifest | None:
        if self.extra_fields is None:
            return None
        return self.extra_fields.to_full_representation()

    def render_metadata(
        self,
        id_resolver: IdResolver,
        extra_metadata: ElabftwControlConfig | None,
    ) -> str:
        extra_fields = self.extra_fields or ExtraFieldsManifest()
        return extra_fields.to_full_representation().to_metadata_str(
            id_resolver=id_resolver,
            extra_metadata=extra_metadata,
        )


class _ObjStateDataInterface(Protocol):
    @property
    def id(self) -> int: ...

    @property
    def metadata(self) -> MetadataModel: ...

    @property
    def tag_map(self) -> dict[str, int]: ...

    def get_values(self, keys: Iterable[str]) -> dict[str, Any]: ...


class _StateInterface(Protocol):
    def get_id(self, node: NameNode) -> IdNode | None: ...

    def get_name(self, node: IdNode) -> NameNode | None: ...

    def contains_id(self, node: IdNode) -> bool: ...

    def contains_name(self, node: NameNode) -> bool: ...

    def get_state_obj(self, node: IdNode) -> _ObjStateDataInterface | None: ...

    def get_manifest(self, node: NameNode) -> _Spec: ...


class TagPatch(NamedTuple):
    entity_type: EntityTypes
    id: int
    to_delete: Mapping[str, int]
    to_add: Collection[str]

    @classmethod
    def new(
        cls,
        entity_type: EntityTypes,
        id: int,
        old: Mapping[str, int],
        new: Collection[str],
    ) -> Self:
        old_s = set(old.keys())
        new_s = set(new)
        return cls(
            entity_type=entity_type,
            id=id,
            to_delete={tag: old[tag] for tag in old_s - new_s},
            to_add=new_s - old_s,
        )

    def apply(self, api: ElabftwApi) -> None:
        for tag in self.to_add:
            api.tags.create(self.entity_type, self.id, tag)
        for tag_id in self.to_delete.values():
            api.tags.delete(self.entity_type, self.id, tag_id)


class Patch:
    def __init__(
        self,
        name_node: NameNode,
        state: _StateInterface,
        diff: Diff,
        extra_metadata: ElabftwControlConfig | None,
        patch_method: Callable[[Patch, ElabftwApi], None],
    ) -> None:
        self.name_node = name_node
        self.state = state
        self.diff = diff
        self.extra_metadata = extra_metadata
        self.patch_method = patch_method

    def __bool__(self) -> bool:
        return bool(self.diff)

    def __call__(self, api: ElabftwApi) -> None:
        self.patch_method(self, api)

    @property
    def spec(self) -> _Spec:
        return self.state.get_manifest(self.name_node)

    @property
    def state_obj(self) -> _ObjStateDataInterface | None:
        return self.state.get_state_obj(self.id_node)

    @property
    def id_node(self) -> IdNode:
        id_node = self.state.get_id(self.name_node)
        if id_node is None:
            raise ValueError(f"Object {self.name_node} does not exist, can not get id.")
        return id_node

    @property
    def id(self) -> int:
        return self.id_node.id

    def get_patch_body(self) -> dict[str, Any]:
        """The `body` that is passed to the respective patch function"""

        def strict_id_resolver(name_node: NameNode) -> IdNode:
            id_node = self.state.get_id(name_node)
            if id_node is None:
                raise ValueError(
                    f"Could not resolve {name_node} as a link: id not found"
                )
            return id_node

        body = self.spec.to_dict(
            id_resolver=strict_id_resolver,
            extra_metadata=self.extra_metadata,
        )

        # these can not be part of the patch body and must be removed
        exclude = ("tags",)
        for key in exclude:
            if key in body:
                del body[key]

        return body

    def get_tag_patch(self, entity_type: EntityTypes) -> TagPatch:
        new_tags = self.spec.get_tags() or []
        if self.state_obj is None:
            old_tags = {}
        else:
            old_tags = self.state_obj.tag_map
        return TagPatch.new(
            entity_type=entity_type,
            id=self.id,
            old=old_tags,
            new=new_tags,
        )

    @classmethod
    def new(
        cls,
        name_node: NameNode,
        state: _StateInterface,
        extra_metadata: ElabftwControlConfig | None,
        patch_method: Callable[[Patch, ElabftwApi], None],
    ) -> Self:
        spec = state.get_manifest(name_node)

        def lax_id_resolver(name_node: NameNode) -> IdNode | None:
            return state.get_id(name_node)

        new_dict = spec.to_dict(
            id_resolver=lax_id_resolver,
            extra_metadata=None,
        )
        # metadata is considered separately
        if "metadata" in new_dict:
            del new_dict["metadata"]

        extra_fields = spec.get_extra_fields()
        if extra_fields is None:
            new_metadata_fields = {}
        else:
            new_metadata_fields = extra_fields.to_field_dict(lax_id_resolver)

        id_node = state.get_id(name_node)
        if id_node is None:
            old_dict: dict[str, Any] = {}
            old_metadata_fields: dict[str, Any] = {}
        else:
            state_obj = state.get_state_obj(id_node)
            if state_obj is None:
                old_dict = {}
                old_metadata_fields = {}
            else:
                old_dict = state_obj.get_values(new_dict.keys())
                old_metadata_fields = state_obj.metadata.field_dict

        if "metadata" in old_dict:
            del old_dict["metadata"]

        diff = Diff.new(
            old=old_dict,
            new=new_dict,
            old_metadata_fields=old_metadata_fields,
            new_metadata_fields=new_metadata_fields,
        )
        return cls(
            name_node=name_node,
            state=state,
            diff=diff,
            extra_metadata=extra_metadata,
            patch_method=patch_method,
        )


class ItemsTypePatchMethod:
    def __call__(self, patch: Patch, api: ElabftwApi) -> None:
        patch_body = patch.get_patch_body()
        # Due to a weirdness that the API returns color without hash
        # but as input you must provide a hash
        if "color" in patch_body:
            color = patch_body["color"]
            if not color.startswith("#"):
                patch_body["color"] = f"#{color}"
        api.items_types.patch(patch.id, patch_body)


class HasTitleBodyAndExtraFields(HasTitleAndBody, HasExtraFields):
    def to_dict(
        self,
        id_resolver: IdResolver,
        extra_metadata: ElabftwControlConfig | None,
    ) -> dict[str, Any]:
        dct = self.model_dump(exclude_none=True)
        if self.extra_fields is not None:
            dct["metadata"] = super().render_metadata(
                id_resolver=id_resolver,
                extra_metadata=extra_metadata,
            )
            del dct["extra_fields"]
        return dct


class HasTitleBodyExtraFieldsAndTags(HasTitleBodyAndExtraFields, HasTags):
    def to_dict(
        self,
        id_resolver: IdResolver,
        extra_metadata: ElabftwControlConfig | None,
    ) -> dict[str, Any]:
        dct = super().to_dict(
            id_resolver=id_resolver,
            extra_metadata=extra_metadata,
        )
        if self.tags is not None:
            dct["tags"] = super().render_tags()
        return dct


class ItemsTypeSpecManifest(HasTitleBodyAndExtraFields):
    color: Optional[str] = None

    def get_tags(self) -> Collection[str] | None:
        return None

    def get_dependencies(self) -> set[NameNode]:
        extra_fields = self.get_extra_fields()
        if extra_fields is None:
            return set()
        else:
            return extra_fields.get_dependencies()

    def to_patch(
        self,
        name_node: NameNode,
        state: _StateInterface,
        version: str | None,
    ) -> Patch:
        return Patch.new(
            name_node=name_node,
            state=state,
            extra_metadata=ElabftwControlConfig(
                template_name=name_node.name,
                version=version,
            ),
            patch_method=ItemsTypePatchMethod(),
        )

    def to_dict(
        self,
        id_resolver: IdResolver,
        extra_metadata: ElabftwControlConfig | None,
    ) -> dict[str, Any]:
        dct = super().to_dict(
            id_resolver=id_resolver,
            extra_metadata=extra_metadata,
        )
        # Due to a weirdness that the API returns color without hash
        # but as input you must provide a hash
        if self.color is not None:
            if self.color.startswith("#"):
                dct["color"] = self.color.replace("#", "")
        return dct


class ItemsTypeManifest(BaseElabObjManifest):
    kind: Literal[ObjectTypes.ITEMS_TYPE] = ObjectTypes.ITEMS_TYPE
    spec: ItemsTypeSpecManifest


class ExperimentTemplatePatchMethod:
    def __call__(self, patch: Patch, api: ElabftwApi) -> None:
        api.experiments_templates.patch(patch.id, patch.get_patch_body())
        patch.get_tag_patch(EntityTypes.EXPERIMENTS_TEMPLATE).apply(api)


class ExperimentTemplateSpecManifest(HasTitleBodyExtraFieldsAndTags):
    def get_dependencies(self) -> set[NameNode]:
        extra_fields = self.get_extra_fields()
        if extra_fields is None:
            return set()
        else:
            return extra_fields.get_dependencies()

    def to_patch(
        self,
        name_node: NameNode,
        state: _StateInterface,
        version: str | None,
    ) -> Patch:
        return Patch.new(
            name_node=name_node,
            state=state,
            extra_metadata=ElabftwControlConfig(
                template_name=name_node.name,
                version=version,
            ),
            patch_method=ExperimentTemplatePatchMethod(),
        )


class ExperimentTemplateManifest(BaseElabObjManifest):
    kind: Literal[ObjectTypes.EXPERIMENTS_TEMPLATE] = ObjectTypes.EXPERIMENTS_TEMPLATE
    spec: ExperimentTemplateSpecManifest


class ItemsPatchMethod:
    def __call__(self, patch: Patch, api: ElabftwApi) -> None:
        api.items.patch(patch.id, patch.get_patch_body())
        patch.get_tag_patch(EntityTypes.ITEM).apply(api)


class _ItemSpecificFields(BaseModel):
    category: str
    rating: int | None = None
    book_can_overlap: bool | None = None
    book_cancel_minutes: int | None = None
    book_is_cancellable: bool | None = None
    book_max_minutes: int | None = None
    book_max_slots: int | None = None
    is_bookable: bool | None = None
    # TODO: canbook -> Auth


class ItemSpecManifest(HasTitleBodyExtraFieldsAndTags, _ItemSpecificFields):
    def get_dependencies(self) -> set[NameNode]:
        dependencies: set[NameNode] = set()

        parent_node = NameNode(kind=ObjectTypes.ITEMS_TYPE, name=self.category)
        dependencies.add(parent_node)

        extra_fields = self.get_extra_fields()

        if extra_fields is not None:
            dependencies = dependencies.union(extra_fields.get_dependencies())

        return dependencies

    def to_patch(
        self,
        name_node: NameNode,
        state: _StateInterface,
        version: str | None,
    ) -> Patch:
        return Patch.new(
            name_node=name_node,
            state=state,
            extra_metadata=ElabftwControlConfig(
                template_name=self.category,
                name=name_node.name,
                version=version,
            ),
            patch_method=ItemsPatchMethod(),
        )

    def to_dict(
        self,
        id_resolver: IdResolver,
        extra_metadata: ElabftwControlConfig | None,
    ) -> dict[str, Any]:
        dct = super().to_dict(
            id_resolver=id_resolver,
            extra_metadata=extra_metadata,
        )
        del dct["category"]
        return dct


class ItemSpecManifestSimplifiedMetadata(HasTitleAndBody, HasTags, _ItemSpecificFields):
    extra_fields: SimpleExtraFieldsManifest

    def to_full_representation(
        self,
        parent: ItemsTypeSpecManifest,
    ) -> ItemSpecManifest:
        data = self.model_dump()

        parent_fields = parent.extra_fields

        if parent_fields is None:
            if self.extra_fields:
                raise ValueError(
                    "Item type has no extra fields while the item expects it."
                )
        else:
            full_extra_fields = self.extra_fields.to_full_representation(parent_fields)
            data["extra_fields"] = full_extra_fields

        return ItemSpecManifest(**data)


class ItemManifest(BaseElabObjManifest):
    kind: Literal[ObjectTypes.ITEM] = ObjectTypes.ITEM
    spec: ItemSpecManifest | ItemSpecManifestSimplifiedMetadata

    def render_spec(self, parent: ItemsTypeSpecManifest) -> ItemSpecManifest:
        if isinstance(self.spec, ItemSpecManifestSimplifiedMetadata):
            return self.spec.to_full_representation(parent)
        else:
            return self.spec


class ExperimentPatchMethod:
    def __call__(self, patch: Patch, api: ElabftwApi) -> None:
        api.experiments.patch(patch.id, patch.get_patch_body())
        patch.get_tag_patch(EntityTypes.EXPERIMENT).apply(api)


class _ExperimentSpecificFields(BaseModel):
    template: str | None = None
    rating: int | None = None


class ExperimentSpecManifest(HasTitleBodyExtraFieldsAndTags, _ExperimentSpecificFields):
    def get_parent(self) -> NameNode | None:
        if self.template is None:
            return None

        return NameNode(kind=ObjectTypes.EXPERIMENTS_TEMPLATE, name=self.template)

    def get_dependencies(self) -> set[NameNode]:
        dependencies: set[NameNode] = set()

        if self.template is not None:
            parent = NameNode(kind=ObjectTypes.EXPERIMENTS_TEMPLATE, name=self.template)
            dependencies.add(parent)

        extra_fields = self.get_extra_fields()
        if extra_fields is not None:
            dependencies = dependencies.union(extra_fields.get_dependencies())

        return dependencies

    def to_patch(
        self,
        name_node: NameNode,
        state: _StateInterface,
        version: str | None,
    ) -> Patch:
        return Patch.new(
            name_node=name_node,
            state=state,
            extra_metadata=ElabftwControlConfig(
                template_name=self.template,
                name=name_node.name,
                version=version,
            ),
            patch_method=ExperimentPatchMethod(),
        )

    def to_dict(
        self,
        id_resolver: IdResolver,
        extra_metadata: ElabftwControlConfig | None,
    ) -> dict[str, Any]:
        dct = super().to_dict(
            id_resolver=id_resolver,
            extra_metadata=extra_metadata,
        )
        if "template" in dct:
            del dct["template"]
        return dct


class ExperimentSpecManifestSimplifiedMetadata(
    HasTitleAndBody,
    HasTags,
    _ExperimentSpecificFields,
):
    extra_fields: SimpleExtraFieldsManifest

    def to_full_representation(
        self,
        parent: ExperimentTemplateSpecManifest,
    ) -> ExperimentSpecManifest:
        data = self.model_dump()

        parent_extra_fields = parent.extra_fields
        if parent_extra_fields is not None:
            full_extra_fields = self.extra_fields.to_full_representation(
                parent_extra_fields
            )
            data["extra_fields"] = full_extra_fields
        else:
            if self.extra_fields:
                raise ValueError(
                    "Experiment template has no metadata but a experiment does."
                )
        return ExperimentSpecManifest(**data)


class ExperimentManifest(BaseElabObjManifest):
    kind: Literal[ObjectTypes.EXPERIMENT] = ObjectTypes.EXPERIMENT
    spec: ExperimentSpecManifest | ExperimentSpecManifestSimplifiedMetadata

    def render_spec(
        self,
        parent: Optional[ExperimentTemplateSpecManifest],
    ) -> ExperimentSpecManifest:
        if isinstance(self.spec, ExperimentSpecManifestSimplifiedMetadata):
            if parent is None:
                raise ValueError(f"Template needed for experiment '{self.id}'")
            return self.spec.to_full_representation(parent)
        else:
            return self.spec


ElabObjManifest = Annotated[
    Union[
        ItemsTypeManifest,
        ExperimentTemplateManifest,
        ItemManifest,
        ExperimentManifest,
    ],
    Field(discriminator="kind"),
]


class ElabObjManifests(BaseModel):
    model_config = ConfigDict(extra="forbid")
    manifests: List[ElabObjManifest]


class _Spec(Protocol):
    def get_tags(self) -> Collection[str] | None: ...

    def get_extra_fields(self) -> ExtraFieldsManifest | None: ...

    def get_dependencies(self) -> set[NameNode]: ...

    def to_patch(
        self,
        name_node: NameNode,
        state: _StateInterface,
        version: str | None,
    ) -> Patch: ...

    def to_dict(
        self,
        id_resolver: IdResolver,
        extra_metadata: ElabftwControlConfig | None,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ManifestIndex:
    specs: Mapping[NameNode, _Spec]
    parents: Mapping[NameNode, NameNode]
    dependency_graph: DependencyGraph[NameNode]

    def __getitem__(self, name_node: NameNode) -> _Spec:
        return self.specs[name_node]

    def __len__(self) -> int:
        return len(self.specs)

    def get_node_creation_order(self) -> list[NameNode]:
        return self.dependency_graph.get_ordered_nodes()

    def get_node_deletion_order(self) -> list[NameNode]:
        creation_order = self.dependency_graph.get_ordered_nodes()
        creation_order.reverse()
        return creation_order

    def get_node_dependencies(self, node: NameNode) -> set[NameNode]:
        return self.dependency_graph.get_dependencies(node)

    @classmethod
    def from_manifests(cls, manifests: Iterable[ElabObjManifest]) -> Self:
        manifest_dict: dict[NameNode, ElabObjManifest] = {}
        specs: dict[NameNode, _Spec] = {}
        parents: dict[NameNode, NameNode] = {}
        dependency_graph: DependencyGraph[NameNode] = DependencyGraph(flexible=False)

        for manifest in manifests:
            new_node = NameNode(kind=manifest.kind, name=manifest.id)
            manifest_dict[new_node] = manifest
            dependency_graph.add_node(new_node)

        for node, manifest in manifest_dict.items():
            spec: _Spec
            if isinstance(manifest, ItemsTypeManifest) or isinstance(
                manifest, ExperimentTemplateManifest
            ):
                spec = manifest.spec
            elif isinstance(manifest, ItemManifest):
                parent_node = NameNode(ObjectTypes.ITEMS_TYPE, manifest.spec.category)
                item_parent = cast(
                    ItemsTypeManifest,
                    manifest_dict[parent_node],
                )
                spec = manifest.render_spec(item_parent.spec)
                parents[node] = parent_node
            elif isinstance(manifest, ExperimentManifest):
                if manifest.spec.template is None:
                    spec = manifest.render_spec(None)
                else:
                    parent_node = NameNode(
                        ObjectTypes.EXPERIMENTS_TEMPLATE,
                        manifest.spec.template,
                    )
                    experiment_parent = cast(
                        ExperimentTemplateManifest,
                        manifest_dict[parent_node],
                    )
                    parents[node] = parent_node
                    spec = manifest.render_spec(experiment_parent.spec)
            else:
                raise ValueError

            specs[node] = spec
            for dependency in spec.get_dependencies():
                dependency_graph.add_edge(node, dependency)

        return cls(
            specs=specs,
            parents=parents,
            dependency_graph=dependency_graph,
        )
