from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
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

from elabftwcontrol.core.models import (
    ConfigMetadata,
    ElabftwControlConfig,
    FieldTypeEnum,
    GroupModel,
    MetadataModel,
)
from elabftwcontrol.core.models import NameNode as Node
from elabftwcontrol.core.models import ObjectTypes, SingleFieldModel
from elabftwcontrol.upload.graph import DependencyGraph

Pathlike = Union[str, Path]


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
    ) -> Self:
        field_data = model.model_dump()
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
        return cls(**extracted_field_data)

    def to_single_field_model(
        self,
        position: Optional[int],
        group_id: Optional[int],
    ) -> SingleFieldModel:
        data = self.model_dump()
        del data["name"]
        del data["group"]
        data["group_id"] = group_id
        data["position"] = position
        return SingleFieldModel(**data)


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
            self.value = self.options[0]

    @model_validator(mode="after")
    def check_consistency(self) -> MetadataRadioFieldManifest:
        if self.value not in self.options:
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
                self.value = [self.options[0]]
            else:
                self.value = self.options[0]

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
            if self.value not in self.options:
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


class MetadataItemsLinkFieldManifest(BaseMetaField):
    type: Literal[FieldTypeEnum.items] = FieldTypeEnum.items
    value: str = ""


class MetadataExperimentsLinkFieldManifest(BaseMetaField):
    type: Literal[FieldTypeEnum.experiments] = FieldTypeEnum.experiments
    value: str = ""


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


class MetadataGroupManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    group_name: str
    sub_fields: List[MetadataFieldManifest]


class ExtraFields:
    """Intermediate representation between manifests and real metadata"""

    _link_map = {
        FieldTypeEnum.items: ObjectTypes.ITEM,
        FieldTypeEnum.experiments: ObjectTypes.EXPERIMENT,
    }

    def __init__(
        self,
        config: MetadataManifestConfig,
        field_map: dict[str, MetadataFieldManifest],
    ) -> None:
        self.config = config
        self.field_map = field_map

    @classmethod
    def new(
        cls,
        config: MetadataManifestConfig,
        fields: Sequence[MetadataFieldManifest],
    ) -> Self:
        field_map = cls.get_field_map(fields)
        return cls(
            config=config,
            field_map=field_map,
        )

    @property
    def field_names(self) -> list[str]:
        return list(self.field_map.keys())

    @property
    def fields(self) -> Iterator[MetadataFieldManifest]:
        for field in self.field_map.values():
            yield field

    def __getitem__(self, field_name: str) -> MetadataFieldManifest:
        return self.field_map[field_name]

    def __contains__(self, field_name: str) -> bool:
        return field_name in self.field_map

    @classmethod
    def get_field_map(
        cls,
        fields: Sequence[MetadataFieldManifest],
    ) -> dict[str, MetadataFieldManifest]:
        field_map = {}
        for field in fields:
            if field.name in field_map:
                raise ValueError(f"Field name '{field.name}' is duplicated!")
            field_map[field.name] = field
        return field_map

    def iter_linkable(
        self,
    ) -> Iterator[
        MetadataItemsLinkFieldManifest | MetadataExperimentsLinkFieldManifest
    ]:
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

    def get_dependencies(self) -> set[Node]:
        """Get items and experiments that depend on this metadata"""
        dependencies: List[Node] = []
        for field in self.iter_linkable():
            if field.value:
                dependencies.append(
                    Node(
                        kind=self._link_map[field.type],
                        name=field.value,
                    )
                )

        return set(dependencies)

    @classmethod
    def parse(cls, **kwargs: Any) -> Self:
        """Shortcut for directly creating ExtraFields from dictionary data"""
        parsed = ExtraFieldsManifest(**kwargs)
        return cls.from_manifest(parsed)

    @classmethod
    def from_model(cls, model: MetadataModel) -> Self:
        """Convert a parsed metadata JSON to this intermediate representation"""
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

        return cls.new(
            config=config,
            fields=fields,
        )

    def to_model(
        self,
        extra_metadata: ElabftwControlConfig | None = None,
    ) -> MetadataModel:
        """Convert manifest to the expected JSON format in eLabFTW"""
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

            new_field = field.to_single_field_model(position=i, group_id=group_id)
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
    ) -> _MetadataFieldType:
        field_type = field.type or "text"
        manifest_type = _metadata_field_name_to_manifest_map[field_type]
        return manifest_type.from_single_field_model(
            name=name,
            group=group,
            model=field,
        )

    @classmethod
    def from_manifest(cls, manifest: ExtraFieldsManifest) -> Self:
        expanded_fields: list[MetadataFieldManifest] = []

        for data in manifest.fields:
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

        return cls.new(
            config=manifest.config.model_copy(),
            fields=expanded_fields,
        )

    def to_manifest(self) -> ExtraFieldsManifest:
        return ExtraFieldsManifest(
            config=self.config,
            fields=list(self.fields),
        )


class ExtraFieldsManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    config: MetadataManifestConfig = MetadataManifestConfig()
    fields: Sequence[Union[MetadataFieldManifest, MetadataGroupManifest]] = []

    def to_full_representation(self) -> ExtraFields:
        return ExtraFields.from_manifest(self)

    def get_dependencies(self) -> set[Node]:
        return self.to_full_representation().get_dependencies()


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
        parent_extra_fields: ExtraFieldsManifest,
    ) -> ExtraFieldsManifest:
        extra_fields = parent_extra_fields.to_full_representation()

        if self.config is not None:
            extra_fields.config = self.config

        for field_name, field_value in self.field_values.items():
            if field_name not in extra_fields:
                raise ValueError(f"Field '{field_name}' not in parent metadata.")

            if isinstance(field_value, _ValueAndUnit):
                value = field_value.value
                unit = field_value.unit
            else:
                value = field_value
                unit = None

            field = extra_fields[field_name]
            field.value = value
            if unit is not None and isinstance(field, MetadataNumberFieldManifest):
                field.unit = unit

        return extra_fields.to_manifest()


class BaseElabObjManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BaseElabObjSpecManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ItemsTypeSpecManifest(BaseElabObjSpecManifest):
    title: str
    tags: Optional[Sequence[str]] = None
    body: Optional[str] = None
    color: Optional[str] = None
    extra_fields: Optional[ExtraFieldsManifest] = None

    def get_dependencies(self) -> set[Node]:
        if self.extra_fields is None:
            return set()
        else:
            return self.extra_fields.get_dependencies()


class ItemsTypeManifest(BaseElabObjManifest):
    version: int = 1
    id: str
    kind: Literal[ObjectTypes.ITEMS_TYPE] = ObjectTypes.ITEMS_TYPE
    spec: ItemsTypeSpecManifest


class ExperimentTemplateSpecManifest(BaseElabObjSpecManifest):
    title: str
    tags: Optional[Sequence[str]] = None
    body: Optional[str] = None
    extra_fields: Optional[ExtraFieldsManifest] = None

    def get_dependencies(self) -> set[Node]:
        if self.extra_fields is None:
            return set()
        else:
            return self.extra_fields.get_dependencies()


class ExperimentTemplateManifest(BaseElabObjManifest):
    version: int = 1
    id: str
    kind: Literal[ObjectTypes.EXPERIMENTS_TEMPLATE] = ObjectTypes.EXPERIMENTS_TEMPLATE
    spec: ExperimentTemplateSpecManifest


class ItemSpecManifest(BaseElabObjSpecManifest):
    title: str
    tags: Optional[Sequence[str]] = None
    body: Optional[str] = None
    category: str
    color: Optional[str] = None
    extra_fields: Optional[ExtraFieldsManifest] = None

    def get_dependencies(self) -> set[Node]:
        dependencies: set[Node] = set()

        parent_node = Node(kind=ObjectTypes.ITEMS_TYPE, name=self.category)
        dependencies.add(parent_node)

        if self.extra_fields is not None:
            dependencies = dependencies.union(self.extra_fields.get_dependencies())

        return dependencies


class ItemSpecManifestSimplifiedMetadata(BaseElabObjSpecManifest):
    title: str
    tags: Optional[Sequence[str]] = None
    body: Optional[str] = None
    category: str
    color: Optional[str] = None
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
    version: int = 1
    id: str
    kind: Literal[ObjectTypes.ITEM] = ObjectTypes.ITEM
    spec: ItemSpecManifest | ItemSpecManifestSimplifiedMetadata

    def render_spec(self, parent: ItemsTypeSpecManifest) -> ItemSpecManifest:
        if isinstance(self.spec, ItemSpecManifestSimplifiedMetadata):
            return self.spec.to_full_representation(parent)
        else:
            return self.spec


class ExperimentSpecManifest(BaseElabObjSpecManifest):
    title: str
    tags: Optional[Sequence[str]] = None
    body: Optional[str] = None
    template: Optional[str] = None
    extra_fields: Optional[ExtraFieldsManifest] = None

    def get_parent(self) -> Node | None:
        if self.template is None:
            return None

        return Node(kind=ObjectTypes.EXPERIMENTS_TEMPLATE, name=self.template)


    def get_dependencies(self) -> set[Node]:
        dependencies: set[Node] = set()

        if self.template is not None:
            parent = Node(kind=ObjectTypes.EXPERIMENTS_TEMPLATE, name=self.template)
            dependencies.add(parent)

        if self.extra_fields is not None:
            dependencies = dependencies.union(self.extra_fields.get_dependencies())

        return dependencies


class ExperimentSpecManifestSimplifiedMetadata(BaseElabObjSpecManifest):
    title: str
    tags: Optional[Sequence[str]] = None
    body: Optional[str] = None
    template: str
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
    version: int = 1
    id: str
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
    def get_dependencies(self) -> set[Node]: ...


@dataclass(frozen=True)
class ManifestIndex:
    specs: Mapping[Node, _Spec]
    parents: Mapping[Node, Node]
    dependency_graph: DependencyGraph[Node]

    def get_node_creation_order(self) -> list[Node]:
        return self.dependency_graph.get_ordered_nodes()

    def get_node_deletion_order(self) -> list[Node]:
        creation_order = self.dependency_graph.get_ordered_nodes()
        creation_order.reverse()
        return creation_order

    def get_node_dependencies(self, node: Node) -> set[Node]:
        return self.dependency_graph.get_dependencies(node)

    @classmethod
    def from_manifests(cls, manifests: Iterable[ElabObjManifest]) -> ManifestIndex:
        manifest_dict: dict[Node, ElabObjManifest] = {}
        specs: dict[Node, _Spec] = {}
        parents: dict[Node, Node] = {}
        dependency_graph: DependencyGraph[Node] = DependencyGraph(flexible=False)

        for manifest in manifests:
            new_node = Node(kind=manifest.kind, name=manifest.id)
            manifest_dict[new_node] = manifest
            dependency_graph.add_node(new_node)

        for node, manifest in manifest_dict.items():
            spec: _Spec
            if isinstance(manifest, ItemsTypeManifest) or isinstance(
                manifest, ExperimentTemplateManifest
            ):
                spec = manifest.spec
            elif isinstance(manifest, ItemManifest):
                parent_node = Node(ObjectTypes.ITEMS_TYPE, manifest.spec.category)
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
                    parent_node = Node(
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
