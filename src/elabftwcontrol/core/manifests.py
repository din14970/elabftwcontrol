from __future__ import annotations

import re
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
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    Type,
    Union,
    cast,
    get_args,
)

from elabapi_python import Experiment
from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self

from elabftwcontrol.core.models import (
    ConfigMetadata,
    GroupModel,
    MetadataModel,
    SingleFieldModel,
)
from elabftwcontrol.upload.graph import DependencyGraph
from elabftwcontrol.utils import parse_tag_str

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
        field_names = cast(dict[str, Any], cls.__fields__).keys()
        data = {
            "name": name,
            "group": group,
            **{field_name: field_data[field_name] for field_name in field_names},
        }
        return cls(**data)

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
    type: Literal["checkbox"] = "checkbox"
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
    type: Literal["radio"] = "radio"

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
    type: Literal["select"] = "select"
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
    type: Literal["text"] = "text"
    value: str = ""


class MetadataNumberFieldManifest(BaseMetaField):
    type: Literal["number"] = "number"
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
    type: Literal["email"] = "email"
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
    type: Literal["url"] = "url"
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
    type: Literal["date"] = "date"
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
    type: Literal["datetime-local"] = "datetime-local"
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
    type: Literal["time"] = "time"
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
    type: Literal["items"] = "items"
    value: str = ""


class MetadataExperimentsLinkFieldManifest(BaseMetaField):
    type: Literal["experiments"] = "experiments"
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
    _link_map = {
        "items": "item",
        "experiments": "experiment",
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
                        id=field.value,
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

        field_tuples = []
        for name, field in model.extra_fields.items():
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
            field_tuples.append((position, parsed_field))

        # sort by position
        field_tuples.sort()
        fields = [field_tuple[1] for field_tuple in field_tuples]

        return cls.new(
            config=config,
            fields=fields,
        )

    def to_model(self) -> MetadataModel:
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
    kind: Literal["items_type"] = "items_type"
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
    kind: Literal["experiments_template"] = "experiments_template"
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

        parent_node = Node(kind="items_type", id=self.category)
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
    kind: Literal["item"] = "item"
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

    def get_dependencies(self) -> set[Node]:
        dependencies: set[Node] = set()

        if self.template is not None:
            parent = Node(kind="experiments_template", id=self.template)
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
    kind: Literal["experiment"] = "experiment"
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

    @classmethod
    def from_api_object(
        cls,
        obj: Experiment,
    ) -> ExperimentManifest:
        extra_fields_manifest = ExtraFields.from_model(obj.metadata).to_manifest()
        return cls(
            id=f"experiment_{obj.id}",
            spec=ExperimentSpecManifest(
                title=obj.title,
                tags=parse_tag_str(obj.tags),
                body=obj.body,
                extra_fields=extra_fields_manifest,
            ),
        )


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


class ManifestIndex(NamedTuple):
    items_types: dict[str, ItemsTypeSpecManifest]
    experiment_templates: dict[str, ExperimentTemplateSpecManifest]
    items: dict[str, ItemSpecManifest]
    experiments: dict[str, ExperimentSpecManifest]
    dependency_graph: DependencyGraph[Node]

    @classmethod
    def from_manifests(cls, manifests: Iterable[ElabObjManifest]) -> ManifestIndex:
        items_types: dict[Node, ItemsTypeManifest] = {}
        experiment_templates: dict[Node, ExperimentTemplateManifest] = {}
        items: dict[Node, ItemManifest] = {}
        experiments: dict[Node, ExperimentManifest] = {}
        dependency_graph: DependencyGraph[Node] = DependencyGraph(flexible=False)

        for manifest in manifests:
            new_node = Node(kind=manifest.kind, id=manifest.id)
            dependency_graph.add_node(new_node)

            if isinstance(manifest, ItemsTypeManifest):
                items_types[new_node] = manifest

            elif isinstance(manifest, ExperimentTemplateManifest):
                experiment_templates[new_node] = manifest

            elif isinstance(manifest, ExperimentManifest):
                experiments[new_node] = manifest

            elif isinstance(manifest, ItemManifest):
                items[new_node] = manifest

            else:
                raise ValueError(f"Type of '{manifest.id}' could not be identified.")

        items_types_spec: dict[str, ItemsTypeSpecManifest] = {}

        for node, items_type in items_types.items():
            items_type_spec = items_type.spec
            items_types_spec[node.id] = items_type_spec
            cls._add_dependencies_to_graph(node, items_type_spec, dependency_graph)

        experiment_templates_spec: dict[str, ExperimentTemplateSpecManifest] = {}

        for node, experiment_template in experiment_templates.items():
            experiment_template_spec = experiment_template.spec
            experiment_templates_spec[node.id] = experiment_template_spec
            cls._add_dependencies_to_graph(
                node, experiment_template_spec, dependency_graph
            )

        items_spec: dict[str, ItemSpecManifest] = {}

        for node, item in items.items():
            item_parent = items_types_spec[item.spec.category]
            item_spec = item.render_spec(item_parent)
            items_spec[node.id] = item_spec
            cls._add_dependencies_to_graph(node, item_spec, dependency_graph)

        experiments_spec: dict[str, ExperimentSpecManifest] = {}

        for node, experiment in experiments.items():
            experiment_parent: Optional[ExperimentTemplateSpecManifest]
            if experiment.spec.template is None:
                experiment_parent = None
            else:
                experiment_parent = experiment_templates_spec[experiment.spec.template]
            experiment_spec = experiment.render_spec(experiment_parent)
            experiments_spec[node.id] = experiment_spec
            cls._add_dependencies_to_graph(node, experiment_spec, dependency_graph)

        return cls(
            items_types=items_types_spec,
            experiment_templates=experiment_templates_spec,
            items=items_spec,
            experiments=experiments_spec,
            dependency_graph=dependency_graph,
        )

    @classmethod
    def _add_dependencies_to_graph(
        cls,
        node: Node,
        spec: _Spec,
        dependency_graph: DependencyGraph[Node],
    ) -> None:
        for dependency in spec.get_dependencies():
            dependency_graph.add_edge(node, dependency)


class Node(NamedTuple):
    """Minimum piece of information to identify a definition"""

    kind: str
    id: str


# field_value = f"{manifest.id} - {parent_title} - {manifest_title}"
