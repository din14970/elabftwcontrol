from __future__ import annotations

import re
from collections import OrderedDict
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
    Set,
    TypeAlias,
    Union,
)

from pydantic import BaseModel, ConfigDict, Field, model_validator

from elabftwcontrol._logging import logger
from elabftwcontrol.models import ExtraFieldData, SingleFieldData
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


MetadataFieldManifest: TypeAlias = Annotated[
    Union[
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
    ],
    Field(discriminator="type"),
]


class MetadataGroupManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    group_name: str
    sub_fields: List[MetadataFieldManifest]


class ExtraFieldsManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    config: MetadataManifestConfig = MetadataManifestConfig()
    fields: List[MetadataFieldManifest] = []
    # the reason we use an explicit index as values in the dictionary instead of
    # a direct reference to the manifest, is because on creating the dictionary the
    # data is apparently copied instead of referenced
    _field_map: Dict[str, int]

    def model_post_init(self, __context: Any) -> None:
        self._field_map = self.get_field_map()

    def __getitem__(self, field_name: str) -> MetadataFieldManifest:
        index = self._field_map[field_name]
        return self.fields[index]

    def __contains__(self, field_name: str) -> bool:
        return field_name in self._field_map

    def get_field_map(self) -> dict[str, int]:
        field_map = {}
        for i, field in enumerate(self.fields):
            if field.name in field_map:
                raise ValueError(f"Field name '{field.name}' is duplicated!")
            field_map[field.name] = i
        return field_map

    def iter(self) -> Iterator[MetadataFieldManifest]:
        for field in self.fields:
            yield field

    def iter_linkable(
        self,
    ) -> Iterator[
        MetadataItemsLinkFieldManifest | MetadataExperimentsLinkFieldManifest
    ]:
        for field in self.iter():
            if isinstance(
                field,
                MetadataExperimentsLinkFieldManifest,
            ) or isinstance(
                field,
                MetadataItemsLinkFieldManifest,
            ):
                yield field

    def get_dependencies(self) -> set[Node]:
        dependencies: List[Node] = []
        link_map = {
            "items": "item",
            "experiments": "experiment",
        }
        for field in self.iter_linkable():
            if field.value:
                dependencies.append(
                    Node(
                        kind=link_map[field.type],
                        id=field.value,
                    )
                )

        return set(dependencies)

    def parse(self) -> ExtraFieldData:
        fields: OrderedDict[str, SingleFieldData] = OrderedDict()
        category_map: Dict[int, str] = {}

        field_id = 1
        unique_groups: Set["str"] = set()

        grouped_fields: Dict[str, List[MetadataFieldManifest]] = OrderedDict()

        for field in self.iter():
            group = field.group
            if group is None:
                logger.debug(f"Field {field.name} has no associated group.")

                new_field_data = {
                    "position": field_id,
                    **field.model_dump(),
                }
                fields[field.name] = SingleFieldData(**new_field_data)
                field_id += 1

            else:
                if group not in grouped_fields:
                    grouped_fields[group] = []

                grouped_fields[group].append(field)

        for group_id, (group_name, sub_fields) in enumerate(grouped_fields.items()):
            category_map[group_id] = group_name
            unique_groups.add(group_name)

            for sub_field in sub_fields:
                new_field_data = {
                    "group_id": group_id,
                    "position": field_id,
                    **sub_field.model_dump(),
                }
                fields[sub_field.name] = SingleFieldData(**new_field_data)
                field_id += 1

        return ExtraFieldData(
            fields=fields,
            category_map=category_map,
        )


class NestedExtraFieldsManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    config: MetadataManifestConfig = MetadataManifestConfig()
    fields: List[Union[MetadataFieldManifest, MetadataGroupManifest]] = []

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
        parent_extra_fields: ExtraFieldsManifest,
    ) -> ExtraFieldsManifest:
        extra_fields = parent_extra_fields.model_copy(deep=True)

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

            extra_fields[field_name].value = value
            if unit is not None:
                extra_fields[field_name].unit = unit

        return extra_fields


class BaseElabObjManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int = 1
    id: str


class BaseElabObjSpecManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str
    tags: Optional[Sequence[str]] = None
    body: Optional[str] = None


class ItemsTypeSpecManifest(BaseElabObjSpecManifest):
    color: Optional[str] = None
    extra_fields: Optional[ExtraFieldsManifest] = None

    def get_dependencies(self) -> set[Node]:
        if self.extra_fields is not None:
            return self.extra_fields.get_dependencies()
        else:
            return set()


class ItemsTypeSpecManifestNestedMetadata(BaseElabObjSpecManifest):
    color: Optional[str] = None
    extra_fields: NestedExtraFieldsManifest

    def to_full_representation(self) -> ItemsTypeSpecManifest:
        data = self.model_dump()
        data["extra_fields"] = self.extra_fields.to_full_representation()
        return ItemsTypeSpecManifest(**data)


class ItemsTypeManifest(BaseElabObjManifest):
    kind: Literal["items_type"] = "items_type"
    spec: ItemsTypeSpecManifest | ItemsTypeSpecManifestNestedMetadata

    def render_spec(self) -> ItemsTypeSpecManifest:
        if isinstance(self.spec, ItemsTypeSpecManifestNestedMetadata):
            return self.spec.to_full_representation()
        else:
            return self.spec


class ExperimentTemplateSpecManifest(BaseElabObjSpecManifest):
    extra_fields: Optional[ExtraFieldsManifest] = None

    def get_dependencies(self) -> set[Node]:
        if self.extra_fields is not None:
            return self.extra_fields.get_dependencies()
        else:
            return set()


class ExperimentTemplateSpecManifestNestedMetadata(BaseElabObjSpecManifest):
    extra_fields: NestedExtraFieldsManifest

    def to_full_representation(self) -> ExperimentTemplateSpecManifest:
        data = self.model_dump()
        data["extra_fields"] = self.extra_fields.to_full_representation()
        return ExperimentTemplateSpecManifest(**data)


class ExperimentTemplateManifest(BaseElabObjManifest):
    kind: Literal["experiments_template"] = "experiments_template"
    spec: ExperimentTemplateSpecManifest | ExperimentTemplateSpecManifestNestedMetadata

    def render_spec(self) -> ExperimentTemplateSpecManifest:
        if isinstance(self.spec, ExperimentTemplateSpecManifestNestedMetadata):
            return self.spec.to_full_representation()
        else:
            return self.spec


class ItemSpecManifest(BaseElabObjSpecManifest):
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


class ItemSpecManifestNestedMetadata(BaseElabObjSpecManifest):
    category: str
    color: Optional[str] = None
    extra_fields: NestedExtraFieldsManifest

    def to_full_representation(self) -> ItemSpecManifest:
        data = self.model_dump()
        data["extra_fields"] = self.extra_fields.to_full_representation()
        return ItemSpecManifest(**data)


class ItemSpecManifestSimplifiedMetadata(BaseElabObjSpecManifest):
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
    kind: Literal["item"] = "item"
    spec: ItemSpecManifest | ItemSpecManifestNestedMetadata | ItemSpecManifestSimplifiedMetadata

    def render_spec(self, parent: ItemsTypeSpecManifest) -> ItemSpecManifest:
        if isinstance(self.spec, ItemSpecManifestSimplifiedMetadata):
            return self.spec.to_full_representation(parent)
        elif isinstance(self.spec, ItemSpecManifestNestedMetadata):
            return self.spec.to_full_representation()
        else:
            return self.spec


class ExperimentSpecManifest(BaseElabObjSpecManifest):
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


class ExperimentSpecManifestNestedMetadata(BaseElabObjSpecManifest):
    template: Optional[str] = None
    extra_fields: NestedExtraFieldsManifest

    def to_full_representation(self) -> ExperimentSpecManifest:
        data = self.model_dump()
        data["extra_fields"] = self.extra_fields.to_full_representation()
        return ExperimentSpecManifest(**data)


class ExperimentSpecManifestSimplifiedMetadata(BaseElabObjSpecManifest):
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
    kind: Literal["experiment"] = "experiment"
    spec: ExperimentSpecManifest | ExperimentSpecManifestNestedMetadata | ExperimentSpecManifestSimplifiedMetadata

    def render_spec(
        self,
        parent: Optional[ExperimentTemplateSpecManifest],
    ) -> ExperimentSpecManifest:
        if isinstance(self.spec, ExperimentSpecManifestSimplifiedMetadata):
            if parent is None:
                raise ValueError(f"Template needed for experiment '{self.id}'")
            return self.spec.to_full_representation(parent)
        elif isinstance(self.spec, ExperimentSpecManifestNestedMetadata):
            return self.spec.to_full_representation()
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
    def get_dependencies(self) -> set[Node]:
        ...


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
            items_type_spec = items_type.render_spec()
            items_types_spec[node.id] = items_type_spec
            cls._add_dependencies_to_graph(node, items_type_spec, dependency_graph)

        experiment_templates_spec: dict[str, ExperimentTemplateSpecManifest] = {}

        for node, experiment_template in experiment_templates.items():
            experiment_template_spec = experiment_template.render_spec()
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
