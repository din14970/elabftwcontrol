from contextlib import nullcontext
from typing import Any, ContextManager, Optional, Type

import pytest
import yaml
from pydantic import ValidationError

from elabftwcontrol.core.manifests import (
    BaseMetaField,
    DependencyGraph,
    ElabObjManifests,
    ExperimentManifest,
    ExperimentSpecManifest,
    ExperimentSpecManifestSimplifiedMetadata,
    ExperimentTemplateManifest,
    ExperimentTemplateSpecManifest,
    ExtraFields,
    ExtraFieldsManifest,
    FieldTypeEnum,
    ItemManifest,
    ItemSpecManifest,
    ItemSpecManifestSimplifiedMetadata,
    ItemsTypeManifest,
    ItemsTypeSpecManifest,
    ManifestIndex,
    MetadataCheckboxFieldManifest,
    MetadataCheckboxFieldOptions,
    MetadataDateFieldManifest,
    MetadataDatetimeFieldManifest,
    MetadataEmailFieldManifest,
    MetadataExperimentsLinkFieldManifest,
    MetadataFieldManifest,
    MetadataGroupManifest,
    MetadataItemsLinkFieldManifest,
    MetadataManifestConfig,
    MetadataNumberFieldManifest,
    MetadataRadioFieldManifest,
    MetadataSelectFieldManifest,
    MetadataTextFieldManifest,
    MetadataTimeFieldManifest,
    MetadataURLFieldManifest,
    Node,
    ObjectTypes,
    SimpleExtraFieldsManifest,
    _ValueAndUnit,
)


@pytest.mark.parametrize(
    ("input", "field_type", "expected", "expectation"),
    (
        (
            {"group": 1},
            BaseMetaField,
            None,
            pytest.raises(ValidationError),
        ),
        (
            {"name": "fieldname", "group": "groupname"},
            BaseMetaField,
            BaseMetaField(
                name="fieldname",
                group="groupname",
            ),
            nullcontext(),
        ),
        (
            {"name": "fieldname", "non_existing": 1},
            BaseMetaField,
            None,
            pytest.raises(ValidationError),
        ),
        (
            {"type": "checkbox", "name": "fieldname", "value": "on"},
            MetadataCheckboxFieldManifest,
            MetadataCheckboxFieldManifest(
                type=FieldTypeEnum.checkbox,
                name="fieldname",
                value=MetadataCheckboxFieldOptions("on"),
            ),
            nullcontext(),
        ),
        (
            {"type": "checkbox", "name": "fieldname", "value": "off"},
            MetadataCheckboxFieldManifest,
            MetadataCheckboxFieldManifest(
                type=FieldTypeEnum.checkbox,
                name="fieldname",
                value=MetadataCheckboxFieldOptions("off"),
            ),
            nullcontext(),
        ),
        (
            {"type": "checkbox", "name": "fieldname", "value": "something_else"},
            MetadataCheckboxFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {"type": "wrongtype", "name": "fieldname", "value": "on"},
            MetadataCheckboxFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "radio",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
                "value": "option 1",
            },
            MetadataRadioFieldManifest,
            MetadataRadioFieldManifest(
                type=FieldTypeEnum.radio,
                name="fieldname",
                value="option 1",
                options=["option 1", "option 2"],
            ),
            nullcontext(),
        ),
        (
            {
                "type": "radio",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
                "value": "option 3",
            },
            MetadataRadioFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "radio",
                "name": "fieldname",
                "value": "option 3",
            },
            MetadataRadioFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "radio",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
            },
            MetadataRadioFieldManifest,
            MetadataRadioFieldManifest(
                type=FieldTypeEnum.radio,
                name="fieldname",
                value="option 1",
                options=["option 1", "option 2"],
            ),
            nullcontext(),
        ),
        (
            {
                "type": "select",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
                "value": "option 1",
            },
            MetadataSelectFieldManifest,
            MetadataSelectFieldManifest(
                type=FieldTypeEnum.select,
                name="fieldname",
                value="option 1",
                options=["option 1", "option 2"],
            ),
            nullcontext(),
        ),
        (
            {
                "type": "select",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
                "value": ["option 1"],
                "allow_multi_values": True,
            },
            MetadataSelectFieldManifest,
            MetadataSelectFieldManifest(
                type=FieldTypeEnum.select,
                name="fieldname",
                value=["option 1"],
                options=["option 1", "option 2"],
                allow_multi_values=True,
            ),
            nullcontext(),
        ),
        (
            {
                "type": "select",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
                "value": ["option 1", "option 2"],
                "allow_multi_values": True,
            },
            MetadataSelectFieldManifest,
            MetadataSelectFieldManifest(
                type=FieldTypeEnum.select,
                name="fieldname",
                value=["option 1", "option 2"],
                options=["option 1", "option 2"],
                allow_multi_values=True,
            ),
            nullcontext(),
        ),
        (
            {
                "type": "select",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
                "value": "option 3",
            },
            MetadataSelectFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "select",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
                "value": "option 1",
                "allow_multi_values": True,
            },
            MetadataSelectFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "select",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
                "value": ["option 3"],
                "allow_multi_values": True,
            },
            MetadataSelectFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "select",
                "name": "fieldname",
                "value": "option 3",
            },
            MetadataSelectFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "select",
                "name": "fieldname",
                "options": ["option 1", "option 2"],
            },
            MetadataSelectFieldManifest,
            MetadataSelectFieldManifest(
                type=FieldTypeEnum.select,
                name="fieldname",
                value="option 1",
                options=["option 1", "option 2"],
            ),
            nullcontext(),
        ),
        (
            {
                "type": "number",
                "name": "fieldname",
                "value": "1234.1",
            },
            MetadataNumberFieldManifest,
            MetadataNumberFieldManifest(
                type=FieldTypeEnum.number,
                name="fieldname",
                value="1234.1",
            ),
            nullcontext(),
        ),
        (
            {
                "type": "number",
                "name": "fieldname",
            },
            MetadataNumberFieldManifest,
            MetadataNumberFieldManifest(
                type=FieldTypeEnum.number,
                name="fieldname",
            ),
            nullcontext(),
        ),
        (
            {
                "type": "number",
                "name": "fieldname",
                "value": "1234.1",
                "unit": "kg",
                "units": ["g", "kg"],
            },
            MetadataNumberFieldManifest,
            MetadataNumberFieldManifest(
                type=FieldTypeEnum.number,
                name="fieldname",
                value="1234.1",
                unit="kg",
                units=["g", "kg"],
            ),
            nullcontext(),
        ),
        (
            {
                "type": "number",
                "name": "fieldname",
                "value": "1234.1",
                "units": ["g", "kg"],
            },
            MetadataNumberFieldManifest,
            MetadataNumberFieldManifest(
                type=FieldTypeEnum.number,
                name="fieldname",
                value="1234.1",
                unit=None,
                units=["g", "kg"],
            ),
            nullcontext(),
        ),
        (
            {
                "type": "number",
                "name": "fieldname",
                "value": "1234.1",
                "unit": "ton",
                "units": ["g", "kg"],
            },
            MetadataNumberFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "number",
                "name": "fieldname",
                "value": "not a number",
            },
            MetadataNumberFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "number",
                "name": "fieldname",
                "unit": "ton",
            },
            MetadataNumberFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "email",
                "name": "fieldname",
                "value": "myname@email.com",
            },
            MetadataEmailFieldManifest,
            MetadataEmailFieldManifest(
                type=FieldTypeEnum.email,
                name="fieldname",
                value="myname@email.com",
            ),
            nullcontext(),
        ),
        (
            {
                "type": "email",
                "name": "fieldname",
                "value": "notanemail",
            },
            MetadataEmailFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "url",
                "name": "fieldname",
                "value": "https://some.url",
            },
            MetadataURLFieldManifest,
            MetadataURLFieldManifest(
                type=FieldTypeEnum.url,
                name="fieldname",
                value="https://some.url",
            ),
            nullcontext(),
        ),
        (
            {
                "type": "url",
                "name": "fieldname",
                "value": "http://some.url",
            },
            MetadataURLFieldManifest,
            MetadataURLFieldManifest(
                type=FieldTypeEnum.url,
                name="fieldname",
                value="http://some.url",
            ),
            nullcontext(),
        ),
        (
            {
                "type": "url",
                "name": "fieldname",
                "value": "notaurl.com",
            },
            MetadataURLFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "date",
                "name": "fieldname",
                "value": "2024-05-28",
            },
            MetadataDateFieldManifest,
            MetadataDateFieldManifest(
                type=FieldTypeEnum.date,
                name="fieldname",
                value="2024-05-28",
            ),
            nullcontext(),
        ),
        (
            {
                "type": "date",
                "name": "fieldname",
                "value": "1234-56-78",
            },
            MetadataDateFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "datetime-local",
                "name": "fieldname",
                "value": "2024-05-28T15:07",
            },
            MetadataDatetimeFieldManifest,
            MetadataDatetimeFieldManifest(
                type=FieldTypeEnum.datetime_local,
                name="fieldname",
                value="2024-05-28T15:07",
            ),
            nullcontext(),
        ),
        (
            {
                "type": "datetime-local",
                "name": "fieldname",
                "value": "2024-05-28T99:99",
            },
            MetadataDatetimeFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
        (
            {
                "type": "time",
                "name": "fieldname",
                "value": "15:07",
            },
            MetadataTimeFieldManifest,
            MetadataTimeFieldManifest(
                type=FieldTypeEnum.time,
                name="fieldname",
                value="15:07",
            ),
            nullcontext(),
        ),
        (
            {
                "type": "time",
                "name": "fieldname",
                "value": "99:99",
            },
            MetadataTimeFieldManifest,
            None,
            pytest.raises(ValueError),
        ),
    ),
)
def test_meta_field(
    input: dict[str, Any],
    field_type: Type[BaseMetaField],
    expected: Optional[MetadataFieldManifest],
    expectation: ContextManager,
) -> None:
    with expectation:
        result = field_type(**input)
        assert result == expected


def test_meta_field_direct_assignment() -> None:
    field = MetadataSelectFieldManifest(
        name="field",
        options=[
            "option 1",
            "option 2",
        ],
        value="option 1",
    )
    field.value = "option 2"
    assert field.value == "option 2"

    with pytest.raises(ValueError):
        field.value = "bad option"


def test_meta_field_non_existing_assignment() -> None:
    field = MetadataSelectFieldManifest(
        name="field",
        options=[
            "option 1",
            "option 2",
        ],
        value="option 1",
    )
    with pytest.raises(AttributeError):
        field.unit == "not an option"  # type: ignore


def test_group_manifest() -> None:
    data = """\
group_name: group 1
sub_fields:
- name: field 1 group 1
  type: select
  value: option 1
  options:
   - option 1
   - option 2
- name: field 2 group 1
  type: number
"""
    parsed = yaml.safe_load(data)
    verified = MetadataGroupManifest(**parsed)
    expected = MetadataGroupManifest(
        group_name="group 1",
        sub_fields=[
            MetadataSelectFieldManifest(
                name="field 1 group 1",
                value="option 1",
                type=FieldTypeEnum.select,
                options=["option 1", "option 2"],
            ),
            MetadataNumberFieldManifest(
                name="field 2 group 1",
                type=FieldTypeEnum.number,
            ),
        ],
    )
    assert verified == expected


def test_group_manifest_non_existing_subtype() -> None:
    data = """\
group_name: group 1
sub_fields:
- name: field
  type: nonexisting
"""
    parsed = yaml.safe_load(data)
    with pytest.raises(ValueError):
        MetadataGroupManifest(**parsed)


class TestExtraFieldsManifest:
    data = """\
config:
    display_main_text: false
fields:
  - name: field 1 group 1
    type: select
    value: option 1
    group: group 1
    options:
     - option 1
     - option 2
  - name: field 2 group 1
    type: number
    group: group 1
  - name: field 1 group 2
    type: text
    group: group 2
  - name: field 1 ungrouped
    type: text
"""
    parsed = yaml.safe_load(data)

    def test_extra_fields_manifest(self) -> None:
        verified = ExtraFieldsManifest(**self.parsed)
        expected = ExtraFieldsManifest(
            config=MetadataManifestConfig(
                display_main_text=False,
            ),
            fields=[
                MetadataSelectFieldManifest(
                    name="field 1 group 1",
                    value="option 1",
                    type=FieldTypeEnum.select,
                    group="group 1",
                    options=["option 1", "option 2"],
                ),
                MetadataNumberFieldManifest(
                    name="field 2 group 1",
                    type=FieldTypeEnum.number,
                    group="group 1",
                ),
                MetadataTextFieldManifest(
                    name="field 1 group 2",
                    group="group 2",
                    type=FieldTypeEnum.text,
                ),
                MetadataTextFieldManifest(
                    name="field 1 ungrouped",
                    type=FieldTypeEnum.text,
                ),
            ],
        )
        assert verified == expected

    def test_extra_fields_get_item(self) -> None:
        loaded = ExtraFieldsManifest(**self.parsed).to_full_representation()
        assert loaded["field 1 group 1"] == MetadataSelectFieldManifest(
            name="field 1 group 1",
            value="option 1",
            type=FieldTypeEnum.select,
            options=["option 1", "option 2"],
            group="group 1",
        )

    def test_extra_fields_empty_manifest(self) -> None:
        verified = ExtraFieldsManifest(**{})
        expected = ExtraFieldsManifest()
        assert verified == expected

    def test_extra_fields_manifest_fail_duplicate_field(self) -> None:
        data = """\
config:
    display_main_text: false
fields:
  - name: duplicated
    type: select
    value: option 1
    group: group 1
    options:
     - option 1
     - option 2
  - name: field 2 group 1
    type: number
    group: group 1
  - name: field 1 group 2
    type: text
    group: group 2
  - name: duplicated
    type: text
"""
        parsed = yaml.safe_load(data)
        with pytest.raises(ValueError):
            ExtraFieldsManifest(**parsed).to_full_representation()

    def test_extra_fields_manifest_iter(self) -> None:
        manifest = ExtraFieldsManifest(**self.parsed).to_full_representation()
        result = [(field.group, field.name) for field in manifest.fields]
        expected = [
            ("group 1", "field 1 group 1"),
            ("group 1", "field 2 group 1"),
            ("group 2", "field 1 group 2"),
            (None, "field 1 ungrouped"),
        ]
        assert result == expected

    linked_data = """\
fields:
  - name: link 1
    type: experiments
    value: link to experiment
    group: group 1
  - name: field 2 group 1
    type: items
    group: group 1
  - name: field 1 group 2
    type: text
  - name: field 3
    type: items
    value: link to item
  - name: field 4
    type: checkbox
  - name: field 5
    type: experiments
  - name: field 6
    type: experiments
    value: link to experiment
"""
    parsed_linked = yaml.safe_load(linked_data)

    def test_extra_fields_get_dependencies(self) -> None:
        manifest = ExtraFieldsManifest(**self.parsed_linked)
        result = manifest.get_dependencies()
        expected = set(
            [
                Node(ObjectTypes.EXPERIMENT, "link to experiment"),
                Node(ObjectTypes.ITEM, "link to item"),
            ]
        )
        assert result == expected

    def test_extra_fields_get_field_names(self) -> None:
        extra_fields = ExtraFields.parse(**self.parsed_linked)
        result = extra_fields.field_names
        expected = [
            "link 1",
            "field 2 group 1",
            "field 1 group 2",
            "field 3",
            "field 4",
            "field 5",
            "field 6",
        ]
        assert result == expected

    nested_data = """\
config:
    display_main_text: false
fields:
  - group_name: group 1
    sub_fields:
    - name: field 1 group 1
      type: select
      value: option 1
      options:
       - option 1
       - option 2
    - name: field 2 group 1
      type: number
      group: group 1
  - name: field 1 group 2
    type: text
    group: group 2
  - name: field 1 ungrouped
    type: text
"""
    parsed_nested = yaml.safe_load(nested_data)

    def test_nested_extra_fields_manifest(self) -> None:
        verified = ExtraFieldsManifest(**self.parsed_nested)
        expected = ExtraFieldsManifest(
            config=MetadataManifestConfig(
                display_main_text=False,
            ),
            fields=[
                MetadataGroupManifest(
                    group_name="group 1",
                    sub_fields=[
                        MetadataSelectFieldManifest(
                            name="field 1 group 1",
                            value="option 1",
                            type=FieldTypeEnum.select,
                            options=["option 1", "option 2"],
                        ),
                        MetadataNumberFieldManifest(
                            name="field 2 group 1",
                            type=FieldTypeEnum.number,
                            group="group 1",
                        ),
                    ],
                ),
                MetadataTextFieldManifest(
                    name="field 1 group 2",
                    group="group 2",
                    type=FieldTypeEnum.text,
                ),
                MetadataTextFieldManifest(
                    name="field 1 ungrouped",
                    type=FieldTypeEnum.text,
                ),
            ],
        )
        assert verified == expected

    def test_nested_extra_fields_manifest_to_full_repr_wrong_group(self) -> None:
        data = """\
fields:
  - group_name: group 1
    sub_fields:
    - name: field 1 group 1
      type: select
      value: option 1
      options:
       - option 1
       - option 2
    - name: field 2 group 1
      type: number
      group: group 2
  - name: field 1 group 2
    type: text
    group: group 2
"""
        parsed = yaml.safe_load(data)
        validated = ExtraFieldsManifest(**parsed)
        with pytest.raises(ValueError):
            validated.to_full_representation()

    simple_data = """\
config:
    display_main_text: false
field_values:
    field 1: value field 1
    field 2: value field 2
    field 3:
        value: value field 3
    field 4:
        value: 4
        unit: kg
    field 6:
        value:
            - option 1
            - option 3
    field 7:
        - option 1
        - option 2
"""
    parsed_simple_data = yaml.safe_load(simple_data)

    def test_simple_extra_field_manifest(self) -> None:
        manifest = SimpleExtraFieldsManifest(**self.parsed_simple_data)
        expected = SimpleExtraFieldsManifest(
            config=MetadataManifestConfig(
                display_main_text=False,
            ),
            field_values={
                "field 1": "value field 1",
                "field 2": "value field 2",
                "field 3": _ValueAndUnit(value="value field 3"),
                "field 4": _ValueAndUnit(value="4", unit="kg"),
                "field 6": _ValueAndUnit(value=["option 1", "option 3"]),
                "field 7": ["option 1", "option 2"],
            },
        )
        assert manifest == expected

    parent_simple = ExtraFieldsManifest(
        fields=[
            MetadataTextFieldManifest(
                name="field 1",
                value="old field 1",
            ),
            MetadataExperimentsLinkFieldManifest(
                name="field 2",
                value="old field 2",
            ),
            MetadataItemsLinkFieldManifest(
                name="field 3",
                value="old field 3",
            ),
            MetadataNumberFieldManifest(
                name="field 4",
                unit="g",
                units=["kg", "g", "ton"],
            ),
            MetadataTextFieldManifest(
                name="field 5",
                value="old field 5",
            ),
            MetadataSelectFieldManifest(
                name="field 6",
                value=[],
                options=[
                    "option 1",
                    "option 2",
                    "option 3",
                ],
                allow_multi_values=True,
            ),
            MetadataSelectFieldManifest(
                name="field 7",
                value=[],
                options=[
                    "option 1",
                    "option 2",
                    "option 3",
                ],
                allow_multi_values=True,
            ),
        ]
    )

    def test_simple_extra_field_manifest_conversion(self) -> None:
        simple = SimpleExtraFieldsManifest(**self.parsed_simple_data)
        full = simple.to_full_representation(self.parent_simple)
        expected = ExtraFieldsManifest(
            config=MetadataManifestConfig(
                display_main_text=False,
            ),
            fields=[
                MetadataTextFieldManifest(
                    name="field 1",
                    value="value field 1",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 2",
                    value="value field 2",
                ),
                MetadataItemsLinkFieldManifest(
                    name="field 3",
                    value="value field 3",
                ),
                MetadataNumberFieldManifest(
                    name="field 4",
                    value="4",
                    unit="kg",
                    units=["kg", "g", "ton"],
                ),
                MetadataTextFieldManifest(
                    name="field 5",
                    value="old field 5",
                ),
                MetadataSelectFieldManifest(
                    name="field 6",
                    value=["option 1", "option 3"],
                    options=[
                        "option 1",
                        "option 2",
                        "option 3",
                    ],
                    allow_multi_values=True,
                ),
                MetadataSelectFieldManifest(
                    name="field 7",
                    value=["option 1", "option 2"],
                    options=[
                        "option 1",
                        "option 2",
                        "option 3",
                    ],
                    allow_multi_values=True,
                ),
            ],
        )
        assert full == expected

    def test_simple_extra_field_manifest_conversion_bad_key(self) -> None:
        simple_data = """\
config:
    display_main_text: false
field_values:
    field 1: value field 1
    field 2: value field 2
    field 3:
        value: value field 3
    field 4:
        value: 4
        unit: kg
    field 6:
        value:
            - option 1
            - option 3
    non_existing_field:
        - option 1
        - option 2
"""
        parsed_simple_data = yaml.safe_load(simple_data)
        simple = SimpleExtraFieldsManifest(**parsed_simple_data)
        with pytest.raises(ValueError):
            simple.to_full_representation(self.parent_simple)


def test_items_type_manifest() -> None:
    data = """\
version: 1
id: items type test
kind: items_type
spec:
    title: test item
    tags:
        - test tag 1
        - test tag 2
    body: something
    color: "#123456"
    extra_fields:
        fields:
          - name: field 1
            type: text
          - name: field 2
            type: items
            value: link to item
"""
    parsed = yaml.safe_load(data)
    manifest = ItemsTypeManifest(**parsed)
    expected = ItemsTypeManifest(
        version=1,
        id="items type test",
        kind=ObjectTypes.ITEMS_TYPE,
        spec=ItemsTypeSpecManifest(
            title="test item",
            tags=["test tag 1", "test tag 2"],
            body="something",
            color="#123456",
            extra_fields=ExtraFieldsManifest(
                fields=[
                    MetadataTextFieldManifest(name="field 1"),
                    MetadataItemsLinkFieldManifest(
                        name="field 2",
                        value="link to item",
                    ),
                ]
            ),
        ),
    )
    assert manifest == expected


def test_items_type_manifest_nested_extra_fields() -> None:
    data = """\
version: 1
id: items type test
kind: items_type
spec:
    title: test item
    tags:
        - test tag 1
        - test tag 2
    body: something
    color: "#123456"
    extra_fields:
        fields:
          - group_name: group 1
            sub_fields:
              - name: field 1
                type: text
          - name: field 2
            type: items
            value: link to item
"""
    parsed = yaml.safe_load(data)
    manifest = ItemsTypeManifest(**parsed)
    expected = ItemsTypeManifest(
        version=1,
        id="items type test",
        kind=ObjectTypes.ITEMS_TYPE,
        spec=ItemsTypeSpecManifest(
            title="test item",
            tags=["test tag 1", "test tag 2"],
            body="something",
            color="#123456",
            extra_fields=ExtraFieldsManifest(
                fields=[
                    MetadataGroupManifest(
                        group_name="group 1",
                        sub_fields=[
                            MetadataTextFieldManifest(name="field 1"),
                        ],
                    ),
                    MetadataItemsLinkFieldManifest(
                        name="field 2",
                        value="link to item",
                    ),
                ]
            ),
        ),
    )
    assert manifest == expected


def test_items_type_spec_get_dependencies() -> None:
    spec = ItemsTypeSpecManifest(
        title="test",
        extra_fields=ExtraFieldsManifest(
            fields=[
                MetadataTextFieldManifest(name="field 1"),
                MetadataItemsLinkFieldManifest(
                    name="field 2",
                    value="link to item",
                ),
                MetadataItemsLinkFieldManifest(
                    name="field 3",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 4",
                    value="link to experiment",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 5",
                ),
            ]
        ),
    )
    result = spec.get_dependencies()
    expected = set(
        (
            Node(kind=ObjectTypes.ITEM, name="link to item"),
            Node(kind=ObjectTypes.EXPERIMENT, name="link to experiment"),
        )
    )
    assert result == expected


def test_experiments_template_spec_manifest() -> None:
    data = """\
version: 1
id: experiment template test
kind: experiments_template
spec:
    title: test experiment template
    tags:
        - test tag 1
        - test tag 2
    body: something
    extra_fields:
        fields:
          - name: field 1
            type: text
          - name: field 2
            type: items
            value: link to item
"""
    parsed = yaml.safe_load(data)
    manifest = ExperimentTemplateManifest(**parsed)
    expected = ExperimentTemplateManifest(
        version=1,
        id="experiment template test",
        spec=ExperimentTemplateSpecManifest(
            title="test experiment template",
            tags=["test tag 1", "test tag 2"],
            body="something",
            extra_fields=ExtraFieldsManifest(
                fields=[
                    MetadataTextFieldManifest(name="field 1"),
                    MetadataItemsLinkFieldManifest(
                        name="field 2",
                        value="link to item",
                    ),
                ]
            ),
        ),
    )
    assert manifest == expected


def test_experiment_template_get_dependencies() -> None:
    spec = ExperimentTemplateSpecManifest(
        title="test",
        extra_fields=ExtraFieldsManifest(
            fields=[
                MetadataTextFieldManifest(name="field 1"),
                MetadataItemsLinkFieldManifest(
                    name="field 2",
                    value="link to item",
                ),
                MetadataItemsLinkFieldManifest(
                    name="field 3",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 4",
                    value="link to experiment",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 5",
                ),
            ]
        ),
    )
    result = spec.get_dependencies()
    expected = set(
        (
            Node(kind=ObjectTypes.ITEM, name="link to item"),
            Node(kind=ObjectTypes.EXPERIMENT, name="link to experiment"),
        )
    )
    assert result == expected


def test_experiment_manifest_simple_metadata() -> None:
    data = """\
version: 1
id: experiment test
kind: experiment
spec:
    title: test experiment
    template: experiment template 1
    tags:
        - test tag 1
        - test tag 2
    body: something
    extra_fields:
        field_values:
            field 1: some value
            field 2: item x
"""
    parsed = yaml.safe_load(data)
    manifest = ExperimentManifest(**parsed)
    expected = ExperimentManifest(
        version=1,
        id="experiment test",
        spec=ExperimentSpecManifestSimplifiedMetadata(
            title="test experiment",
            template="experiment template 1",
            tags=["test tag 1", "test tag 2"],
            body="something",
            extra_fields=SimpleExtraFieldsManifest(
                field_values={
                    "field 1": "some value",
                    "field 2": "item x",
                }
            ),
        ),
    )
    assert manifest == expected


def test_experiment_manifest_simple_metadata_get_dependencies() -> None:
    parent_spec = ExperimentTemplateSpecManifest(
        title="test",
        extra_fields=ExtraFieldsManifest(
            fields=[
                MetadataItemsLinkFieldManifest(
                    name="field 1",
                    value="link to item from template field 1",
                ),
                MetadataItemsLinkFieldManifest(
                    name="field 2",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 3",
                    value="link to experiment from template field 3",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 4",
                ),
                MetadataTextFieldManifest(
                    name="field 5",
                ),
            ]
        ),
    )
    experiment_spec = ExperimentSpecManifestSimplifiedMetadata(
        title="test experiment",
        template="experiment template 1",
        extra_fields=SimpleExtraFieldsManifest(
            field_values={
                "field 1": "link to item from experiment field 1",
                "field 2": "link to item from experiment field 2",
                "field 4": "link to experiment from experiment field 4",
                "field 5": "some value",
            }
        ),
    )
    rendered_spec = experiment_spec.to_full_representation(parent_spec)
    result = rendered_spec.get_dependencies()
    expected = set(
        (
            Node(kind=ObjectTypes.EXPERIMENTS_TEMPLATE, name="experiment template 1"),
            Node(kind=ObjectTypes.ITEM, name="link to item from experiment field 1"),
            Node(kind=ObjectTypes.ITEM, name="link to item from experiment field 2"),
            Node(
                kind=ObjectTypes.EXPERIMENT,
                name="link to experiment from template field 3",
            ),
            Node(
                kind=ObjectTypes.EXPERIMENT,
                name="link to experiment from experiment field 4",
            ),
        )
    )
    assert result == expected


def test_experiment_manifest_nested_metadata() -> None:
    data = """\
version: 1
id: experiment test
kind: experiment
spec:
    title: test experiment
    template: experiment template 1
    tags:
        - test tag 1
        - test tag 2
    body: something
    extra_fields:
        fields:
        - group_name: group 1
          sub_fields:
            - name: field 1
              type: text
              value: some value
        - name: field 2
          type: items
          value: item x
"""
    parsed = yaml.safe_load(data)
    manifest = ExperimentManifest(**parsed)
    expected = ExperimentManifest(
        version=1,
        id="experiment test",
        spec=ExperimentSpecManifest(
            title="test experiment",
            template="experiment template 1",
            tags=["test tag 1", "test tag 2"],
            body="something",
            extra_fields=ExtraFieldsManifest(
                fields=[
                    MetadataGroupManifest(
                        group_name="group 1",
                        sub_fields=[
                            MetadataTextFieldManifest(
                                name="field 1",
                                type=FieldTypeEnum.text,
                                value="some value",
                            ),
                        ],
                    ),
                    MetadataItemsLinkFieldManifest(
                        name="field 2",
                        type=FieldTypeEnum.items,
                        value="item x",
                    ),
                ]
            ),
        ),
    )
    assert manifest == expected


def test_experiment_spec_nested_metadata_get_dependencies() -> None:
    spec = ExperimentSpecManifest(
        title="test",
        extra_fields=ExtraFieldsManifest(
            fields=[
                MetadataTextFieldManifest(name="field 1"),
                MetadataItemsLinkFieldManifest(
                    name="field 2",
                    value="link to item",
                ),
                MetadataItemsLinkFieldManifest(
                    name="field 3",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 4",
                    value="link to experiment",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 5",
                ),
            ]
        ),
    )
    result = spec.get_dependencies()
    expected = set(
        (
            Node(kind=ObjectTypes.ITEM, name="link to item"),
            Node(kind=ObjectTypes.EXPERIMENT, name="link to experiment"),
        )
    )
    assert result == expected


def test_experiment_manifest_fail() -> None:
    data = """\
version: 1
id: experiment test
kind: experiment
spec:
    title: test experiment
    extra_fields:
        field_values:
            field 1: some value
            field 2: item x
"""
    parsed = yaml.safe_load(data)
    with pytest.raises(ValueError):
        ExperimentManifest(**parsed)


def test_item_manifest() -> None:
    data = """\
version: 1
id: item test
kind: item
spec:
    title: test item
    category: item type 1
    tags:
        - test tag 1
        - test tag 2
    body: something
    extra_fields:
        field_values:
            field 1: some value
            field 2: item x
"""
    parsed = yaml.safe_load(data)
    manifest = ItemManifest(**parsed)
    expected = ItemManifest(
        version=1,
        id="item test",
        spec=ItemSpecManifestSimplifiedMetadata(
            title="test item",
            category="item type 1",
            tags=["test tag 1", "test tag 2"],
            body="something",
            extra_fields=SimpleExtraFieldsManifest(
                field_values={
                    "field 1": "some value",
                    "field 2": "item x",
                }
            ),
        ),
    )
    assert manifest == expected


def test_item_complex_metadata_get_dependencies() -> None:
    spec = ItemSpecManifest(
        title="test",
        category="item template",
        extra_fields=ExtraFieldsManifest(
            fields=[
                MetadataTextFieldManifest(name="field 1"),
                MetadataItemsLinkFieldManifest(
                    name="field 2",
                    value="link to item",
                ),
                MetadataItemsLinkFieldManifest(
                    name="field 3",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 4",
                    value="link to experiment",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 5",
                ),
            ]
        ),
    )
    result = spec.get_dependencies()
    expected = set(
        (
            Node(kind=ObjectTypes.ITEMS_TYPE, name="item template"),
            Node(kind=ObjectTypes.ITEM, name="link to item"),
            Node(kind=ObjectTypes.EXPERIMENT, name="link to experiment"),
        )
    )
    assert result == expected


def test_item_manifest_simple_metadata_get_dependencies() -> None:
    parent_spec = ItemsTypeSpecManifest(
        title="test",
        extra_fields=ExtraFieldsManifest(
            fields=[
                MetadataItemsLinkFieldManifest(
                    name="field 1",
                    value="link to item from template field 1",
                ),
                MetadataItemsLinkFieldManifest(
                    name="field 2",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 3",
                    value="link to experiment from template field 3",
                ),
                MetadataExperimentsLinkFieldManifest(
                    name="field 4",
                ),
                MetadataTextFieldManifest(
                    name="field 5",
                ),
            ]
        ),
    )
    item_spec = ItemSpecManifestSimplifiedMetadata(
        title="test item",
        category="items type 1",
        extra_fields=SimpleExtraFieldsManifest(
            field_values={
                "field 1": "link to item from experiment field 1",
                "field 2": "link to item from experiment field 2",
                "field 4": "link to experiment from experiment field 4",
                "field 5": "some value",
            }
        ),
    )
    rendered = item_spec.to_full_representation(parent_spec)
    result = rendered.get_dependencies()
    expected = set(
        (
            Node(kind=ObjectTypes.ITEMS_TYPE, name="items type 1"),
            Node(kind=ObjectTypes.ITEM, name="link to item from experiment field 1"),
            Node(kind=ObjectTypes.ITEM, name="link to item from experiment field 2"),
            Node(
                kind=ObjectTypes.EXPERIMENT,
                name="link to experiment from template field 3",
            ),
            Node(
                kind=ObjectTypes.EXPERIMENT,
                name="link to experiment from experiment field 4",
            ),
        )
    )
    assert result == expected


class TestElabObjManifests:
    data = """\
- version: 1
  id: item 1
  kind: item
  spec:
      title: test item 1
      category: item type 1
      extra_fields:
          field_values:
              field 1: some value
              field 2: item 2

- version: 1
  id: item 2
  kind: item
  spec:
      title: test item 2
      category: item type 1

- version: 1
  id: experiment 1
  kind: experiment
  spec:
      title: test experiment 1

- version: 1
  id: experiment 2
  kind: experiment
  spec:
      title: test experiment 2
      template: experiment template 1

- version: 1
  id: item type 1
  kind: items_type
  spec:
      title: test item type
      extra_fields:
          fields:
          - name: field 1
            type: text
          - name: field 2
            type: items
          - name: field 3
            type: experiments
            value: experiment 1

- version: 1
  id: experiment template 1
  kind: experiments_template
  spec:
      title: test experiments template
      extra_fields:
          fields:
          - group_name: group 1
            sub_fields:
              - name: field 4
                type: text
              - name: field 5
                type: items
                value: item 1
          - name: field 6
            type: experiments
"""
    parsed_data = yaml.safe_load(data)

    def test_elabobjmanifests(self) -> None:
        manifest = ElabObjManifests(manifests=self.parsed_data)
        expected = ElabObjManifests(
            manifests=[
                ItemManifest(
                    version=1,
                    id="item 1",
                    kind=ObjectTypes.ITEM,
                    spec=ItemSpecManifestSimplifiedMetadata(
                        title="test item 1",
                        category="item type 1",
                        extra_fields=SimpleExtraFieldsManifest(
                            field_values={
                                "field 1": "some value",
                                "field 2": "item 2",
                            }
                        ),
                    ),
                ),
                ItemManifest(
                    version=1,
                    id="item 2",
                    kind=ObjectTypes.ITEM,
                    spec=ItemSpecManifest(
                        title="test item 2",
                        category="item type 1",
                        extra_fields=None,
                    ),
                ),
                ExperimentManifest(
                    version=1,
                    id="experiment 1",
                    kind=ObjectTypes.EXPERIMENT,
                    spec=ExperimentSpecManifest(title="test experiment 1"),
                ),
                ExperimentManifest(
                    version=1,
                    id="experiment 2",
                    kind=ObjectTypes.EXPERIMENT,
                    spec=ExperimentSpecManifest(
                        title="test experiment 2",
                        template="experiment template 1",
                    ),
                ),
                ItemsTypeManifest(
                    version=1,
                    id="item type 1",
                    kind=ObjectTypes.ITEMS_TYPE,
                    spec=ItemsTypeSpecManifest(
                        title="test item type",
                        extra_fields=ExtraFieldsManifest(
                            fields=[
                                MetadataTextFieldManifest(name="field 1"),
                                MetadataItemsLinkFieldManifest(name="field 2"),
                                MetadataExperimentsLinkFieldManifest(
                                    name="field 3",
                                    value="experiment 1",
                                ),
                            ]
                        ),
                    ),
                ),
                ExperimentTemplateManifest(
                    version=1,
                    id="experiment template 1",
                    kind=ObjectTypes.EXPERIMENTS_TEMPLATE,
                    spec=ExperimentTemplateSpecManifest(
                        title="test experiments template",
                        extra_fields=ExtraFieldsManifest(
                            fields=[
                                MetadataGroupManifest(
                                    group_name="group 1",
                                    sub_fields=[
                                        MetadataTextFieldManifest(name="field 4"),
                                        MetadataItemsLinkFieldManifest(
                                            name="field 5",
                                            value="item 1",
                                        ),
                                    ],
                                ),
                                MetadataExperimentsLinkFieldManifest(name="field 6"),
                            ]
                        ),
                    ),
                ),
            ]
        )
        assert manifest == expected


class TestManifestIndex:
    data = """\
- version: 1
  id: item 1
  kind: item
  spec:
      title: test item 1
      category: item type 1
      extra_fields:
          field_values:
              field 1: some value
              field 2: item 2

- version: 1
  id: item 2
  kind: item
  spec:
      title: test item 2
      category: item type 1

- version: 1
  id: experiment 1
  kind: experiment
  spec:
      title: test experiment 1

- version: 1
  id: experiment 2
  kind: experiment
  spec:
      title: test experiment 2
      template: experiment template 1

- version: 1
  id: item type 1
  kind: items_type
  spec:
      title: test item type
      extra_fields:
          fields:
          - name: field 1
            type: text
          - name: field 2
            type: items
          - name: field 3
            type: experiments
            value: experiment 1

- version: 1
  id: experiment template 1
  kind: experiments_template
  spec:
      title: test experiments template
      extra_fields:
          fields:
          - group_name: group 1
            sub_fields:
              - name: field 4
                type: text
              - name: field 5
                type: items
                value: item 1
          - name: field 6
            type: experiments
"""
    parsed_data = yaml.safe_load(data)

    def test_manifestindex_from_manifests(self) -> None:
        manifest = ElabObjManifests(manifests=self.parsed_data)
        index = ManifestIndex.from_manifests(manifest.manifests)
        expected = ManifestIndex(
            specs={
                Node(ObjectTypes.ITEM, "item 1"): ItemSpecManifest(
                    title="test item 1",
                    category="item type 1",
                    extra_fields=ExtraFieldsManifest(
                        fields=[
                            MetadataTextFieldManifest(
                                name="field 1",
                                value="some value",
                            ),
                            MetadataItemsLinkFieldManifest(
                                name="field 2",
                                value="item 2",
                            ),
                            MetadataExperimentsLinkFieldManifest(
                                name="field 3",
                                value="experiment 1",
                            ),
                        ]
                    ),
                ),
                Node(ObjectTypes.ITEM, "item 2"): ItemSpecManifest(
                    title="test item 2",
                    category="item type 1",
                    extra_fields=None,
                ),
                Node(ObjectTypes.EXPERIMENT, "experiment 1"): ExperimentSpecManifest(title="test experiment 1"),
                Node(ObjectTypes.EXPERIMENT, "experiment 2"): ExperimentSpecManifest(
                    title="test experiment 2",
                    template="experiment template 1",
                ),
                Node(ObjectTypes.ITEMS_TYPE, "item type 1"): ItemsTypeSpecManifest(
                    title="test item type",
                    extra_fields=ExtraFieldsManifest(
                        fields=[
                            MetadataTextFieldManifest(name="field 1"),
                            MetadataItemsLinkFieldManifest(name="field 2"),
                            MetadataExperimentsLinkFieldManifest(
                                name="field 3",
                                value="experiment 1",
                            ),
                        ]
                    ),
                ),
                Node(ObjectTypes.EXPERIMENTS_TEMPLATE, "experiment template 1"): ExperimentTemplateSpecManifest(
                    title="test experiments template",
                    extra_fields=ExtraFieldsManifest(
                        fields=[
                            MetadataGroupManifest(
                                group_name="group 1",
                                sub_fields=[
                                    MetadataTextFieldManifest(
                                        name="field 4",
                                    ),
                                    MetadataItemsLinkFieldManifest(
                                        name="field 5",
                                        value="item 1",
                                    ),
                                ],
                            ),
                            MetadataExperimentsLinkFieldManifest(name="field 6"),
                        ]
                    ),
                ),
            },
            dependency_graph=DependencyGraph(
                edges={
                    Node(kind=ObjectTypes.ITEM, name="item 1"): set(
                        [
                            Node(kind=ObjectTypes.ITEM, name="item 2"),
                            Node(kind=ObjectTypes.ITEMS_TYPE, name="item type 1"),
                            Node(kind=ObjectTypes.EXPERIMENT, name="experiment 1"),
                        ]
                    ),
                    Node(kind=ObjectTypes.ITEM, name="item 2"): set(
                        [
                            Node(kind=ObjectTypes.ITEMS_TYPE, name="item type 1"),
                        ]
                    ),
                    Node(kind=ObjectTypes.EXPERIMENT, name="experiment 1"): set(),
                    Node(kind=ObjectTypes.EXPERIMENT, name="experiment 2"): set(
                        [
                            Node(
                                kind=ObjectTypes.EXPERIMENTS_TEMPLATE,
                                name="experiment template 1",
                            ),
                        ]
                    ),
                    Node(kind=ObjectTypes.ITEMS_TYPE, name="item type 1"): set(
                        [
                            Node(kind=ObjectTypes.EXPERIMENT, name="experiment 1"),
                        ]
                    ),
                    Node(
                        kind=ObjectTypes.EXPERIMENTS_TEMPLATE,
                        name="experiment template 1",
                    ): set(
                        [
                            Node(kind=ObjectTypes.ITEM, name="item 1"),
                        ]
                    ),
                },
                flexible=False,
            ),
        )
        assert index == expected
