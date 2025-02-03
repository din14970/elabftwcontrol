from typing import Any

from elabftwcontrol.core.models import (
    ConfigMetadata,
    FieldTypeEnum,
    GroupModel,
    MetadataModel,
    SingleFieldModel,
)
from elabftwcontrol.core.parsers import MetadataParser, TagIdParser, TagParser


class TestMetadataParser:
    def test_metadata_parser(self) -> None:
        dummy_metadata = """\
{
    "elabftw": {
        "extra_fields_groups": [
            {"id": 1, "name": "g1"},
            {"id": 2, "name": "g2"},
            {"id": 3, "name": "g3"}
        ]
    },
    "extra_fields": {
        "f1": {
            "type": "number",
            "value": "2.3",
            "unit": "kg",
            "group_id": "1",
            "position": "3"
        },
        "k3": {
            "type": "date",
            "value": "2023-09-12",
            "group_id": "1",
            "position": "2"
        }
    }
}
"""
        parser = MetadataParser()
        model = parser(dummy_metadata)
        expected = MetadataModel(
            elabftw=ConfigMetadata(
                extra_fields_groups=[
                    GroupModel(id=1, name="g1"),
                    GroupModel(id=2, name="g2"),
                    GroupModel(id=3, name="g3"),
                ]
            ),
            extra_fields={
                "f1": SingleFieldModel(
                    type=FieldTypeEnum.number,
                    value="2.3",
                    unit="kg",
                    group_id=1,
                    position=3,
                ),
                "k3": SingleFieldModel(
                    type=FieldTypeEnum.date,
                    value="2023-09-12",
                    group_id=1,
                    position=2,
                ),
            },
        )
        assert model == expected

    def test_empty_metadata(self) -> None:
        parser = MetadataParser()
        model = parser(None)
        expected = MetadataModel()
        assert model == expected

    def test_json_error_metadata(self) -> None:
        json_with_errors = "some non json metadata"
        parser = MetadataParser()
        model = parser(json_with_errors)
        expected = MetadataModel()
        assert model == expected


class TestTagParser:
    def test_normal_tags(self) -> None:
        tags = "tag1|tag2|tag3"
        parser = TagParser()
        result = parser(tags)
        expected = ["tag1", "tag2", "tag3"]
        assert result == expected

    def test_none(self) -> None:
        tags = None
        parser = TagParser()
        result = parser(tags)
        assert result == []

    def test_incorrect(self) -> None:
        tags: Any = object()
        parser = TagParser()
        result = parser(tags)
        assert result == []


class TestTagIdParser:
    def test_normal_tags(self) -> None:
        tags = "123,234,2"
        parser = TagIdParser()
        result = parser(tags)
        expected = [123, 234, 2]
        assert result == expected

    def test_none(self) -> None:
        tags = None
        parser = TagIdParser()
        result = parser(tags)
        assert result == []

    def test_incorrect(self) -> None:
        tags = "not_int,1, 2,3"
        parser = TagIdParser()
        result = parser(tags)
        assert result == []
