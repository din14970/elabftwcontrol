import pytest

from elabftwcontrol.core.models import (
    ConfigMetadata,
    GroupModel,
    MetadataModel,
    SingleFieldModel,
)


def test_metadata_model() -> None:
    dummy_extra_data = {
        "elabftw": {
            "extra_fields_groups": [
                {"id": 1, "name": "g1"},
                {"id": 2, "name": "g2"},
                {"id": 3, "name": "g3"},
            ],
        },
        "extra_fields": {
            "f1": {
                "type": "number",
                "value": "2.3",
                "unit": "kg",
                "group_id": "1",
                "position": "3",
            },
            "k3": {
                "type": "date",
                "value": "2023-09-12",
                "group_id": "1",
                "position": "2",
            },
        },
    }
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
                type="number",
                value="2.3",
                unit="kg",
                group_id="1",
                position="3",
            ),
            "k3": SingleFieldModel(
                type="date",
                value="2023-09-12",
                group_id="1",
                position="2",
            )
        }
    )
    model = MetadataModel(**dummy_extra_data)
    assert expected == model


def test_metadatamodel_empty() -> None:
    expected = MetadataModel()
    model = MetadataModel(**{})
    assert expected == model


def test_metadatamodel_failure() -> None:
    dummy_extra_data = {
        "extra_fields": {
            "f2": {
                "type": "something else",
                "value": "blabla",
                "group_id": "2",
                "position": "1",
            },
        },
    }
    with pytest.raises(Exception):
        _ = MetadataModel(**dummy_extra_data)
