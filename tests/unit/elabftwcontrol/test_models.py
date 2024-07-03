from datetime import date

from elabftwcontrol.models import NO_CATEGORY_NAME, ExtraFieldData, SingleFieldData


class TestExtraField:
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
            "f2": {
                "type": "something else",
                "value": "blabla",
                "group_id": "2",
                "position": "1",
            },
            "k3": {
                "type": "date",
                "value": "2023-09-12",
                "group_id": "1",
                "position": "2",
            },
            "k4": {
                "type": "wrong",
                "value": "NA",
                "unit": "bb",
                "group_id": "4",
                "position": "4",
            },
        },
    }

    def test_from_extra_data(self) -> None:
        field_data = ExtraFieldData.from_extra_data(self.dummy_extra_data)

        expected_fields = ["f2", "k3", "f1", "k4"]
        expected_values = ["blabla", date(2023, 9, 12), 2.3, "NA"]
        expected_units = [None, None, "kg", "bb"]
        expected_categories = ["g2", "g1", "g1", None]
        assert list(field_data.get_field_names()) == expected_fields
        assert list(field_data.get_field_values()) == expected_values
        assert list(field_data.get_field_units()) == expected_units
        assert list(field_data.get_field_categories()) == expected_categories

    def test_to_extra_data(self) -> None:
        field_data = ExtraFieldData.from_extra_data(self.dummy_extra_data)
        assert field_data.to_extra_data() == self.dummy_extra_data

    def test_select_categories(self) -> None:
        field_data = ExtraFieldData.from_extra_data(self.dummy_extra_data)
        field_data = field_data.select_categories(["g1"])

        expected_fields = ["k3", "f1"]
        expected_values = [date(2023, 9, 12), 2.3]
        expected_units = [None, "kg"]
        expected_categories = ["g1", "g1"]
        assert list(field_data.get_field_names()) == expected_fields
        assert list(field_data.get_field_values()) == expected_values
        assert list(field_data.get_field_units()) == expected_units
        assert list(field_data.get_field_categories()) == expected_categories

    def test_select_fields(self) -> None:
        field_data = ExtraFieldData.from_extra_data(self.dummy_extra_data)
        field_data = field_data.select_fields(["f1", "k3"])

        expected_fields = ["k3", "f1"]
        expected_values = [date(2023, 9, 12), 2.3]
        expected_units = [None, "kg"]
        expected_categories = ["g1", "g1"]
        assert list(field_data.get_field_names()) == expected_fields
        assert list(field_data.get_field_values()) == expected_values
        assert list(field_data.get_field_units()) == expected_units
        assert list(field_data.get_field_categories()) == expected_categories

    def test_fields_in_category(self) -> None:
        field_data = ExtraFieldData.from_extra_data(self.dummy_extra_data)
        result = field_data.get_fields_in_category("g1")
        result_processed = [field.name for field in result]
        expected = ["k3", "f1"]
        assert result_processed == expected

    def test_category_to_field_map(self) -> None:
        field_data = ExtraFieldData.from_extra_data(self.dummy_extra_data)
        result = field_data.get_category_to_field_map()
        result_processed = {k: [vv.name for vv in v] for k, v in result.items()}
        expected = {
            "g1": ["k3", "f1"],
            "g2": ["f2"],
            NO_CATEGORY_NAME: ["k4"],
        }
        assert result_processed == expected

    def test_from_lists(self) -> None:
        field_names = [
            "some field 1",
            "some field 2",
            "some field 3",
        ]
        field_values = [
            3,
            "string",
            date(2023, 4, 1),
        ]
        field_categories = [
            "group 1",
            "group 2",
            "group 1",
        ]
        field_units = [
            "kg",
            None,
            None,
        ]
        extra_fields = ExtraFieldData.from_lists(
            field_names=field_names,
            field_values=field_values,
            field_categories=field_categories,
            field_units=field_units,
        )
        expected_fields = [
            SingleFieldData(
                name="some field 1",
                value=3.0,
                group_id=1,
                unit="kg",
                type="number",
                position=1,
            ),
            SingleFieldData(
                name="some field 2",
                value="string",
                group_id=2,
                type="text",
                position=2,
            ),
            SingleFieldData(
                name="some field 3",
                value=date(2023, 4, 1),
                group_id=1,
                type="date",
                position=3,
            ),
        ]
        expected_category_map = {
            1: "group 1",
            2: "group 2",
        }
        assert list(extra_fields.fields.values()) == expected_fields
        assert extra_fields.category_map == expected_category_map

    def test_to_lists(self) -> None:
        fields = [
            SingleFieldData(
                name="some field 1",
                value=3.0,
                group_id=1,
                unit="kg",
                type="number",
                position=1,
            ),
            SingleFieldData(
                name="some field 2",
                value="string",
                group_id=2,
                type="text",
                position=2,
            ),
            SingleFieldData(
                name="some field 3",
                value=date(2023, 4, 1),
                group_id=1,
                type="date",
                position=3,
            ),
        ]
        category_map = {
            1: "group 1",
            2: "group 2",
        }
        ordered_fields = ExtraFieldData._field_list_to_ordered_dict(fields)
        extra_fields = ExtraFieldData(
            fields=ordered_fields,
            category_map=category_map,
        )
        names, values, categories, units = extra_fields.to_lists()
        expected_field_names = [
            "some field 1",
            "some field 2",
            "some field 3",
        ]
        expected_field_values = [
            3,
            "string",
            date(2023, 4, 1),
        ]
        expected_field_categories = [
            "group 1",
            "group 2",
            "group 1",
        ]
        expected_field_units = [
            "kg",
            None,
            None,
        ]
        assert names == expected_field_names
        assert values == expected_field_values
        assert categories == expected_field_categories
        assert units == expected_field_units
