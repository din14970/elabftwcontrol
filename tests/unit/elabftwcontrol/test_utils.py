from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

import elabftwcontrol.utils as utils


def test_sanitize_columns() -> None:
    result = list(
        utils.sanitize_column_names(
            pd.DataFrame({"A b C": [], "g F   Hsdf": []})
        ).columns
    )
    expected = ["a_b_c", "g_f_hsdf"]
    assert result == expected


def test_join_dict_values() -> None:
    d1 = {
        "a": 1,
        "b": 1,
        "d": 1,
    }
    d2 = {
        "a": 2,
        "b": 2,
        "c": 2,
    }
    d3 = {
        "a": 3,
        "c": 3,
        "e": 3,
    }
    result = utils.join_dict_values((d1, d2, d3))
    expected = {
        "a": [1, 2, 3],
        "b": [1, 2, None],
        "c": [None, 2, 3],
        "d": [1, None, None],
        "e": [None, None, 3],
    }
    assert result == expected


def test_lists_in_dict_same_length() -> None:
    d = {1: [1, 2, 3], 2: [1, 2]}
    assert not utils.lists_in_dict_are_same_length(d)
    d = {1: [1, 2, 3], 2: [1, 2, 3]}
    assert utils.lists_in_dict_are_same_length(d)


def test_parse_optional_datetime() -> None:
    assert utils.parse_optional_datetime("2023-08-12 12:13:14") == datetime(
        2023, 8, 12, 12, 13, 14
    )
    assert utils.parse_optional_datetime(None) is None


def test_parse_optional_date() -> None:
    assert utils.parse_optional_date("2023-08-12") == date(2023, 8, 12)
    assert utils.parse_optional_date(None) is None


def test_parse_optional_float() -> None:
    assert abs(utils.parse_optional_float("8.32") - 8.32) < 1e-7
    assert utils.parse_optional_float(None) is None
