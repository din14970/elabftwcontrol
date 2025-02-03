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
