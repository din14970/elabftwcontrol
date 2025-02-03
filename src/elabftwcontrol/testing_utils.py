import math
from typing import Any


def assert_dicts_equal(
    result: dict[str, Any],
    expected: dict[str, Any],
    order_is_important: bool = False,
) -> None:
    """Utility to check whether two dictionaries are the same even if a value of NaN"""
    if order_is_important:
        assert list(result.keys()) == list(expected.keys())
    else:
        assert set(result.keys()) == set(expected.keys())
    for field_r, value_r in result.items():
        value_e = expected[field_r]
        if isinstance(value_r, float) and math.isnan(value_r):
            assert math.isnan(value_e)
        else:
            assert value_r == value_e
