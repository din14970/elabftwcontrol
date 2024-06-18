from __future__ import annotations

import csv
import re
import warnings
from datetime import date, datetime
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import pandas as pd
import yaml

Pathlike = Union[Path, str]

T = TypeVar("T")
V = TypeVar("V")


def number_to_base(n: int, base: int) -> list[int]:
    if n == 0:
        return [0]
    digits = []
    while n:
        digits.append(int(n % base))
        n //= base
    return digits[::-1]


def read_yaml(filepath: Pathlike) -> Union[Dict, List]:
    with open(filepath, "r") as f:
        data = yaml.safe_load(f)
    return data


def compare_dicts(
    new_dict: Dict[str, Any],
    old_dict: Dict[str, Any],
    ignore_none_values: bool = False,
) -> Dict[str, Any]:
    """See what has changed to a dict compared to an old version"""
    if ignore_none_values:
        new_values = set((k, v) for k, v in new_dict.items() if v is not None)
    else:
        new_values = set(new_dict.items())
    old_values = set(old_dict.items())
    changed_pairs = new_values - old_values
    difference = dict(changed_pairs)
    return difference


def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Replace whitespace with underscore and put everything in lowercase"""

    def sanitize(col: str) -> str:
        return re.sub("\s+", "_", col.lower().strip())

    return df.rename(columns=sanitize)


def df_to_excel(df: pd.DataFrame, filepath: Pathlike) -> None:
    df.to_excel(
        filepath,
        index=False,
    )


def df_to_csv(df: pd.DataFrame, filepath: Pathlike) -> None:
    df.to_csv(
        filepath,
        index=False,
        sep=";",
        escapechar="\\",
        quoting=csv.QUOTE_ALL,
    )


def join_dict_values(dicts: Sequence[Dict[Any, Any]]) -> Dict[Any, List[Any]]:
    """Join multiple dictionaries on the key and merge the values in a list per key.
    If input dicts miss a key, None is inserted for that dict in the list.
    """
    iter_keys: Iterable[Any]
    all_output_keys = set().union(*(d.keys() for d in dicts))
    if all_output_keys == set(dicts[0].keys()):
        iter_keys = dicts[0].keys()
    else:
        warnings.warn(
            "Not all dicts have the same keys, resulting dict has random order."
        )
        iter_keys = all_output_keys

    output_dict: Dict[Any, List[Any]] = {k: [] for k in iter_keys}

    for dct in dicts:
        for k in iter_keys:
            v = dct.get(k)
            output_dict[k].append(v)
    return output_dict


def lists_in_dict_are_same_length(dct: Dict[Any, List[Any]]) -> bool:
    """Check whether list values in a dict are all the same lenght"""
    return len(set(map(len, dct.values()))) == 1


def parse_optional(
    value: Optional[T],
    parse_func: Callable[[T], V],
) -> Optional[V]:
    if value is None or value == "" or value == "None":
        return None
    else:
        return parse_func(value)


def parse_optional_datetime(value: Optional[str]) -> Optional[datetime]:
    return parse_optional(
        value,
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
    )


def parse_optional_date(value: Optional[str]) -> Optional[date]:
    def parse_date_flexible(value: str) -> date:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").date()

    return parse_optional(
        value,
        parse_date_flexible,
    )


def parse_optional_float(value: Optional[str]) -> Optional[float]:
    return parse_optional(value, float)


def parse_optional_int(value: Optional[str]) -> Optional[int]:
    return parse_optional(value, int)
