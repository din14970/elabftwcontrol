from __future__ import annotations

import re
import warnings
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, TypeVar, Union

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


def read_yaml(filepath: Pathlike) -> Union[dict, list]:
    with open(filepath, "r") as f:
        data = yaml.safe_load(f)
    return data


def compare_dicts(
    new_dict: dict[str, Any],
    old_dict: dict[str, Any],
    ignore_none_values: bool = False,
) -> dict[str, Any]:
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
        return re.sub(r"\s+", "_", col.lower().strip())

    return df.rename(columns=sanitize)


def join_dict_values(dicts: Sequence[dict[Any, Any]]) -> dict[Any, list[Any]]:
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

    output_dict: dict[Any, list[Any]] = {k: [] for k in iter_keys}

    for dct in dicts:
        for k in iter_keys:
            v = dct.get(k)
            output_dict[k].append(v)
    return output_dict


def parse_tag_str(data: Any) -> Optional[list[str]]:
    if isinstance(data, str):
        data = data.split("|")
    return data


def parse_tag_id_str(data: Any) -> Optional[list[int]]:
    if isinstance(data, str):
        data = [int(tag_id) for tag_id in data.split(",")]
    return data


def sanitize_name_for_glue(x: str) -> str:
    """Convert any string to one that is valid for Glue.

    The following rules apply:
    * shorter than 255 characters
    * only letters, numbers and underscores

    Whitespace and non alphanumeric characters are converted to underscores.
    Greek letters are spelled out with regular letters.
    If at the end underscores are doubled or trailing, those are also removed.
    """
    simpled = x.lower().replace(" ", "_")
    greek_letter_replace_map = {
        "α": "alpha_",
        "β": "beta_",
        "γ": "gamma_",
        "δ": "delta_",
        "ε": "epsilon_",
        "ζ": "zeta_",
        "η": "eta_",
        "θ": "theta_",
        "ι": "iota_",
        "κ": "kappa_",
        "λ": "lambda_",
        "μ": "mu_",
        "ν": "nu_",
        "ξ": "xi_",
        "ο": "omicron_",
        "π": "pi_",
        "ρ": "rho_",
        "σ": "sigma_",
        "ς": "sigma_",
        "τ": "tau_",
        "υ": "upsilon_",
        "φ": "phi_",
        "χ": "chi_",
        "ψ": "psi_",
        "ω": "omega_",
    }
    for greek, ascii in greek_letter_replace_map.items():
        simpled = simpled.replace(greek, ascii)
    simpled = re.sub(r"[^a-z0-9_]+", "_", simpled)
    simpled = re.sub(r"__+", "_", simpled).rstrip("_")
    simpled = simpled[: min(255, len(simpled))]
    return simpled


def do_nothing(x: T) -> T:
    return x
