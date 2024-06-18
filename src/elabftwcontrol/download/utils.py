from __future__ import annotations

import re
from typing import TypeVar

import pandas as pd

T = TypeVar("T")


def do_nothing(x: T) -> T:
    return x


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


class ObjectConverter:
    def __init__(self, schema: dict[str, str]) -> None:
        self.schema = schema

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        columns = list(self.schema.keys())
        return data[columns].replace("", None).astype(self.schema)


class ItemConverter(ObjectConverter):
    """Creates Items Dataframe"""

    def __init__(self) -> None:
        schema = {
            "id": "Int64",
            "elabid": "string",
            "category": "Int64",
            "category_title": "string",
            "title": "string",
            "body": "string",
            "tags": "string",
            "fullname": "string",
            "created_at": "datetime64[ns]",
            "modified_at": "datetime64[ns]",
            "rating": "UInt8",
        }
        super().__init__(schema)


class ExperimentConverter(ObjectConverter):
    """Creates Experiments Dataframe"""

    def __init__(self) -> None:
        schema = {
            "id": "Int64",
            "elabid": "string",
            "title": "string",
            "body": "string",
            "tags": "string",
            "fullname": "string",
            "created_at": "datetime64[ns]",
            "modified_at": "datetime64[ns]",
            "rating": "UInt8",
        }
        super().__init__(schema)
