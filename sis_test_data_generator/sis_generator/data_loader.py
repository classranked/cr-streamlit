import json
from functools import lru_cache

import pandas as pd

from .constants import DATA_DIR, EDGE_CASE_COLUMNS, NAME_COLUMNS


@lru_cache(maxsize=1)
def load_first_names() -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "common_first_names.csv")
    return df[NAME_COLUMNS].copy()


@lru_cache(maxsize=1)
def load_last_names() -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "common_last_names.csv")
    return df[NAME_COLUMNS].copy()


@lru_cache(maxsize=1)
def load_edge_case_names() -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "edge_case_names.csv")
    return df[EDGE_CASE_COLUMNS].copy()


@lru_cache(maxsize=1)
def load_metadata() -> dict:
    with open(DATA_DIR / "seed_metadata.json", encoding="utf-8") as handle:
        return json.load(handle)
