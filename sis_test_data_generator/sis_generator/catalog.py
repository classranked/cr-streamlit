from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .constants import (
    ACADEMIC_UNIT_CATALOG_COLUMNS,
    ACADEMIC_UNIT_CATALOG_COLUMNS as HIERARCHY_COLUMNS,
    ATTRIBUTE_CATALOG_COLUMNS,
    CATALOG_FILES,
    COURSE_CATALOG_COLUMNS,
    COURSE_REQUIRED_COLUMNS,
    HIERARCHY_LEVEL_COLUMNS,
    SECTION_ATTRIBUTE_COLUMNS,
)


COURSE_CATALOG_PATH = CATALOG_FILES["course_catalog"]
ATTRIBUTE_CATALOG_PATH = CATALOG_FILES["attribute_catalog"]
ACADEMIC_UNIT_CATALOG_PATH = CATALOG_FILES["academic_unit_catalog"]
LEGACY_MASTER_CATALOG_PATH = CATALOG_FILES["legacy_master_catalog"]

DEFAULT_CATALOGS = {
    "course_catalog": COURSE_CATALOG_COLUMNS,
    "attribute_catalog": ATTRIBUTE_CATALOG_COLUMNS,
    "academic_unit_catalog": ACADEMIC_UNIT_CATALOG_COLUMNS,
}

ACADEMIC_UNIT_TYPE_ORDER = {
    "university": 0,
    "college": 1,
    "division": 2,
    "department": 3,
}


def sort_hierarchy_top_down(df: pd.DataFrame) -> pd.DataFrame:
    hierarchy = _clean(df, HIERARCHY_COLUMNS)
    if hierarchy.empty:
        return hierarchy

    def sort_key(item: pd.Series) -> tuple[int, str, str]:
        return (
            ACADEMIC_UNIT_TYPE_ORDER.get(str(item["Academic Unit Type"]).strip().lower(), 999),
            str(item["Title"]).lower(),
            str(item["Abbreviation"]).lower(),
        )

    ordered = sorted(
        hierarchy.to_dict("records"),
        key=sort_key,
    )
    return pd.DataFrame(ordered, columns=HIERARCHY_COLUMNS)


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", str(value)).strip()
    return cleaned


def _initialism(value: str) -> str:
    parts = [part for part in _slugify(value).split() if part]
    if not parts:
        return "UNIT"
    letters = "".join(part[0].upper() for part in parts[:6])
    return letters or parts[0][:6].upper()


def _unique_abbreviation(title: str, used: set[str]) -> str:
    base = _initialism(title)
    candidate = base
    suffix = 1
    while candidate in used:
        suffix += 1
        candidate = f"{base}{suffix}"
    used.add(candidate)
    return candidate


def _clean(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    cleaned = df.copy()
    for column in columns:
        if column not in cleaned.columns:
            cleaned[column] = ""
    return cleaned[columns].fillna("")


def _normalize_course_catalog(df: pd.DataFrame) -> pd.DataFrame:
    course_df = df.copy()
    if set(COURSE_REQUIRED_COLUMNS).issubset(course_df.columns):
        normalized = course_df.copy()
    elif {"subject_code", "course_number", "course_id", "title", "default_department_abbreviation"}.issubset(course_df.columns):
        normalized = course_df.rename(
            columns={
                "title": "Title",
                "course_id": "Abbreviation",
                "default_department_abbreviation": "Parent Academic Unit",
            }
        )
        normalized["Academic Unit Type"] = normalized.get("Academic Unit Type", "Course")
        normalized["Subject Code"] = normalized.get("Subject Code", normalized["subject_code"])
        normalized["Course Number"] = normalized.get("Course Number", normalized["course_number"])
    else:
        normalized = course_df.copy()

    if "Subject Code" not in normalized.columns:
        normalized["Subject Code"] = normalized["Abbreviation"].astype(str).str.extract(r"^([A-Za-z]+)", expand=False).fillna("")
    if "Course Number" not in normalized.columns:
        normalized["Course Number"] = normalized["Abbreviation"].astype(str).str.extract(r"(\d+)$", expand=False).fillna("")
    if "Academic Unit Type" not in normalized.columns:
        normalized["Academic Unit Type"] = "Course"
    return _clean(normalized, COURSE_CATALOG_COLUMNS)


def _build_course_catalog(legacy_df: pd.DataFrame) -> pd.DataFrame:
    course_df = legacy_df[
        ["title", "course_id", "parent_academic_unit", "subject_code", "course_number"]
    ].copy()
    course_df = course_df.rename(
        columns={
            "title": "Title",
            "course_id": "Abbreviation",
            "parent_academic_unit": "Parent Academic Unit",
            "subject_code": "Subject Code",
            "course_number": "Course Number",
        }
    )
    course_df["Academic Unit Type"] = "Course"
    course_df = course_df.drop_duplicates().sort_values(
        ["Subject Code", "Course Number", "Abbreviation"]
    )
    return _normalize_course_catalog(course_df)


def _build_attribute_catalog(legacy_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for column in SECTION_ATTRIBUTE_COLUMNS:
        source = column.lower().replace(" ", "_")
        if source not in legacy_df.columns:
            continue
        counts = legacy_df[source].astype(str).value_counts()
        for value, count in counts.items():
            if not value:
                continue
            rows.append(
                {
                    "attribute_name": column,
                    "column_name": column,
                    "value": value,
                    "weight": str(int(count)),
                    "allow_overflow": "true",
                    "is_hierarchy_level": "true" if column in HIERARCHY_LEVEL_COLUMNS else "false",
                }
            )
    return _clean(
        pd.DataFrame(rows).drop_duplicates(
            subset=["attribute_name", "column_name", "value"]
        ),
        ATTRIBUTE_CATALOG_COLUMNS,
    ).sort_values(["attribute_name", "value"])


def _build_academic_unit_catalog(legacy_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    used_abbreviations: set[str] = set()
    university_abbrev: dict[str, str] = {}
    college_abbrev: dict[str, str] = {}
    division_abbrev: dict[str, str] = {}

    for university in sorted(set(legacy_df["university"].astype(str))):
        if not university:
            continue
        abbrev = _unique_abbreviation(university, used_abbreviations)
        university_abbrev[university] = abbrev
        rows.append(
            {
                "Title": university,
                "Abbreviation": abbrev,
                "Parent Academic Unit": "",
                "Academic Unit Type": "University",
            }
        )

    for college, university in (
        legacy_df[["college", "university"]]
        .drop_duplicates()
        .sort_values(["university", "college"])
        .itertuples(index=False, name=None)
    ):
        if not college:
            continue
        abbrev = college_abbrev.get(college)
        if abbrev is None:
            abbrev = _unique_abbreviation(college, used_abbreviations)
            college_abbrev[college] = abbrev
        rows.append(
            {
                "Title": college,
                "Abbreviation": abbrev,
                "Parent Academic Unit": university_abbrev.get(university, ""),
                "Academic Unit Type": "College",
            }
        )

    for division, college in (
        legacy_df[["division", "college"]]
        .drop_duplicates()
        .sort_values(["college", "division"])
        .itertuples(index=False, name=None)
    ):
        if not division:
            continue
        abbrev = division_abbrev.get(division)
        if abbrev is None:
            abbrev = _unique_abbreviation(division, used_abbreviations)
            division_abbrev[division] = abbrev
        rows.append(
            {
                "Title": division,
                "Abbreviation": abbrev,
                "Parent Academic Unit": college_abbrev.get(college, ""),
                "Academic Unit Type": "Division",
            }
        )

    for department, division, subject_code in (
        legacy_df[["department", "division", "subject_code"]]
        .drop_duplicates()
        .sort_values(["division", "department", "subject_code"])
        .itertuples(index=False, name=None)
    ):
        if not department or not subject_code:
            continue
        used_abbreviations.add(subject_code)
        rows.append(
            {
                "Title": department,
                "Abbreviation": subject_code,
                "Parent Academic Unit": division_abbrev.get(division, ""),
                "Academic Unit Type": "Department",
            }
        )

    units = pd.DataFrame(rows).drop_duplicates(subset=["Abbreviation"])
    return sort_hierarchy_top_down(units)


def migrate_legacy_catalogs(
    legacy_path: Path | None = None,
    course_catalog_path: Path | None = None,
    attribute_catalog_path: Path | None = None,
    academic_unit_catalog_path: Path | None = None,
) -> None:
    legacy_catalog = legacy_path or LEGACY_MASTER_CATALOG_PATH
    if not legacy_catalog.exists():
        for key, columns in DEFAULT_CATALOGS.items():
            path = (
                course_catalog_path
                if key == "course_catalog"
                else attribute_catalog_path
                if key == "attribute_catalog"
                else academic_unit_catalog_path
            ) or CATALOG_FILES[key]
            if not path.exists():
                _clean(pd.DataFrame(columns=columns), columns).to_csv(path, index=False)
        return

    legacy_df = pd.read_csv(legacy_catalog, dtype=str).fillna("")
    outputs = {
        (course_catalog_path or COURSE_CATALOG_PATH): _build_course_catalog(legacy_df),
        (attribute_catalog_path or ATTRIBUTE_CATALOG_PATH): _build_attribute_catalog(legacy_df),
        (academic_unit_catalog_path or ACADEMIC_UNIT_CATALOG_PATH): _build_academic_unit_catalog(legacy_df),
    }
    for path, df in outputs.items():
        if not path.exists():
            df.to_csv(path, index=False)


def ensure_catalogs_exist() -> None:
    migrate_legacy_catalogs()


def load_catalog(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        ensure_catalogs_exist()
    df = pd.read_csv(path, dtype=str).fillna("")
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Catalog is missing columns: {', '.join(missing)}")
    return _clean(df, columns)


def load_course_catalog(path: Path | None = None) -> pd.DataFrame:
    catalog_path = path or COURSE_CATALOG_PATH
    if not catalog_path.exists():
        ensure_catalogs_exist()
    df = pd.read_csv(catalog_path, dtype=str).fillna("")
    return _normalize_course_catalog(df)


def load_attribute_catalog(path: Path | None = None) -> pd.DataFrame:
    return load_catalog(path or ATTRIBUTE_CATALOG_PATH, ATTRIBUTE_CATALOG_COLUMNS)


def load_academic_unit_catalog(path: Path | None = None) -> pd.DataFrame:
    return sort_hierarchy_top_down(
        load_catalog(path or ACADEMIC_UNIT_CATALOG_PATH, ACADEMIC_UNIT_CATALOG_COLUMNS)
    )


def save_catalog(df: pd.DataFrame, path: Path, columns: list[str]) -> None:
    cleaned = _normalize_course_catalog(df) if columns == COURSE_CATALOG_COLUMNS else _clean(df, columns)
    if columns == ACADEMIC_UNIT_CATALOG_COLUMNS:
        cleaned = sort_hierarchy_top_down(cleaned)
    cleaned.to_csv(path, index=False)
