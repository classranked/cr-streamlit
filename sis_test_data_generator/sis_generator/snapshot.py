from __future__ import annotations

import io
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from .constants import CSV_HEADERS, FILE_STEMS, LEGACY_CSV_HEADERS, UPLOAD_NAME_MAP


def _normalize_header(header: list[str]) -> tuple[str, ...]:
    return tuple(str(value).strip() for value in header)


EXPECTED_HEADERS = {
    key: {_normalize_header(value)}
    for key, value in CSV_HEADERS.items()
}
for key, value in LEGACY_CSV_HEADERS.items():
    EXPECTED_HEADERS.setdefault(key, set()).add(_normalize_header(value))


@dataclass
class SnapshotContext:
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def existing_emails(self) -> set[str]:
        emails: set[str] = set()
        for key in ("students", "instructors"):
            if key in self.tables and "Email" in self.tables[key].columns:
                emails.update(self.tables[key]["Email"].dropna().astype(str).str.lower())
        return emails

    @property
    def existing_school_ids(self) -> set[str]:
        ids: set[str] = set()
        for key in ("students", "instructors"):
            if key in self.tables and "School Id" in self.tables[key].columns:
                ids.update(self.tables[key]["School Id"].dropna().astype(str))
        return ids

    @property
    def existing_course_ids(self) -> set[str]:
        if "courses" in self.tables and "Abbreviation" in self.tables["courses"].columns:
            return set(self.tables["courses"]["Abbreviation"].dropna().astype(str))
        if "course_sections" in self.tables:
            return set(self.tables["course_sections"]["Course ID"].dropna().astype(str))
        return set()

    @property
    def existing_section_ids(self) -> set[str]:
        if "course_sections" not in self.tables:
            return set()
        return set(self.tables["course_sections"]["Section ID"].dropna().astype(str))

    @property
    def existing_term_titles(self) -> set[str]:
        if "terms" not in self.tables:
            return set()
        return set(self.tables["terms"]["Title"].dropna().astype(str))

    def next_section_number(self, course_id: str, term_title: str) -> int:
        if "course_sections" not in self.tables:
            return 1
        df = self.tables["course_sections"]
        rows = df[
            (df["Course ID"].astype(str) == course_id)
            & (df["Term"].astype(str) == term_title)
        ]
        max_value = 0
        for section_id in rows["Section ID"].astype(str):
            suffix = section_id.rsplit("-", 1)[-1]
            if suffix.isdigit():
                max_value = max(max_value, int(suffix))
        return max_value + 1

    def summary(self) -> pd.DataFrame:
        rows = []
        for key, file_name in FILE_STEMS.items():
            df = self.tables.get(key)
            rows.append(
                {
                    "dataset": key,
                    "file_name": file_name,
                    "rows": 0 if df is None else len(df),
                }
            )
        return pd.DataFrame(rows)


def _detect_category(file_name: str, header: list[str]) -> str | None:
    stem = Path(file_name).stem.strip().lower().replace(" ", "-")
    if stem in UPLOAD_NAME_MAP:
        return UPLOAD_NAME_MAP[stem]
    normalized = _normalize_header(header)
    for category, expected_variants in EXPECTED_HEADERS.items():
        if normalized in expected_variants:
            return category
    return None


def _normalize_uploaded_table(category: str, df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if category == "courses" and "Subject Code" not in df.columns:
        df["Subject Code"] = df["Abbreviation"].astype(str).str.extract(r"^([A-Za-z]+)", expand=False).fillna("")
        df["Course Number"] = df["Abbreviation"].astype(str).str.extract(r"(\d+)$", expand=False).fillna("")
    if category == "course_sections" and "Course Attributes" not in df.columns:
        df["Course Attributes"] = ""
    return df[CSV_HEADERS[category]].copy()


def load_snapshot_files(files: list) -> SnapshotContext:
    context = SnapshotContext()
    for uploaded_file in files:
        raw_bytes = uploaded_file.getvalue()
        text_stream = io.StringIO(raw_bytes.decode("utf-8-sig", errors="replace"))
        df = pd.read_csv(text_stream, dtype=str).fillna("")
        category = _detect_category(uploaded_file.name, list(df.columns))
        if category is None:
            context.errors.append(f"Could not classify upload: {uploaded_file.name}")
            continue
        normalized = _normalize_header(list(df.columns))
        if normalized not in EXPECTED_HEADERS[category]:
            expected = [list(values) for values in EXPECTED_HEADERS[category]]
            context.errors.append(
                f"{uploaded_file.name} has unexpected columns for {category}: "
                f"expected one of {expected}, got {list(df.columns)}"
            )
            continue
        context.tables[category] = _normalize_uploaded_table(category, df)
    return context
