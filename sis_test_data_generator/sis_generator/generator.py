from __future__ import annotations

import io
import random
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from .constants import (
    COURSE_REQUIRED_COLUMNS,
    CSV_HEADERS,
    FILE_STEMS,
    HIERARCHY_LEVEL_COLUMNS,
    SECTION_ATTRIBUTE_COLUMNS,
)
from .catalog import sort_hierarchy_top_down
from .data_loader import load_edge_case_names, load_first_names, load_last_names
from .snapshot import SnapshotContext


DATE_FORMAT = "%m/%d/%Y %I:%M %p"
TERM_SYSTEM_PATTERNS = {
    "semester": [
        ("Spring", (1, 10), 115),
        ("Summer", (5, 20), 60),
        ("Fall", (8, 28), 110),
    ],
    "quarter": [
        ("Winter", (1, 5), 75),
        ("Spring", (3, 25), 75),
        ("Summer", (6, 20), 75),
        ("Fall", (9, 25), 75),
    ],
}
STUDENT_GRADES = ["Freshman", "Sophomore", "Junior", "Senior", "Graduate"]
GENDERS = ["Female", "Male", "Non-Binary", "Prefer Not to Say"]
TENURE_TRACKS = ["Assistant", "Associate", "Full", "Non-Tenure", "Adjunct"]
TERM_TITLE_PATTERN = re.compile(r"^\s*(\d{4})\s+(.+?)\s*$")
GENERIC_ATTRIBUTE_COLUMNS = [
    "Program",
    "Campus",
    "Session",
    "Course Level",
    "Course Type",
    "Delivery Method",
]
FLAT_HIERARCHY_SLOT_ORDER = ["University", "College", "Division", "Department"]


@dataclass
class GenerationConfig:
    mode: str
    structure_mode: str
    seed: int
    term_count: int
    term_system: str
    start_term_label: str
    start_term_year: int
    courses_count: int
    sections_per_course_min: int
    sections_per_course_max: int
    student_count: int
    instructor_count: int
    enrollments_per_section_min: int
    enrollments_per_section_max: int
    instructors_per_section_min: int
    instructors_per_section_max: int
    duplicate_mode: bool
    duplicate_count: int
    edge_case_rate: float
    email_domain: str
    institution_name: str
    institution_abbreviation: str
    flat_variance_rate: float = 0.15
    hierarchy_source: str = "fallback"
    include_section_attributes: bool = True


@dataclass
class GenerationResult:
    files: dict[str, pd.DataFrame]
    summary: pd.DataFrame
    collision_report: pd.DataFrame
    duplicate_report: pd.DataFrame
    zip_bytes: bytes


@dataclass
class HierarchyContext:
    table: pd.DataFrame
    nodes: dict[str, dict[str, str]]
    children: dict[str, list[str]]
    roots: list[str]
    department_abbreviations: list[str]


@dataclass(frozen=True)
class GenerationPlan:
    authoritative: frozenset[str]
    generated: frozenset[str]
    skipped: frozenset[str]
    resolved_sources: dict[str, str]
    section_driver: str | None
    flat_hierarchy_source: str | None

    def is_authoritative(self, dataset: str) -> bool:
        return dataset in self.authoritative

    def should_generate(self, dataset: str) -> bool:
        return dataset in self.generated

    def source_for(self, dataset: str) -> str | None:
        return self.resolved_sources.get(dataset)


def validate_hierarchy(df: pd.DataFrame) -> pd.DataFrame:
    required = CSV_HEADERS["hierarchy"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Hierarchy is missing columns: {', '.join(missing)}")

    hierarchy = df[required].fillna("").copy()
    if hierarchy.empty:
        raise ValueError("Hierarchy cannot be empty in hierarchy mode.")

    if hierarchy["Abbreviation"].eq("").any():
        raise ValueError("Hierarchy contains a row with an empty Abbreviation.")
    if hierarchy["Title"].eq("").any():
        raise ValueError("Hierarchy contains a row with an empty Title.")
    if hierarchy["Academic Unit Type"].eq("").any():
        raise ValueError("Hierarchy contains a row with an empty Academic Unit Type.")
    if hierarchy["Abbreviation"].duplicated().any():
        duplicates = sorted(hierarchy.loc[hierarchy["Abbreviation"].duplicated(), "Abbreviation"].unique())
        raise ValueError(f"Hierarchy contains duplicate abbreviations: {', '.join(duplicates)}")

    nodes = hierarchy.set_index("Abbreviation").to_dict("index")
    roots = []
    for abbreviation, row in nodes.items():
        parent = row["Parent Academic Unit"]
        if not parent:
            roots.append(abbreviation)
            continue
        if parent not in nodes:
            raise ValueError(
                f"Hierarchy row {abbreviation} references missing parent {parent}."
            )

    if len(roots) != 1:
        raise ValueError(
            f"Hierarchy must have exactly one root academic unit; found {len(roots)}."
        )

    visiting: set[str] = set()
    visited: set[str] = set()

    def walk(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise ValueError(f"Hierarchy contains a cycle at {node}.")
        visiting.add(node)
        parent = nodes[node]["Parent Academic Unit"]
        if parent:
            walk(parent)
        visiting.remove(node)
        visited.add(node)

    for abbreviation in nodes:
        walk(abbreviation)
    return sort_hierarchy_top_down(hierarchy)


def plan_generation(structure_mode: str, snapshot: SnapshotContext) -> GenerationPlan:
    datasets = set(FILE_STEMS)
    authoritative = set(snapshot.tables)
    generated: set[str] = set()
    resolved_sources: dict[str, str] = {}

    for dataset in authoritative:
        resolved_sources[dataset] = f"uploaded:{dataset}"

    if "course_sections" in authoritative:
        resolved_sources["course_sections"] = "uploaded:course_sections"
        resolved_sources.setdefault(
            "terms",
            "uploaded:terms" if "terms" in authoritative else "derived:course_sections",
        )
        section_driver = "course_sections"
    else:
        section_driver = "courses" if "courses" in authoritative else "catalog"
        generated.add("course_sections")
        resolved_sources["course_sections"] = f"generated:{section_driver}"

        if "terms" in authoritative:
            resolved_sources["terms"] = "uploaded:terms"
        else:
            generated.add("terms")
            resolved_sources["terms"] = "generated:terms"

        if structure_mode == "hierarchy":
            if "courses" in authoritative:
                resolved_sources["courses"] = "uploaded:courses"
            else:
                generated.add("courses")
                resolved_sources["courses"] = (
                    "generated:hierarchy"
                    if "hierarchy" in authoritative
                    else "generated:fallback_hierarchy"
                )

            if "hierarchy" in authoritative:
                resolved_sources["hierarchy"] = "uploaded:hierarchy"
            else:
                generated.add("hierarchy")
                resolved_sources["hierarchy"] = "generated:fallback_hierarchy"
        else:
            if "courses" in authoritative:
                resolved_sources["courses"] = "uploaded:courses"
            if "hierarchy" in authoritative:
                resolved_sources["hierarchy"] = "uploaded:hierarchy"

    if "students" in authoritative:
        resolved_sources["students"] = "uploaded:students"
    else:
        generated.add("students")
        resolved_sources["students"] = "generated:students"

    if "instructors" in authoritative:
        resolved_sources["instructors"] = "uploaded:instructors"
    else:
        generated.add("instructors")
        resolved_sources["instructors"] = "generated:instructors"

    if "student_enrollments" in authoritative:
        resolved_sources["student_enrollments"] = "uploaded:student_enrollments"
    else:
        generated.add("student_enrollments")
        resolved_sources["student_enrollments"] = f"generated:{section_driver or 'course_sections'}+students"

    if "instructor_assignments" in authoritative:
        resolved_sources["instructor_assignments"] = "uploaded:instructor_assignments"
    else:
        generated.add("instructor_assignments")
        resolved_sources["instructor_assignments"] = f"generated:{section_driver or 'course_sections'}+instructors"

    skipped = datasets - authoritative - generated
    return GenerationPlan(
        authoritative=frozenset(sorted(authoritative)),
        generated=frozenset(sorted(generated)),
        skipped=frozenset(sorted(skipped)),
        resolved_sources=resolved_sources,
        section_driver=section_driver,
        flat_hierarchy_source=(
            "hierarchy" if structure_mode == "flat" and "hierarchy" in authoritative else None
        ),
    )


def validate_course_catalog(df: pd.DataFrame, hierarchy_df: pd.DataFrame) -> pd.DataFrame:
    courses = df.copy().fillna("")
    if {"subject_code", "course_number", "course_id", "title", "default_department_abbreviation"}.issubset(courses.columns):
        courses = courses.rename(
            columns={
                "title": "Title",
                "course_id": "Abbreviation",
                "default_department_abbreviation": "Parent Academic Unit",
            }
        )
        courses["Academic Unit Type"] = courses.get("Academic Unit Type", "Course")
        courses["Subject Code"] = courses.get("Subject Code", courses["subject_code"])
        courses["Course Number"] = courses.get("Course Number", courses["course_number"])

    missing = [column for column in COURSE_REQUIRED_COLUMNS if column not in courses.columns]
    if missing:
        raise ValueError(f"Courses are missing columns: {', '.join(missing)}")

    required_view = courses[COURSE_REQUIRED_COLUMNS]
    if required_view.empty:
        return courses

    if required_view["Title"].eq("").any():
        raise ValueError("Courses contain a row with an empty Title.")
    if required_view["Abbreviation"].eq("").any():
        raise ValueError("Courses contain a row with an empty Abbreviation.")
    if required_view["Parent Academic Unit"].eq("").any():
        raise ValueError("Courses contain a row with an empty Parent Academic Unit.")
    if required_view["Academic Unit Type"].eq("").any():
        raise ValueError("Courses contain a row with an empty Academic Unit Type.")

    parents = set(validate_hierarchy(hierarchy_df)["Abbreviation"].astype(str))
    invalid_parents = sorted(
        {
            parent
            for parent in required_view["Parent Academic Unit"].astype(str)
            if parent and parent not in parents
        }
    )
    if invalid_parents:
        raise ValueError(
            "Courses reference missing parent academic units: "
            + ", ".join(invalid_parents)
            + "."
        )

    course_types = required_view["Academic Unit Type"].astype(str).str.strip()
    invalid_types = sorted({value for value in course_types if value and value.lower() != "course"})
    if invalid_types:
        raise ValueError(
            "Courses must use `Academic Unit Type` of `Course`; found "
            + ", ".join(invalid_types)
            + "."
        )
    return courses


def _build_hierarchy_context(df: pd.DataFrame) -> HierarchyContext:
    nodes = df.set_index("Abbreviation").to_dict("index")
    children: dict[str, list[str]] = {abbreviation: [] for abbreviation in nodes}
    roots: list[str] = []
    for abbreviation, row in nodes.items():
        parent = row["Parent Academic Unit"]
        if parent:
            children.setdefault(parent, []).append(abbreviation)
        else:
            roots.append(abbreviation)
    departments = [
        abbreviation
        for abbreviation, row in nodes.items()
        if row["Academic Unit Type"].strip().lower() == "department"
    ]
    if not departments:
        departments = [
            abbreviation
            for abbreviation, row in nodes.items()
            if not children.get(abbreviation)
        ]
    return HierarchyContext(
        table=df.copy(),
        nodes=nodes,
        children=children,
        roots=roots,
        department_abbreviations=sorted(departments),
    )


def _apply_default_university_to_hierarchy(
    hierarchy_df: pd.DataFrame,
    config: GenerationConfig,
) -> pd.DataFrame:
    if config.hierarchy_source != "fallback":
        return hierarchy_df

    institution_name = config.institution_name.strip()
    institution_abbreviation = config.institution_abbreviation.strip().upper()
    if not institution_name or not institution_abbreviation:
        raise ValueError(
            "Institution name and university abbreviation are required when using the fallback hierarchy."
        )

    hierarchy = hierarchy_df.copy()
    root_rows = hierarchy[hierarchy["Parent Academic Unit"].astype(str).str.strip() == ""]
    university_roots = root_rows[
        root_rows["Academic Unit Type"].astype(str).str.strip().str.lower() == "university"
    ]
    if len(university_roots) != 1:
        return hierarchy

    root_index = university_roots.index[0]
    existing_root_abbreviation = str(hierarchy.loc[root_index, "Abbreviation"]).strip()
    duplicate_abbreviation = hierarchy.index != root_index
    if hierarchy.loc[duplicate_abbreviation, "Abbreviation"].astype(str).str.strip().eq(institution_abbreviation).any():
        raise ValueError(
            f"University abbreviation '{institution_abbreviation}' conflicts with another academic unit in the fallback hierarchy."
        )

    hierarchy.loc[root_index, "Title"] = institution_name
    hierarchy.loc[root_index, "Abbreviation"] = institution_abbreviation
    if existing_root_abbreviation and existing_root_abbreviation != institution_abbreviation:
        parent_values = hierarchy["Parent Academic Unit"].astype(str).str.strip()
        hierarchy.loc[parent_values == existing_root_abbreviation, "Parent Academic Unit"] = institution_abbreviation

    return hierarchy


def _weighted_choice(rng: random.Random, values: list[str], weights: list[float]) -> str:
    return rng.choices(values, weights=weights, k=1)[0]


def _randint_inclusive(rng: random.Random, minimum: int, maximum: int) -> int:
    if maximum < minimum:
        minimum, maximum = maximum, minimum
    return rng.randint(minimum, maximum)


def _normalize_for_email(value: str) -> str:
    replacements = {
        "á": "a",
        "à": "a",
        "ä": "a",
        "â": "a",
        "ã": "a",
        "å": "a",
        "é": "e",
        "è": "e",
        "ë": "e",
        "ê": "e",
        "í": "i",
        "ì": "i",
        "ï": "i",
        "î": "i",
        "ñ": "n",
        "ó": "o",
        "ò": "o",
        "ö": "o",
        "ô": "o",
        "õ": "o",
        "ú": "u",
        "ù": "u",
        "ü": "u",
        "û": "u",
        "ý": "y",
        "ÿ": "y",
        "ç": "c",
        "ß": "ss",
        "œ": "oe",
        "æ": "ae",
    }
    cleaned = value.strip().lower()
    for source, target in replacements.items():
        cleaned = cleaned.replace(source, target)
    cleaned = re.sub(r"[^a-z0-9]+", ".", cleaned)
    cleaned = re.sub(r"\.+", ".", cleaned).strip(".")
    return cleaned or "user"


def _normalize_term_label(label: str) -> str:
    return " ".join(str(label).strip().split()).lower()


def _parse_term_title(title: str) -> tuple[int, str] | None:
    match = TERM_TITLE_PATTERN.match(str(title))
    if not match:
        return None
    return int(match.group(1)), _normalize_term_label(match.group(2))


def _infer_term_system(snapshot: SnapshotContext) -> str | None:
    labels = {
        parsed[1]
        for title in snapshot.existing_term_titles
        if (parsed := _parse_term_title(title)) is not None
    }
    if not labels:
        return None
    quarter_labels = {_normalize_term_label(label) for label, _, _ in TERM_SYSTEM_PATTERNS["quarter"]}
    semester_labels = {_normalize_term_label(label) for label, _, _ in TERM_SYSTEM_PATTERNS["semester"]}
    if labels.issubset(quarter_labels):
        return "quarter"
    if labels.issubset(semester_labels):
        return "semester"
    if "winter" in labels:
        return "quarter"
    return "semester"


def _resolve_term_start(config: GenerationConfig, snapshot: SnapshotContext) -> tuple[str, int, int]:
    term_system = config.term_system
    if config.mode == "delta":
        term_system = _infer_term_system(snapshot) or term_system
    patterns = TERM_SYSTEM_PATTERNS[term_system]
    normalized_labels = [_normalize_term_label(label) for label, _, _ in patterns]
    if config.mode == "delta":
        parsed_terms = []
        for title in snapshot.existing_term_titles:
            parsed = _parse_term_title(title)
            if parsed is None:
                continue
            year, label = parsed
            if label not in normalized_labels:
                continue
            parsed_terms.append((year, normalized_labels.index(label)))
        if parsed_terms:
            last_year, last_index = max(parsed_terms)
            next_index = (last_index + 1) % len(patterns)
            next_year = last_year + (1 if next_index == 0 else 0)
            return term_system, next_index, next_year

    start_label = _normalize_term_label(config.start_term_label)
    if start_label not in normalized_labels:
        start_label = normalized_labels[0]
    return term_system, normalized_labels.index(start_label), config.start_term_year


def _build_terms(config: GenerationConfig, snapshot: SnapshotContext) -> pd.DataFrame:
    term_system, pattern_index, year = _resolve_term_start(config, snapshot)
    patterns = TERM_SYSTEM_PATTERNS[term_system]
    titles_seen = set(snapshot.existing_term_titles)
    rows = []
    while len(rows) < config.term_count:
        season, (month, day), duration_days = patterns[pattern_index]
        title = f"{year} {season}"
        if title in titles_seen:
            pattern_index = (pattern_index + 1) % len(patterns)
            if pattern_index == 0:
                year += 1
            continue
        start_date = datetime(year, month, day)
        end_date = start_date + timedelta(days=duration_days)
        rows.append(
            {
                "Title": title,
                "Start Date": start_date.strftime(DATE_FORMAT),
                "End Date": end_date.strftime(DATE_FORMAT),
            }
        )
        titles_seen.add(title)
        pattern_index = (pattern_index + 1) % len(patterns)
        if pattern_index == 0:
            year += 1
    return pd.DataFrame(rows, columns=CSV_HEADERS["terms"])


def _pick_name(rng: random.Random, config: GenerationConfig) -> tuple[str, str]:
    if rng.random() < config.edge_case_rate:
        edge_cases = load_edge_case_names()
        pick = edge_cases.iloc[rng.randrange(len(edge_cases))]
        return str(pick["first_name"]), str(pick["last_name"])

    first_names = load_first_names()
    last_names = load_last_names()
    return (
        _weighted_choice(
            rng,
            first_names["name"].tolist(),
            first_names["weight"].astype(float).tolist(),
        ),
        _weighted_choice(
            rng,
            last_names["name"].tolist(),
            last_names["weight"].astype(float).tolist(),
        ),
    )


def _next_unique_email(
    first_name: str,
    last_name: str,
    domain: str,
    existing_emails: set[str],
    collisions: list[dict[str, str]],
) -> str:
    base = f"{_normalize_for_email(first_name)}.{_normalize_for_email(last_name)}"
    candidate = f"{base}@{domain}"
    suffix = 1
    while candidate.lower() in existing_emails:
        collisions.append(
            {"type": "email", "value": candidate, "resolution": f"regenerated-{suffix}"}
        )
        suffix += 1
        candidate = f"{base}{suffix}@{domain}"
    existing_emails.add(candidate.lower())
    return candidate


def _next_unique_school_id(
    rng: random.Random,
    existing_school_ids: set[str],
    collisions: list[dict[str, str]],
) -> str:
    while True:
        candidate = f"{rng.randint(100000, 999999)}"
        if candidate not in existing_school_ids:
            existing_school_ids.add(candidate)
            return candidate
        collisions.append({"type": "school_id", "value": candidate, "resolution": "regenerated"})


def _attribute_options(attribute_catalog: pd.DataFrame) -> dict[str, dict[str, object]]:
    options: dict[str, dict[str, object]] = {}
    for column, group in attribute_catalog.groupby("column_name"):
        values = group["value"].astype(str).tolist()
        weights = pd.to_numeric(group["weight"], errors="coerce").fillna(1.0).tolist()
        options[column] = {
            "values": values,
            "weights": [float(weight) if float(weight) > 0 else 1.0 for weight in weights],
            "allow_overflow": any(
                str(value).strip().lower() == "true" for value in group["allow_overflow"]
            ),
            "is_hierarchy_level": any(
                str(value).strip().lower() == "true" for value in group["is_hierarchy_level"]
            ),
        }
    return options


def _course_level_from_number(course_number: str) -> str:
    digits = "".join(char for char in str(course_number) if char.isdigit())
    if not digits:
        return "General"
    bucket = (int(digits) // 100) * 100
    return f"{bucket} Level"


def _resolve_course_home_unit(
    course: pd.Series,
    hierarchy: HierarchyContext,
    rng: random.Random,
) -> str:
    preferred = str(course.get("default_department_abbreviation", "")).strip()
    if preferred and preferred in hierarchy.nodes:
        return preferred
    if hierarchy.department_abbreviations:
        return rng.choice(hierarchy.department_abbreviations)
    return hierarchy.roots[0]


def _hierarchy_chain(unit_abbreviation: str, hierarchy: HierarchyContext) -> list[dict[str, str]]:
    chain: list[dict[str, str]] = []
    current = unit_abbreviation
    while current:
        row = hierarchy.nodes.get(current)
        if row is None:
            break
        chain.append(row)
        current = row["Parent Academic Unit"]
    chain.reverse()
    return chain


def _chain_titles_for_unit(unit_abbreviation: str, hierarchy: HierarchyContext) -> dict[str, str]:
    title_by_type = {column: "" for column in HIERARCHY_LEVEL_COLUMNS}
    for row in _hierarchy_chain(unit_abbreviation, hierarchy):
        unit_type = str(row["Academic Unit Type"]).strip().lower()
        if unit_type == "department":
            title_by_type["Department"] = row["Title"]
        elif unit_type == "division":
            title_by_type["Division"] = row["Title"]
        elif unit_type == "college":
            title_by_type["College"] = row["Title"]
        elif unit_type == "university":
            title_by_type["University"] = row["Title"]
    return title_by_type


def _chain_titles_for_root(hierarchy: HierarchyContext) -> dict[str, str]:
    title_by_type = {column: "" for column in HIERARCHY_LEVEL_COLUMNS}
    if not hierarchy.roots:
        return title_by_type
    root = hierarchy.nodes.get(hierarchy.roots[0])
    if root is None:
        return title_by_type

    root_type = str(root["Academic Unit Type"]).strip().lower()
    if root_type == "department":
        title_by_type["Department"] = root["Title"]
    elif root_type == "division":
        title_by_type["Division"] = root["Title"]
    elif root_type == "college":
        title_by_type["College"] = root["Title"]
    elif root_type == "university":
        title_by_type["University"] = root["Title"]
    return title_by_type


def _sequential_chain_titles_for_unit(unit_abbreviation: str, hierarchy: HierarchyContext) -> dict[str, str]:
    title_by_slot = {column: "" for column in HIERARCHY_LEVEL_COLUMNS}
    chain_titles = [str(row["Title"]) for row in _hierarchy_chain(unit_abbreviation, hierarchy) if str(row["Title"]).strip()]
    if not chain_titles:
        return title_by_slot

    visible_titles = chain_titles[-len(FLAT_HIERARCHY_SLOT_ORDER):]
    start = len(FLAT_HIERARCHY_SLOT_ORDER) - len(visible_titles)
    for offset, title in enumerate(visible_titles):
        title_by_slot[FLAT_HIERARCHY_SLOT_ORDER[start + offset]] = title
    return title_by_slot


def _section_hierarchy_titles_for_unit(
    unit_abbreviation: str,
    hierarchy: HierarchyContext,
    structure_mode: str,
) -> dict[str, str]:
    chain = _hierarchy_chain(unit_abbreviation, hierarchy)
    unit_types = {
        str(row["Academic Unit Type"]).strip().lower()
        for row in chain
        if str(row["Academic Unit Type"]).strip()
    }
    standard_types = {"university", "college", "division", "department"}
    if structure_mode == "hierarchy" and unit_types.issubset(standard_types):
        return _chain_titles_for_unit(unit_abbreviation, hierarchy)
    return _sequential_chain_titles_for_unit(unit_abbreviation, hierarchy)


def _sample_attribute(
    rng: random.Random,
    options: dict[str, dict[str, object]],
    column: str,
    fallback: str = "",
) -> tuple[str, list[str]]:
    config = options.get(column)
    if not config:
        return fallback, []
    values = [value for value in config["values"] if value]
    weights = list(config["weights"])
    if not values:
        return fallback, []
    primary = fallback if fallback in values and fallback else _weighted_choice(rng, values, weights)
    extras: list[str] = []
    if config["allow_overflow"] and len(values) > 1 and rng.random() < 0.35:
        remaining = [value for value in values if value != primary]
        if remaining:
            max_extra = 2 if len(remaining) > 1 else 1
            extra_count = _randint_inclusive(rng, 1, max_extra)
            extras = rng.sample(remaining, k=min(extra_count, len(remaining)))
    return primary, extras


def _serialize_overflow(overflow: dict[str, list[str]]) -> str:
    parts = []
    for title, values in overflow.items():
        unique_values = [value for value in values if value]
        if not unique_values:
            continue
        parts.append(f"{title}: {', '.join(unique_values)}")
    return "; ".join(parts)


def _apply_flat_variance(
    rng: random.Random,
    section_row: dict[str, str],
    hierarchy: HierarchyContext,
    current_department_title: str,
) -> dict[str, str]:
    if not hierarchy.department_abbreviations:
        return section_row

    candidates = [
        abbreviation
        for abbreviation in hierarchy.department_abbreviations
        if hierarchy.nodes[abbreviation]["Title"] != current_department_title
    ]
    if not candidates:
        return section_row
    replacement = rng.choice(candidates)
    section_row.update(_sequential_chain_titles_for_unit(replacement, hierarchy))
    return section_row


def _build_courses(
    course_catalog: pd.DataFrame,
) -> pd.DataFrame:
    if course_catalog.empty:
        return pd.DataFrame(columns=CSV_HEADERS["courses"])
    course_templates = _normalize_course_templates(course_catalog).sort_values(
        ["subject_code", "course_number", "course_id"]
    )
    rows = [
        {
            "Title": row["title"],
            "Abbreviation": row["course_id"],
            "Parent Academic Unit": row["default_department_abbreviation"],
            "Academic Unit Type": "Course",
            "Subject Code": row["subject_code"],
            "Course Number": row["course_number"],
        }
        for _, row in course_templates.iterrows()
    ]
    return pd.DataFrame(rows, columns=CSV_HEADERS["courses"])


def _normalize_course_templates(source_df: pd.DataFrame) -> pd.DataFrame:
    if source_df.empty:
        return pd.DataFrame(
            columns=[
                "subject_code",
                "course_number",
                "course_id",
                "title",
                "default_department_abbreviation",
            ]
        )

    if {"subject_code", "course_number", "course_id", "title"}.issubset(source_df.columns):
        normalized = source_df.copy()
        if "default_department_abbreviation" not in normalized.columns:
            normalized["default_department_abbreviation"] = ""
        return normalized[
            [
                "subject_code",
                "course_number",
                "course_id",
                "title",
                "default_department_abbreviation",
            ]
        ].fillna("")

    normalized = source_df.rename(
        columns={
            "Title": "title",
            "Abbreviation": "course_id",
            "Subject Code": "subject_code",
            "Course Number": "course_number",
            "Parent Academic Unit": "default_department_abbreviation",
        }
    ).copy()
    if "subject_code" not in normalized.columns:
        normalized["subject_code"] = normalized["course_id"].astype(str).str.extract(r"^([A-Za-z]+)", expand=False).fillna("")
    if "course_number" not in normalized.columns:
        normalized["course_number"] = normalized["course_id"].astype(str).str.extract(r"(\d+)$", expand=False).fillna("")
    if "default_department_abbreviation" not in normalized.columns:
        normalized["default_department_abbreviation"] = ""
    normalized["default_department_abbreviation"] = normalized.get(
        "default_department_abbreviation",
        "",
    )
    return normalized[
        [
            "subject_code",
            "course_number",
            "course_id",
            "title",
            "default_department_abbreviation",
        ]
    ].fillna("")


def _resolve_course_templates(
    config: GenerationConfig,
    course_catalog: pd.DataFrame,
    apply_course_limit: bool = True,
) -> pd.DataFrame:
    normalized_catalog = _normalize_course_templates(course_catalog)
    if normalized_catalog.empty:
        return normalized_catalog.iloc[0:0]
    if not apply_course_limit:
        return normalized_catalog
    normalized_catalog = normalized_catalog.sort_values(["subject_code", "course_number", "course_id"])
    if config.courses_count <= 0:
        return normalized_catalog.iloc[0:0]
    return normalized_catalog.head(config.courses_count)


def _build_sections(
    rng: random.Random,
    config: GenerationConfig,
    course_source: pd.DataFrame,
    attribute_catalog: pd.DataFrame,
    hierarchy_df: pd.DataFrame | None,
    terms_df: pd.DataFrame,
    snapshot: SnapshotContext,
    collisions: list[dict[str, str]],
    apply_course_limit: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    course_templates = _resolve_course_templates(config, course_source, apply_course_limit=apply_course_limit)
    if course_templates.empty:
        return (
            pd.DataFrame(columns=CSV_HEADERS["courses"]),
            pd.DataFrame(columns=CSV_HEADERS["course_sections"]),
        )

    hierarchy_context = None
    if hierarchy_df is not None and not hierarchy_df.empty:
        hierarchy_context = _build_hierarchy_context(validate_hierarchy(hierarchy_df))
    attribute_options = _attribute_options(attribute_catalog)

    course_rows = []
    section_rows = []
    existing_section_ids = set(snapshot.existing_section_ids)
    flat_course_id = config.institution_abbreviation
    for _, course in course_templates.iterrows():
        home_unit = (
            _resolve_course_home_unit(course, hierarchy_context, rng)
            if hierarchy_context is not None
            else ""
        )
        section_chain = (
            _section_hierarchy_titles_for_unit(home_unit, hierarchy_context, config.structure_mode)
            if hierarchy_context is not None and home_unit
            else {column: "" for column in HIERARCHY_LEVEL_COLUMNS}
        )
        course_rows.append(
            {
                "Title": course["title"],
                "Abbreviation": course["course_id"],
                "Parent Academic Unit": home_unit,
                "Academic Unit Type": "Course",
                "Subject Code": course["subject_code"],
                "Course Number": course["course_number"],
            }
        )

        for _, term in terms_df.iterrows():
            section_number = snapshot.next_section_number(course["course_id"], term["Title"])
            section_count = _randint_inclusive(
                rng,
                config.sections_per_course_min,
                config.sections_per_course_max,
            )
            for _ in range(section_count):
                section_id = f"{course['course_id']}-{section_number:02d}"
                while section_id in existing_section_ids:
                    collisions.append(
                        {"type": "section_id", "value": section_id, "resolution": "incremented"}
                    )
                    section_number += 1
                    section_id = f"{course['course_id']}-{section_number:02d}"
                existing_section_ids.add(section_id)

                overflow: dict[str, list[str]] = {}
                section_row = {
                    "Title": course["title"],
                    "Section ID": section_id,
                    "Course ID": flat_course_id if config.structure_mode == "flat" else course["course_id"],
                    "Term": term["Title"],
                    "Start Date": term["Start Date"],
                    "End Date": term["End Date"],
                    "Course": course["course_id"],
                    "Department": section_chain["Department"],
                    "Division": section_chain["Division"],
                    "College": section_chain["College"],
                    "University": section_chain["University"],
                    "Program": "",
                    "Campus": "",
                    "Session": "",
                    "Course Level": _course_level_from_number(course["course_number"]),
                    "Course Type": "",
                    "Delivery Method": "",
                    "Course Attributes": "",
                }

                if config.structure_mode == "flat" and rng.random() < config.flat_variance_rate:
                    if hierarchy_context is not None:
                        section_row = _apply_flat_variance(
                            rng,
                            section_row,
                            hierarchy_context,
                            section_chain["Department"],
                        )

                if config.include_section_attributes:
                    for column in GENERIC_ATTRIBUTE_COLUMNS:
                        fallback = section_row.get(column, "")
                        primary, extras = _sample_attribute(rng, attribute_options, column, fallback)
                        section_row[column] = primary or fallback
                        if extras:
                            overflow[column] = extras
                else:
                    for column in GENERIC_ATTRIBUTE_COLUMNS:
                        section_row[column] = ""

                if config.structure_mode == "hierarchy":
                    section_row["University"] = section_row["University"] or config.institution_name
                section_row["Course Attributes"] = (
                    _serialize_overflow(overflow)
                    if config.include_section_attributes
                    else ""
                )
                section_rows.append(section_row)
                section_number += 1

    return (
        pd.DataFrame(course_rows, columns=CSV_HEADERS["courses"]).drop_duplicates(),
        pd.DataFrame(section_rows, columns=CSV_HEADERS["course_sections"]),
    )


def _generate_people(
    rng: random.Random,
    config: GenerationConfig,
    count: int,
    role: str,
    attribute_catalog: pd.DataFrame,
    existing_emails: set[str],
    existing_school_ids: set[str],
    collisions: list[dict[str, str]],
) -> pd.DataFrame:
    options = _attribute_options(attribute_catalog)
    department_values = options.get("Department", {}).get("values", []) or ["Academic Affairs"]
    program_values = options.get("Program", {}).get("values", []) or ["General Studies"]
    rows = []
    for _ in range(count):
        first_name, last_name = _pick_name(rng, config)
        email = _next_unique_email(
            first_name,
            last_name,
            config.email_domain,
            existing_emails,
            collisions,
        )
        school_id = _next_unique_school_id(rng, existing_school_ids, collisions)
        gender = rng.choice(GENDERS)
        if role == "student":
            rows.append(
                {
                    "First Name": first_name,
                    "Last Name": last_name,
                    "School Id": school_id,
                    "Email": email,
                    "Password": f"pw{school_id}",
                    "Grade": rng.choice(STUDENT_GRADES),
                    "Gender": gender,
                    "Phone Number": f"{rng.randint(201, 989)}-{rng.randint(200, 999)}-{rng.randint(1000, 9999)}",
                    "Program": rng.choice(list(program_values)),
                }
            )
        else:
            rows.append(
                {
                    "First Name": first_name,
                    "Last Name": last_name,
                    "School Id": school_id,
                    "Email": email,
                    "Password": f"pw{school_id}",
                    "Gender": gender,
                    "Tenure Track": rng.choice(TENURE_TRACKS),
                    "Department": rng.choice(list(department_values)),
                }
            )
    key = "students" if role == "student" else "instructors"
    return pd.DataFrame(rows, columns=CSV_HEADERS[key])


def _build_assignments_and_enrollments(
    rng: random.Random,
    sections_df: pd.DataFrame,
    students_df: pd.DataFrame,
    instructors_df: pd.DataFrame,
    config: GenerationConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    enrollment_rows = []
    assignment_rows = []
    if sections_df.empty:
        return (
            pd.DataFrame(columns=CSV_HEADERS["student_enrollments"]),
            pd.DataFrame(columns=CSV_HEADERS["instructor_assignments"]),
        )

    student_emails = students_df["Email"].tolist()
    instructor_emails = instructors_df["Email"].tolist()

    student_emails = list(dict.fromkeys(email for email in student_emails if email))
    instructor_emails = list(dict.fromkeys(email for email in instructor_emails if email))

    for _, section in sections_df.iterrows():
        if instructor_emails:
            instructor_count = min(
                _randint_inclusive(
                    rng,
                    config.instructors_per_section_min,
                    config.instructors_per_section_max,
                ),
                len(instructor_emails),
            )
            for index, email in enumerate(rng.sample(instructor_emails, k=instructor_count)):
                assignment_rows.append(
                    {
                        "Academic Unit": section["Section ID"],
                        "Email": email,
                        "Role": "Primary Instructor" if index == 0 else "Teaching Assistant",
                        "Term": section["Term"],
                    }
                )

        if student_emails:
            seat_count = min(
                _randint_inclusive(
                    rng,
                    config.enrollments_per_section_min,
                    config.enrollments_per_section_max,
                ),
                len(student_emails),
            )
            for email in rng.sample(student_emails, k=seat_count):
                enrollment_rows.append(
                    {
                        "Academic Unit": section["Section ID"],
                        "Email": email,
                        "Role": "Student",
                        "Term": section["Term"],
                    }
                )

    return (
        pd.DataFrame(enrollment_rows, columns=CSV_HEADERS["student_enrollments"]),
        pd.DataFrame(assignment_rows, columns=CSV_HEADERS["instructor_assignments"]),
    )


def _inject_duplicates(
    rng: random.Random,
    files: dict[str, pd.DataFrame],
    duplicate_count: int,
) -> pd.DataFrame:
    duplicate_rows = []
    candidate_keys = [
        "students",
        "instructors",
        "course_sections",
        "student_enrollments",
        "instructor_assignments",
    ]
    available_keys = [key for key in candidate_keys if key in files and not files[key].empty]
    if not available_keys or duplicate_count <= 0:
        return pd.DataFrame(columns=["dataset", "row_index"])

    for _ in range(duplicate_count):
        dataset = rng.choice(available_keys)
        source_df = files[dataset]
        row_index = rng.randrange(len(source_df))
        duplicate_rows.append({"dataset": dataset, "row_index": row_index})
        files[dataset] = pd.concat(
            [files[dataset], source_df.iloc[[row_index]].copy()],
            ignore_index=True,
        )
    return pd.DataFrame(duplicate_rows)


def _export_zip(files: dict[str, pd.DataFrame]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for key, df in files.items():
            archive.writestr(FILE_STEMS[key], df.to_csv(index=False))
    return buffer.getvalue()


def generate_package(
    config: GenerationConfig,
    course_catalog: pd.DataFrame,
    attribute_catalog: pd.DataFrame,
    academic_unit_catalog: pd.DataFrame,
    snapshot: SnapshotContext | None = None,
) -> GenerationResult:
    snapshot = snapshot or SnapshotContext()
    plan = plan_generation(config.structure_mode, snapshot)
    rng = random.Random(config.seed)
    collisions: list[dict[str, str]] = []

    if plan.is_authoritative("terms"):
        terms_df = snapshot.tables["terms"].copy()
    elif plan.should_generate("terms"):
        terms_df = _build_terms(config, snapshot)
    else:
        terms_df = pd.DataFrame(columns=CSV_HEADERS["terms"])

    validated_hierarchy = pd.DataFrame(columns=CSV_HEADERS["hierarchy"])
    hierarchy_for_sections: pd.DataFrame | None = None
    if plan.is_authoritative("hierarchy"):
        config.hierarchy_source = "uploaded"
        validated_hierarchy = validate_hierarchy(snapshot.tables["hierarchy"].copy())
    elif plan.should_generate("hierarchy") or (
        config.structure_mode == "hierarchy"
        and not plan.is_authoritative("course_sections")
    ):
        config.hierarchy_source = "fallback"
        fallback_hierarchy = _apply_default_university_to_hierarchy(
            academic_unit_catalog.copy(),
            config,
        )
        validated_hierarchy = validate_hierarchy(fallback_hierarchy)

    if config.structure_mode == "hierarchy":
        hierarchy_for_sections = validated_hierarchy if not validated_hierarchy.empty else None
    elif plan.flat_hierarchy_source == "hierarchy":
        hierarchy_for_sections = validated_hierarchy if not validated_hierarchy.empty else None

    course_source = (
        snapshot.tables["courses"].copy()
        if plan.is_authoritative("courses")
        else course_catalog.copy()
    )
    if plan.is_authoritative("courses") and not validated_hierarchy.empty:
        validate_course_catalog(course_source, validated_hierarchy)

    courses_df = (
        snapshot.tables["courses"].copy()
        if plan.is_authoritative("courses")
        else pd.DataFrame(columns=CSV_HEADERS["courses"])
    )
    sections_df = (
        snapshot.tables["course_sections"].copy()
        if plan.is_authoritative("course_sections")
        else pd.DataFrame(columns=CSV_HEADERS["course_sections"])
    )
    if plan.should_generate("course_sections"):
        generated_courses_df, generated_sections_df = _build_sections(
            rng,
            config,
            course_source,
            attribute_catalog,
            hierarchy_for_sections,
            terms_df,
            snapshot,
            collisions,
            apply_course_limit=not plan.is_authoritative("courses"),
        )
        sections_df = generated_sections_df
        if plan.should_generate("courses"):
            courses_df = generated_courses_df

    students_df = (
        snapshot.tables["students"].copy()
        if plan.is_authoritative("students")
        else _generate_people(
            rng,
            config,
            config.student_count,
            "student",
            attribute_catalog,
            set(snapshot.existing_emails),
            set(snapshot.existing_school_ids),
            collisions,
        )
    )
    instructors_df = (
        snapshot.tables["instructors"].copy()
        if plan.is_authoritative("instructors")
        else _generate_people(
            rng,
            config,
            config.instructor_count,
            "instructor",
            attribute_catalog,
            set(snapshot.existing_emails).union(set(students_df["Email"].str.lower())),
            set(snapshot.existing_school_ids).union(set(students_df["School Id"])),
            collisions,
        )
    )

    enrollments_df = (
        snapshot.tables["student_enrollments"].copy()
        if plan.is_authoritative("student_enrollments")
        else pd.DataFrame(columns=CSV_HEADERS["student_enrollments"])
    )
    assignments_df = (
        snapshot.tables["instructor_assignments"].copy()
        if plan.is_authoritative("instructor_assignments")
        else pd.DataFrame(columns=CSV_HEADERS["instructor_assignments"])
    )
    if plan.should_generate("student_enrollments") or plan.should_generate("instructor_assignments"):
        generated_enrollments_df, generated_assignments_df = _build_assignments_and_enrollments(
            rng,
            sections_df,
            students_df,
            instructors_df,
            config,
        )
        if plan.should_generate("student_enrollments"):
            enrollments_df = generated_enrollments_df
        if plan.should_generate("instructor_assignments"):
            assignments_df = generated_assignments_df

    files: dict[str, pd.DataFrame] = {}
    for key in FILE_STEMS:
        if plan.is_authoritative(key):
            files[key] = snapshot.tables[key].copy()
        elif key == "terms" and plan.should_generate(key):
            files[key] = terms_df
        elif key == "courses" and (plan.should_generate(key) or plan.is_authoritative(key)):
            files[key] = courses_df
        elif key == "hierarchy" and (plan.should_generate(key) or plan.is_authoritative(key)):
            files[key] = validated_hierarchy
        elif key == "course_sections" and plan.should_generate(key):
            files[key] = sections_df
        elif key == "students" and plan.should_generate(key):
            files[key] = students_df
        elif key == "instructors" and plan.should_generate(key):
            files[key] = instructors_df
        elif key == "student_enrollments" and plan.should_generate(key):
            files[key] = enrollments_df
        elif key == "instructor_assignments" and plan.should_generate(key):
            files[key] = assignments_df

    duplicate_report = pd.DataFrame(columns=["dataset", "row_index"])
    if config.duplicate_mode:
        duplicate_report = _inject_duplicates(rng, files, config.duplicate_count)

    ordered_files = {
        key: files[key]
        for key in FILE_STEMS
        if key in files
    }
    summary = pd.DataFrame(
        [
            {"dataset": key, "rows": len(df), "file_name": FILE_STEMS[key]}
            for key, df in ordered_files.items()
        ]
    )
    collision_report = pd.DataFrame(collisions, columns=["type", "value", "resolution"])
    zip_bytes = _export_zip(ordered_files)

    return GenerationResult(
        files=ordered_files,
        summary=summary,
        collision_report=collision_report,
        duplicate_report=duplicate_report,
        zip_bytes=zip_bytes,
    )
