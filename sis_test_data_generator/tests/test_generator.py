from __future__ import annotations

import unittest

import pandas as pd

from sis_test_data_generator.sis_generator.constants import CSV_HEADERS
from sis_test_data_generator.sis_generator.generator import (
    GenerationConfig,
    generate_package,
    validate_hierarchy,
)
from sis_test_data_generator.sis_generator.snapshot import SnapshotContext


def sample_course_catalog(default_department: str = "CS") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "subject_code": "CS",
                "course_number": "101",
                "course_id": "CS101",
                "title": "Intro to CS",
                "default_department_abbreviation": default_department,
            }
        ]
    )


def sample_attribute_catalog() -> pd.DataFrame:
    rows = []
    for attribute_name, values in {
        "Department": ["Computer Science", "Data Science"],
        "Division": ["School of Computing", "Analytics Division"],
        "College": ["College of Engineering"],
        "University": ["ClassRanked University"],
        "Program": ["Computer Science", "Data Science"],
        "Campus": ["Boston", "Online"],
        "Session": ["Fall 1", "Full Term"],
        "Course Level": ["100 Level"],
        "Course Type": ["Lecture", "Lab"],
        "Delivery Method": ["On-Campus", "Online"],
    }.items():
        for value in values:
            rows.append(
                {
                    "attribute_name": attribute_name,
                    "column_name": attribute_name,
                    "value": value,
                    "weight": "5",
                    "allow_overflow": "true",
                    "is_hierarchy_level": "true" if attribute_name in {"Department", "Division", "College", "University"} else "false",
                }
            )
    return pd.DataFrame(rows)


def fallback_hierarchy() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Title": "Atlas State University",
                "Abbreviation": "ASU",
                "Parent Academic Unit": "",
                "Academic Unit Type": "University",
            },
            {
                "Title": "College of Engineering",
                "Abbreviation": "COE",
                "Parent Academic Unit": "ASU",
                "Academic Unit Type": "College",
            },
            {
                "Title": "School of Computing",
                "Abbreviation": "SOC",
                "Parent Academic Unit": "COE",
                "Academic Unit Type": "Division",
            },
            {
                "Title": "Computer Science",
                "Abbreviation": "CS",
                "Parent Academic Unit": "SOC",
                "Academic Unit Type": "Department",
            },
            {
                "Title": "Data Science",
                "Abbreviation": "DS",
                "Parent Academic Unit": "SOC",
                "Academic Unit Type": "Department",
            },
        ],
        columns=CSV_HEADERS["hierarchy"],
    )


def uploaded_hierarchy() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Title": "Atlas State University",
                "Abbreviation": "ASU",
                "Parent Academic Unit": "",
                "Academic Unit Type": "University",
            },
            {
                "Title": "College of Analytics",
                "Abbreviation": "COA",
                "Parent Academic Unit": "ASU",
                "Academic Unit Type": "College",
            },
            {
                "Title": "Analytics Division",
                "Abbreviation": "ANL",
                "Parent Academic Unit": "COA",
                "Academic Unit Type": "Division",
            },
            {
                "Title": "Data Science",
                "Abbreviation": "DS",
                "Parent Academic Unit": "ANL",
                "Academic Unit Type": "Department",
            },
        ],
        columns=CSV_HEADERS["hierarchy"],
    )


def uploaded_students() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "First Name": "Ada",
                "Last Name": "Lovelace",
                "School Id": "100001",
                "Email": "ada@classranked.edu",
                "Password": "pw100001",
                "Grade": "Senior",
                "Gender": "Female",
                "Phone Number": "555-010-1001",
                "Program": "Computer Science",
            }
        ],
        columns=CSV_HEADERS["students"],
    )


def uploaded_instructors() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "First Name": "Grace",
                "Last Name": "Hopper",
                "School Id": "200001",
                "Email": "grace@classranked.edu",
                "Password": "pw200001",
                "Gender": "Female",
                "Tenure Track": "Full",
                "Department": "Computer Science",
            }
        ],
        columns=CSV_HEADERS["instructors"],
    )


def base_config(structure_mode: str) -> GenerationConfig:
    return GenerationConfig(
        mode="full_package",
        structure_mode=structure_mode,
        seed=7,
        term_count=1,
        term_system="semester",
        start_term_label="Fall",
        start_term_year=2026,
        courses_count=1,
        sections_per_course_min=10,
        sections_per_course_max=10,
        student_count=5,
        instructor_count=3,
        enrollments_per_section_min=1,
        enrollments_per_section_max=2,
        instructors_per_section_min=1,
        instructors_per_section_max=1,
        duplicate_mode=False,
        duplicate_count=0,
        edge_case_rate=0.0,
        email_domain="classranked.edu",
        institution_name="ClassRanked University",
        institution_abbreviation="CRU",
        flat_variance_rate=1.0,
    )


class GeneratorTests(unittest.TestCase):
    def test_hierarchy_mode_uses_fallback_when_no_upload_exists(self) -> None:
        result = generate_package(
            config=base_config("hierarchy"),
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=SnapshotContext(),
        )

        self.assertIn("hierarchy", result.files)
        self.assertEqual(result.files["hierarchy"].iloc[0]["Title"], "ClassRanked University")
        self.assertEqual(result.files["hierarchy"].iloc[0]["Abbreviation"], "CRU")
        hierarchy_types = result.files["hierarchy"]["Academic Unit Type"].tolist()
        self.assertEqual(
            hierarchy_types,
            ["University"] + ["College"] * 1 + ["Division"] * 1 + ["Department"] * 2,
        )
        self.assertEqual(
            result.files["course_sections"].iloc[0]["Department"],
            "Computer Science",
        )

    def test_hierarchy_mode_prefers_uploaded_hierarchy(self) -> None:
        snapshot = SnapshotContext(tables={"hierarchy": uploaded_hierarchy()})
        result = generate_package(
            config=base_config("hierarchy"),
            course_catalog=sample_course_catalog(default_department="DS"),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertEqual(
            list(result.files["hierarchy"]["Abbreviation"]),
            list(uploaded_hierarchy()["Abbreviation"]),
        )
        self.assertEqual(result.files["hierarchy"].iloc[0]["Title"], "Atlas State University")
        self.assertEqual(
            result.files["course_sections"].iloc[0]["Division"],
            "Analytics Division",
        )
        self.assertEqual(
            result.files["hierarchy"]["Academic Unit Type"].tolist(),
            ["University", "College", "Division", "Department"],
        )

    def test_validate_hierarchy_rejects_missing_parent(self) -> None:
        invalid = fallback_hierarchy().copy()
        invalid.loc[1, "Parent Academic Unit"] = "MISSING"
        with self.assertRaises(ValueError):
            validate_hierarchy(invalid)

    def test_flat_mode_applies_controlled_variance(self) -> None:
        result = generate_package(
            config=base_config("flat"),
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=SnapshotContext(),
        )

        self.assertNotIn("courses", result.files)
        self.assertNotIn("hierarchy", result.files)
        sections = result.files["course_sections"]
        self.assertTrue((sections["Department"] == "").all())
        self.assertTrue((sections["Division"] == "").all())
        self.assertTrue((sections["College"] == "").all())
        self.assertTrue((sections["University"] == "ClassRanked University").all())

    def test_attribute_overflow_serializes_into_course_attributes(self) -> None:
        result = generate_package(
            config=base_config("flat"),
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=SnapshotContext(),
        )

        course_attributes = result.files["course_sections"]["Course Attributes"].tolist()
        self.assertTrue(any("Campus:" in value for value in course_attributes))

    def test_delta_mode_uses_uploaded_people_for_assignments_and_enrollments(self) -> None:
        config = base_config("hierarchy")
        config.mode = "delta"
        config.student_count = 0
        config.instructor_count = 0
        config.sections_per_course_min = 1
        config.sections_per_course_max = 1
        config.enrollments_per_section_min = 1
        config.enrollments_per_section_max = 1
        config.instructors_per_section_min = 1
        config.instructors_per_section_max = 1

        snapshot = SnapshotContext(
            tables={
                "students": uploaded_students(),
                "instructors": uploaded_instructors(),
            }
        )
        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertEqual(result.files["students"].empty, True)
        self.assertEqual(result.files["instructors"].empty, True)
        self.assertEqual(
            result.files["student_enrollments"]["Email"].tolist(),
            ["ada@classranked.edu"],
        )
        self.assertEqual(
            result.files["instructor_assignments"]["Email"].tolist(),
            ["grace@classranked.edu"],
        )


if __name__ == "__main__":
    unittest.main()
