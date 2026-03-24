from __future__ import annotations

import unittest

import pandas as pd

from sis_test_data_generator.sis_generator.constants import CSV_HEADERS
from sis_test_data_generator.sis_generator.generator import (
    GenerationConfig,
    generate_package,
    validate_course_catalog,
    validate_hierarchy,
)
from sis_test_data_generator.sis_generator.snapshot import SnapshotContext, load_snapshot_files


class UploadedFile:
    def __init__(self, name: str, content: str) -> None:
        self.name = name
        self._content = content.encode("utf-8")

    def getvalue(self) -> bytes:
        return self._content


def sample_course_catalog(default_department: str = "CS") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "subject_code": "CS",
                "course_number": "101",
                "course_id": "CS101",
                "title": "Intro to CS",
                "default_department_abbreviation": default_department,
            },
            {
                "subject_code": "MATH",
                "course_number": "201",
                "course_id": "MATH201",
                "title": "Discrete Math",
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


def uploaded_flat_hierarchy() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Title": "College of Analytics",
                "Abbreviation": "COA",
                "Parent Academic Unit": "",
                "Academic Unit Type": "College",
            },
            {
                "Title": "Analytics Division",
                "Abbreviation": "ANL",
                "Parent Academic Unit": "COA",
                "Academic Unit Type": "Division",
            },
            {
                "Title": "Data Science Cluster",
                "Abbreviation": "DSC",
                "Parent Academic Unit": "ANL",
                "Academic Unit Type": "Program Cluster",
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


def uploaded_courses() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Title": "Imported Biology",
                "Abbreviation": "BIO150",
                "Parent Academic Unit": "DS",
                "Academic Unit Type": "Course",
                "Subject Code": "BIO",
                "Course Number": "150",
            }
        ],
        columns=["Title", "Abbreviation", "Parent Academic Unit", "Academic Unit Type", "Subject Code", "Course Number"],
    )


def uploaded_terms() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Title": "2026 Fall",
                "Start Date": "08/28/2026 08:00 AM",
                "End Date": "12/15/2026 05:00 PM",
            }
        ],
        columns=CSV_HEADERS["terms"],
    )


def uploaded_course_sections() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Title": "Imported Biology",
                "Section ID": "BIO150-01",
                "Course ID": "BIO150",
                "Term": "2026 Fall",
                "Start Date": "08/28/2026 08:00 AM",
                "End Date": "12/15/2026 05:00 PM",
                "Course": "BIO150",
                "Department": "Data Science",
                "Division": "School of Computing",
                "College": "College of Engineering",
                "University": "Atlas State University",
                "Program": "",
                "Campus": "",
                "Session": "",
                "Course Level": "100 Level",
                "Course Type": "",
                "Delivery Method": "",
                "Course Attributes": "",
            }
        ],
        columns=CSV_HEADERS["course_sections"],
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
        self.assertTrue((sections["University"] == "").all())
        self.assertTrue((sections["Course ID"] == "CRU").all())
        self.assertTrue((sections["Course"] == "CS101").all())

    def test_flat_mode_uses_uploaded_hierarchy_for_section_fields(self) -> None:
        config = base_config("flat")
        config.flat_variance_rate = 0.0
        flat_courses = pd.DataFrame(
            [
                {
                    "Title": "Imported Biology",
                    "Abbreviation": "BIO150",
                    "Parent Academic Unit": "DSC",
                    "Academic Unit Type": "Course",
                    "Subject Code": "BIO",
                    "Course Number": "150",
                }
            ],
            columns=CSV_HEADERS["courses"],
        )
        snapshot = SnapshotContext(
            tables={
                "courses": flat_courses,
                "hierarchy": uploaded_flat_hierarchy(),
            }
        )

        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        section = result.files["course_sections"].iloc[0]
        self.assertEqual(section["College"], "College of Analytics")
        self.assertEqual(section["Division"], "Analytics Division")
        self.assertEqual(section["Department"], "Data Science Cluster")
        self.assertEqual(section["University"], "")

    def test_flat_mode_without_uploaded_hierarchy_leaves_hierarchy_fields_blank(self) -> None:
        config = base_config("flat")
        config.flat_variance_rate = 0.0
        snapshot = SnapshotContext(tables={"courses": uploaded_courses()})

        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        section = result.files["course_sections"].iloc[0]
        self.assertEqual(section["Department"], "")
        self.assertEqual(section["Division"], "")
        self.assertEqual(section["College"], "")
        self.assertEqual(section["University"], "")

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

    def test_uploaded_courses_drive_section_generation(self) -> None:
        snapshot = SnapshotContext(tables={"courses": uploaded_courses()})
        result = generate_package(
            config=base_config("hierarchy"),
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertEqual(
            list(result.files["courses"]["Abbreviation"]),
            ["BIO150"],
        )
        self.assertTrue((result.files["course_sections"]["Course ID"] == "BIO150").all())
        self.assertEqual(
            result.files["course_sections"].iloc[0]["Section ID"],
            "BIO150-01",
        )
        self.assertTrue((result.files["course_sections"]["Title"] == "Imported Biology").all())
        self.assertEqual(
            result.files["course_sections"].iloc[0]["Department"],
            "Data Science",
        )
        self.assertEqual(
            result.files["courses"].iloc[0]["Parent Academic Unit"],
            "DS",
        )

    def test_generator_limits_catalog_courses_to_requested_count(self) -> None:
        config = base_config("hierarchy")
        config.courses_count = 1
        config.sections_per_course_min = 1
        config.sections_per_course_max = 1

        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=SnapshotContext(),
        )

        self.assertEqual(
            set(result.files["courses"]["Abbreviation"]),
            {"CS101"},
        )
        self.assertEqual(
            set(result.files["course_sections"]["Course ID"]),
            {"CS101"},
        )

    def test_uploaded_courses_ignore_requested_course_count(self) -> None:
        config = base_config("hierarchy")
        config.courses_count = 0
        config.sections_per_course_min = 1
        config.sections_per_course_max = 1
        snapshot = SnapshotContext(tables={"courses": uploaded_courses()})

        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertEqual(
            set(result.files["courses"]["Abbreviation"]),
            {"BIO150"},
        )
        self.assertEqual(
            set(result.files["course_sections"]["Course ID"]),
            {"BIO150"},
        )

    def test_uploaded_courses_preserve_input_order_for_section_generation(self) -> None:
        config = base_config("hierarchy")
        config.sections_per_course_min = 1
        config.sections_per_course_max = 1
        ordered_courses = pd.DataFrame(
            [
                {
                    "Title": "Second Course",
                    "Abbreviation": "ZZZ200",
                    "Parent Academic Unit": "DS",
                    "Academic Unit Type": "Course",
                    "Subject Code": "ZZZ",
                    "Course Number": "200",
                },
                {
                    "Title": "First Course",
                    "Abbreviation": "AAA100",
                    "Parent Academic Unit": "DS",
                    "Academic Unit Type": "Course",
                    "Subject Code": "AAA",
                    "Course Number": "100",
                },
            ],
            columns=CSV_HEADERS["courses"],
        )
        snapshot = SnapshotContext(tables={"courses": ordered_courses})

        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertEqual(
            result.files["course_sections"]["Course"].head(2).tolist(),
            ["ZZZ200", "AAA100"],
        )

    def test_flat_mode_uploaded_courses_keep_university_course_id(self) -> None:
        config = base_config("flat")
        config.sections_per_course_min = 1
        config.sections_per_course_max = 1
        snapshot = SnapshotContext(tables={"courses": uploaded_courses()})

        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertEqual(
            set(result.files["course_sections"]["Course ID"]),
            {"CRU"},
        )
        self.assertEqual(
            set(result.files["course_sections"]["Course"]),
            {"BIO150"},
        )

    def test_hierarchy_mode_uses_sequential_mapping_for_nonstandard_hierarchy_types(self) -> None:
        config = base_config("hierarchy")
        config.sections_per_course_min = 1
        config.sections_per_course_max = 1
        flat_courses = pd.DataFrame(
            [
                {
                    "Title": "Imported Biology",
                    "Abbreviation": "BIO150",
                    "Parent Academic Unit": "DSC",
                    "Academic Unit Type": "Course",
                    "Subject Code": "BIO",
                    "Course Number": "150",
                }
            ],
            columns=CSV_HEADERS["courses"],
        )
        snapshot = SnapshotContext(
            tables={
                "courses": flat_courses,
                "hierarchy": uploaded_flat_hierarchy(),
            }
        )

        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        section = result.files["course_sections"].iloc[0]
        self.assertEqual(section["University"], "ClassRanked University")
        self.assertEqual(section["College"], "College of Analytics")
        self.assertEqual(section["Division"], "Analytics Division")
        self.assertEqual(section["Department"], "Data Science Cluster")

    def test_uploaded_courses_with_extra_columns_remain_authoritative(self) -> None:
        config = base_config("hierarchy")
        config.sections_per_course_min = 1
        config.sections_per_course_max = 1
        uploaded_csv = (
            "Title,Abbreviation,Academic Unit Type,Parent Academic Unit,Institution ID,Integration Source ID\n"
            "Sensory Evaluation & Analysis of Beer,BRW270,Course,DS,,\n"
        )
        snapshot = load_snapshot_files([UploadedFile("courses.csv", uploaded_csv)])

        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertFalse(snapshot.errors)
        self.assertEqual(
            set(result.files["courses"]["Abbreviation"]),
            {"BRW270"},
        )
        self.assertEqual(
            set(result.files["course_sections"]["Course ID"]),
            {"BRW270"},
        )

    def test_attributes_can_be_disabled(self) -> None:
        config = base_config("hierarchy")
        config.include_section_attributes = False

        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=SnapshotContext(),
        )

        section = result.files["course_sections"].iloc[0]
        self.assertEqual(section["Program"], "")
        self.assertEqual(section["Campus"], "")
        self.assertEqual(section["Session"], "")
        self.assertEqual(section["Course Level"], "")
        self.assertEqual(section["Course Type"], "")
        self.assertEqual(section["Delivery Method"], "")
        self.assertEqual(section["Course Attributes"], "")
        self.assertEqual(section["Department"], "Computer Science")

    def test_delta_mode_uses_uploaded_people_for_assignments_and_enrollments(self) -> None:
        config = base_config("hierarchy")
        config.mode = "delta"
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

        self.assertEqual(result.files["students"].equals(uploaded_students()), True)
        self.assertEqual(result.files["instructors"].equals(uploaded_instructors()), True)
        self.assertEqual(
            set(result.files["student_enrollments"]["Email"].tolist()),
            {"ada@classranked.edu"},
        )
        self.assertEqual(
            set(result.files["instructor_assignments"]["Email"].tolist()),
            {"grace@classranked.edu"},
        )

    def test_course_sections_snapshot_skips_courses_hierarchy_and_terms_generation(self) -> None:
        snapshot = SnapshotContext(
            tables={
                "course_sections": uploaded_course_sections(),
                "students": uploaded_students(),
                "instructors": uploaded_instructors(),
            }
        )
        result = generate_package(
            config=base_config("hierarchy"),
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertEqual(result.files["course_sections"].equals(uploaded_course_sections()), True)
        self.assertEqual(result.files["students"].equals(uploaded_students()), True)
        self.assertEqual(result.files["instructors"].equals(uploaded_instructors()), True)
        self.assertNotIn("courses", result.files)
        self.assertNotIn("hierarchy", result.files)
        self.assertNotIn("terms", result.files)
        self.assertEqual(
            set(result.files["student_enrollments"]["Academic Unit"].tolist()),
            {"BIO150-01"},
        )
        self.assertEqual(
            set(result.files["instructor_assignments"]["Academic Unit"].tolist()),
            {"BIO150-01"},
        )

    def test_most_downstream_structural_upload_wins(self) -> None:
        snapshot = SnapshotContext(
            tables={
                "hierarchy": uploaded_hierarchy(),
                "courses": uploaded_courses(),
                "course_sections": uploaded_course_sections(),
                "students": uploaded_students(),
                "instructors": uploaded_instructors(),
            }
        )

        result = generate_package(
            config=base_config("hierarchy"),
            course_catalog=sample_course_catalog(default_department="CS"),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertEqual(result.files["course_sections"].equals(uploaded_course_sections()), True)
        self.assertEqual(result.files["courses"].equals(uploaded_courses()), True)
        self.assertEqual(result.files["hierarchy"].equals(uploaded_hierarchy()), True)
        self.assertEqual(
            set(result.files["student_enrollments"]["Academic Unit"].tolist()),
            {"BIO150-01"},
        )

    def test_terms_snapshot_is_reused_for_section_generation(self) -> None:
        config = base_config("hierarchy")
        config.sections_per_course_min = 1
        config.sections_per_course_max = 1
        snapshot = SnapshotContext(tables={"terms": uploaded_terms()})
        result = generate_package(
            config=config,
            course_catalog=sample_course_catalog(),
            attribute_catalog=sample_attribute_catalog(),
            academic_unit_catalog=fallback_hierarchy(),
            snapshot=snapshot,
        )

        self.assertEqual(result.files["terms"].equals(uploaded_terms()), True)
        self.assertEqual(
            set(result.files["course_sections"]["Term"].tolist()),
            {"2026 Fall"},
        )
        self.assertEqual(
            set(result.files["course_sections"]["Start Date"].tolist()),
            {"08/28/2026 08:00 AM"},
        )

    def test_full_package_honors_snapshot_students_and_instructors(self) -> None:
        config = base_config("hierarchy")
        config.sections_per_course_min = 1
        config.sections_per_course_max = 1
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

        self.assertEqual(result.files["students"].equals(uploaded_students()), True)
        self.assertEqual(result.files["instructors"].equals(uploaded_instructors()), True)
        self.assertEqual(
            set(result.files["student_enrollments"]["Email"].tolist()),
            {"ada@classranked.edu"},
        )
        self.assertEqual(
            set(result.files["instructor_assignments"]["Email"].tolist()),
            {"grace@classranked.edu"},
        )

    def test_validate_course_catalog_rejects_missing_parent_unit(self) -> None:
        invalid = uploaded_courses().copy()
        invalid.loc[0, "Parent Academic Unit"] = "MISSING"

        with self.assertRaises(ValueError):
            validate_course_catalog(invalid, fallback_hierarchy())


if __name__ == "__main__":
    unittest.main()
