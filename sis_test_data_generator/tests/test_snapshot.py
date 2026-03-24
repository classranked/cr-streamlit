from __future__ import annotations

import io
import unittest

from sis_test_data_generator.sis_generator.snapshot import load_snapshot_files


class UploadedFile:
    def __init__(self, name: str, content: str) -> None:
        self.name = name
        self._content = content.encode("utf-8")

    def getvalue(self) -> bytes:
        return self._content


class SnapshotTests(unittest.TestCase):
    def test_header_based_classification_ignores_column_order(self) -> None:
        terms_csv = (
            "End Date,Title,Start Date\n"
            "12/15/2026 05:00 PM,2026 Fall,08/28/2026 08:00 AM\n"
        )

        context = load_snapshot_files([UploadedFile("unknown-upload.csv", terms_csv)])

        self.assertFalse(context.errors)
        self.assertIn("terms", context.tables)
        self.assertEqual(
            list(context.tables["terms"].columns),
            ["Title", "Start Date", "End Date"],
        )

    def test_known_filename_allows_reordered_columns_and_extra_columns(self) -> None:
        students_csv = (
            "Program,First Name,Last Name,Email,Password,Grade,Phone Number,Gender,School Id,Extra Column\n"
            "Business,Ada,Lovelace,ada@example.com,secret,12,555-0101,F,12345,ignored\n"
        )

        context = load_snapshot_files([UploadedFile("students.csv", students_csv)])

        self.assertFalse(context.errors)
        self.assertIn("students", context.tables)
        self.assertEqual(
            list(context.tables["students"].columns),
            ["First Name", "Last Name", "School Id", "Email", "Password", "Grade", "Gender", "Phone Number", "Program"],
        )
        self.assertNotIn("Extra Column", context.tables["students"].columns)
        self.assertEqual(context.tables["students"].iloc[0]["First Name"], "Ada")

    def test_missing_required_column_fails_validation(self) -> None:
        students_csv = (
            "First Name,Last Name,Email,Password,Grade,Gender,Phone Number,Program\n"
            "Ada,Lovelace,ada@example.com,secret,12,F,555-0101,Business\n"
        )

        context = load_snapshot_files([UploadedFile("students.csv", students_csv)])

        self.assertFalse(context.tables)
        self.assertEqual(len(context.errors), 1)
        self.assertIn("missing required columns for students", context.errors[0])
        self.assertIn("School Id", context.errors[0])

    def test_legacy_course_sections_are_normalized_with_course_attributes(self) -> None:
        legacy_csv = io.StringIO()
        legacy_csv.write(
            "Department,Title,Section ID,Course ID,Term,Start Date,End Date,Course,Division,College,University,Program,Campus,Session,Course Level,Course Type,Delivery Method\n"
        )
        legacy_csv.write(
            "Computer Science,Intro to CS,CS101-001,CS101,2026 Fall,08/28/2026 08:00 AM,12/15/2026 05:00 PM,CS101,School of Computing,College of Engineering,ClassRanked University,Computer Science,Boston,Fall 1,100 Level,Lecture,On-Campus\n"
        )

        context = load_snapshot_files([UploadedFile("course-sections.csv", legacy_csv.getvalue())])

        self.assertFalse(context.errors)
        self.assertIn("Course Attributes", context.tables["course_sections"].columns)
        self.assertEqual(context.tables["course_sections"].iloc[0]["Course Attributes"], "")
        self.assertEqual(
            list(context.tables["course_sections"].columns[-2:]),
            ["Delivery Method", "Course Attributes"],
        )

    def test_hierarchy_csv_is_classified(self) -> None:
        hierarchy_csv = (
            "Title,Abbreviation,Parent Academic Unit,Academic Unit Type\n"
            "ClassRanked University,CRU,,University\n"
        )

        context = load_snapshot_files([UploadedFile("hierarchy.csv", hierarchy_csv)])

        self.assertFalse(context.errors)
        self.assertIn("hierarchy", context.tables)

    def test_course_catalog_schema_can_drive_courses_upload(self) -> None:
        courses_csv = (
            "Title,Abbreviation,Parent Academic Unit,Academic Unit Type,Subject Code,Course Number\n"
            "Imported Biology,BIO150,BIO,Course,BIO,150\n"
        )

        context = load_snapshot_files([UploadedFile("courses.csv", courses_csv)])

        self.assertFalse(context.errors)
        self.assertIn("courses", context.tables)
        self.assertEqual(
            list(context.tables["courses"].columns),
            ["Title", "Abbreviation", "Parent Academic Unit", "Academic Unit Type", "Subject Code", "Course Number"],
        )
        self.assertEqual(context.tables["courses"].iloc[0]["Abbreviation"], "BIO150")

    def test_standard_courses_csv_preserves_parent_academic_unit(self) -> None:
        courses_csv = (
            "Title,Abbreviation,Parent Academic Unit,Academic Unit Type,Subject Code,Course Number\n"
            "Imported Biology,BIO150,BIO,Course,BIO,150\n"
        )

        context = load_snapshot_files([UploadedFile("courses.csv", courses_csv)])

        self.assertFalse(context.errors)
        self.assertIn("courses", context.tables)
        self.assertEqual(context.tables["courses"].iloc[0]["Abbreviation"], "BIO150")
        self.assertEqual(context.tables["courses"].iloc[0]["Parent Academic Unit"], "BIO")

    def test_courses_upload_requires_canonical_parent_column(self) -> None:
        courses_csv = (
            "Title,Abbreviation,Academic Unit Type,Subject Code,Course Number\n"
            "Imported Biology,BIO150,Course,BIO,150\n"
        )

        context = load_snapshot_files([UploadedFile("courses.csv", courses_csv)])

        self.assertFalse(context.tables)
        self.assertEqual(len(context.errors), 1)
        self.assertIn("missing required columns for courses", context.errors[0])
        self.assertIn("Parent Academic Unit", context.errors[0])


if __name__ == "__main__":
    unittest.main()
