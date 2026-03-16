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
    def test_legacy_course_sections_are_normalized_with_course_attributes(self) -> None:
        legacy_csv = io.StringIO()
        legacy_csv.write(
            "Title,Section ID,Course ID,Term,Start Date,End Date,Course,Department,Division,College,University,Program,Campus,Session,Course Level,Course Type,Delivery Method\n"
        )
        legacy_csv.write(
            "Intro to CS,CS101-001,CS101,2026 Fall,08/28/2026 08:00 AM,12/15/2026 05:00 PM,CS101,Computer Science,School of Computing,College of Engineering,ClassRanked University,Computer Science,Boston,Fall 1,100 Level,Lecture,On-Campus\n"
        )

        context = load_snapshot_files([UploadedFile("course-sections.csv", legacy_csv.getvalue())])

        self.assertFalse(context.errors)
        self.assertIn("Course Attributes", context.tables["course_sections"].columns)
        self.assertEqual(context.tables["course_sections"].iloc[0]["Course Attributes"], "")

    def test_hierarchy_csv_is_classified(self) -> None:
        hierarchy_csv = (
            "Title,Abbreviation,Parent Academic Unit,Academic Unit Type\n"
            "ClassRanked University,CRU,,University\n"
        )

        context = load_snapshot_files([UploadedFile("hierarchy.csv", hierarchy_csv)])

        self.assertFalse(context.errors)
        self.assertIn("hierarchy", context.tables)


if __name__ == "__main__":
    unittest.main()
