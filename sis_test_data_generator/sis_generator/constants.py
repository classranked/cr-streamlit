from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

CSV_HEADERS = {
    "terms": ["Title", "Start Date", "End Date"],
    "courses": [
        "Title",
        "Abbreviation",
        "Parent Academic Unit",
        "Academic Unit Type",
        "Subject Code",
        "Course Number",
    ],
    "hierarchy": [
        "Title",
        "Abbreviation",
        "Parent Academic Unit",
        "Academic Unit Type",
    ],
    "course_sections": [
        "Title",
        "Section ID",
        "Course ID",
        "Term",
        "Start Date",
        "End Date",
        "Course",
        "Department",
        "Division",
        "College",
        "University",
        "Program",
        "Campus",
        "Session",
        "Course Level",
        "Course Type",
        "Delivery Method",
        "Course Attributes",
    ],
    "instructors": [
        "First Name",
        "Last Name",
        "School Id",
        "Email",
        "Password",
        "Gender",
        "Tenure Track",
        "Department",
    ],
    "students": [
        "First Name",
        "Last Name",
        "School Id",
        "Email",
        "Password",
        "Grade",
        "Gender",
        "Phone Number",
        "Program",
    ],
    "student_enrollments": ["Academic Unit", "Email", "Role", "Term"],
    "instructor_assignments": ["Academic Unit", "Email", "Role", "Term"],
}

COURSE_REQUIRED_COLUMNS = [
    "Title",
    "Abbreviation",
    "Parent Academic Unit",
    "Academic Unit Type",
]

COURSE_OPTIONAL_COLUMNS = ["Subject Code", "Course Number"]

LEGACY_CSV_HEADERS = {
    "courses": ["Title", "Abbreviation", "Academic Unit Type", "Parent Academic Unit"],
    "course_sections": [
        "Title",
        "Section ID",
        "Course ID",
        "Term",
        "Start Date",
        "End Date",
        "Course",
        "Department",
        "Division",
        "College",
        "University",
        "Program",
        "Campus",
        "Session",
        "Course Level",
        "Course Type",
        "Delivery Method",
    ],
}

FILE_STEMS = {
    "terms": "terms.csv",
    "courses": "courses.csv",
    "hierarchy": "hierarchy.csv",
    "course_sections": "course-sections.csv",
    "instructors": "instructors.csv",
    "students": "students.csv",
    "student_enrollments": "student-enrollments.csv",
    "instructor_assignments": "instructor-assignments.csv",
}

DISPLAY_NAMES = {
    "terms": "Terms",
    "courses": "Courses",
    "hierarchy": "Hierarchy",
    "course_sections": "Course Sections",
    "instructors": "Instructors",
    "students": "Students",
    "student_enrollments": "Student Enrollments",
    "instructor_assignments": "Instructor Assignments",
}

UPLOAD_NAME_MAP = {
    "terms": "terms",
    "courses": "courses",
    "hierarchy": "hierarchy",
    "course-sections": "course_sections",
    "course_sections": "course_sections",
    "instructors": "instructors",
    "faculty-and-staff": "instructors",
    "students": "students",
    "student-enrollments": "student_enrollments",
    "student_enrollments": "student_enrollments",
    "instructor-assignments": "instructor_assignments",
    "instructor_assignments": "instructor_assignments",
}

COURSE_CATALOG_COLUMNS = COURSE_REQUIRED_COLUMNS + COURSE_OPTIONAL_COLUMNS

ATTRIBUTE_CATALOG_COLUMNS = [
    "attribute_name",
    "column_name",
    "value",
    "weight",
    "allow_overflow",
    "is_hierarchy_level",
]

ACADEMIC_UNIT_CATALOG_COLUMNS = CSV_HEADERS["hierarchy"]

CATALOG_FILES = {
    "course_catalog": DATA_DIR / "course_catalog.csv",
    "attribute_catalog": DATA_DIR / "attribute_catalog.csv",
    "academic_unit_catalog": DATA_DIR / "academic_unit_catalog.csv",
    "legacy_master_catalog": DATA_DIR / "master_course_catalog.csv",
}

CATALOG_DISPLAY_NAMES = {
    "course_catalog": "Course Catalog",
    "attribute_catalog": "Attribute Catalog",
    "academic_unit_catalog": "Academic Unit Catalog",
}

SECTION_ATTRIBUTE_COLUMNS = [
    "Department",
    "Division",
    "College",
    "University",
    "Program",
    "Campus",
    "Session",
    "Course Level",
    "Course Type",
    "Delivery Method",
]

HIERARCHY_LEVEL_COLUMNS = ["Department", "Division", "College", "University"]

NAME_COLUMNS = ["name", "weight"]
EDGE_CASE_COLUMNS = ["first_name", "last_name", "label"]
