# SIS Test Data Generator Feature Specs

This document captures the agreed product behavior for the SIS Test Data Generator so future changes can be evaluated against a stable reference.

## Primary Use Cases

1. Generate a full fallback-themed SIS demo package from scratch for internal demos.
2. Generate a new term of data for an existing school by reusing uploaded SIS exports as authoritative inputs and generating only the missing downstream files.

## Datasets

The app works with these datasets:

- `terms.csv`
- `courses.csv`
- `hierarchy.csv`
- `course-sections.csv`
- `instructors.csv`
- `students.csv`
- `student-enrollments.csv`
- `instructor-assignments.csv`

## Snapshot Authority Rules

- Uploaded files are authoritative references, not merge inputs.
- If a user uploads a dataset type, the generator must not add more rows of that same type.
- Uploaded datasets should be echoed back unchanged in the output package.
- Downstream datasets must be generated from the most specific uploaded source available.

### Structural Precedence

The structural precedence order is:

- `course-sections.csv` overrides `courses.csv`
- `courses.csv` overrides `hierarchy.csv`
- `hierarchy.csv` overrides fallback hierarchy data

Implications:

- If `course-sections.csv` is uploaded, it defines the section structure for the run. No new `courses.csv` or `hierarchy.csv` should be generated from fallback data.
- If `courses.csv` is uploaded, it becomes the template for section generation.
- If `hierarchy.csv` is uploaded and `courses.csv` is not, generated courses and sections should use that uploaded hierarchy.

### People Precedence

- Uploaded `students.csv` is authoritative for student generation and downstream enrollments.
- Uploaded `instructors.csv` is authoritative for instructor generation and downstream assignments.
- Uploaded `student-enrollments.csv` and `instructor-assignments.csv` are authoritative if present and should not be regenerated.

### Terms

- Uploaded `terms.csv` is authoritative if present.
- If `course-sections.csv` is uploaded, section terms come from that file directly.

## Structure Modes

### Hierarchy Mode

- The run should export `hierarchy.csv` and `courses.csv` when those datasets are not already uploaded.
- If an uploaded hierarchy is present, it is the authoritative hierarchy source.
- If no uploaded hierarchy is present, the app-managed academic unit catalog is the fallback hierarchy source.
- Section hierarchy fields should be derived from the active hierarchy chain, including nonstandard academic unit types.

### Flat Mode

- Flat mode does not generate hierarchy or courses from fallback data unless those files were uploaded and are being echoed back unchanged.
- `Course ID` in generated `course-sections.csv` should be the configured University Abbreviation from the UI.
- `Course` in generated `course-sections.csv` should be the actual connected course abbreviation.
- If an uploaded `hierarchy.csv` is present, hierarchy-related section fields should be derived sequentially from that hierarchy.
- If no uploaded `hierarchy.csv` is present, Flat mode should leave hierarchy-related section fields blank.
- Hierarchy-related section fields must not be hardcoded to assume only `Department`, `Division`, `College`, and `University` source types.

## Hierarchy Field Mapping

- Hierarchy values should come from the active hierarchy chain for the course home unit.
- Nonstandard academic unit types such as `School` or `Program Cluster` must still be represented in section hierarchy fields by sequential mapping through the chain.
- The mapping behavior should prefer preserving the actual uploaded hierarchy over forcing fallback semantic labels.

## File Contracts

### Terms

Input and output columns for `terms.csv`:

- `Title`
- `Start Date`
- `End Date`

### Hierarchy

Input and output columns for `hierarchy.csv`:

- `Title`
- `Abbreviation`
- `Parent Academic Unit`
- `Academic Unit Type`

### Courses

Canonical output columns for `courses.csv`:

- `Title`
- `Abbreviation`
- `Parent Academic Unit`
- `Academic Unit Type`
- `Subject Code`
- `Course Number`

Accepted input shapes for `courses.csv`:

- Canonical required columns:
  - `Title`
  - `Abbreviation`
  - `Parent Academic Unit`
  - `Academic Unit Type`
- Optional canonical columns:
  - `Subject Code`
  - `Course Number`
- Alternate normalized shape:
  - `subject_code`
  - `course_number`
  - `course_id`
  - `title`
  - `default_department_abbreviation`

Extra upload columns may be ignored after normalization.

### Course Sections

Canonical input and output columns for `course-sections.csv`:

- `Title`
- `Section ID`
- `Course ID`
- `Term`
- `Start Date`
- `End Date`
- `Course`
- `Department`
- `Division`
- `College`
- `University`
- `Program`
- `Campus`
- `Session`
- `Course Level`
- `Course Type`
- `Delivery Method`
- `Course Attributes`

Legacy uploads without `Course Attributes` are accepted and normalized.

### Instructors

Input and output columns for `instructors.csv`:

- `First Name`
- `Last Name`
- `School Id`
- `Email`
- `Password`
- `Gender`
- `Tenure Track`
- `Department`

### Students

Input and output columns for `students.csv`:

- `First Name`
- `Last Name`
- `School Id`
- `Email`
- `Password`
- `Grade`
- `Gender`
- `Phone Number`
- `Program`

### Student Enrollments

Input and output columns for `student-enrollments.csv`:

- `Academic Unit`
- `Email`
- `Role`
- `Term`

### Instructor Assignments

Input and output columns for `instructor-assignments.csv`:

- `Academic Unit`
- `Email`
- `Role`
- `Term`

## Generation Expectations

- Uploaded `courses.csv` should drive section generation using only uploaded course IDs.
- Uploaded `course-sections.csv` should drive assignment and enrollment generation using only uploaded section IDs.
- Generated output must not silently fall back to the managed course catalog if an uploaded authoritative course source exists.
- If an uploaded authoritative file is invalid for the needed downstream flow, generation should fail clearly instead of silently falling back.
- When uploaded `courses.csv` is used as the authoritative section template, section generation should preserve the input course order rather than re-sorting to a fallback catalog order.

## UI / UX Expectations

- Snapshot status should be shown in one summary banner, not one alert per dataset.
- The Generate tab should clearly indicate which uploaded datasets were recognized and which downstream files will be generated from them.
- If `courses.csv` is uploaded, the course-count input should be disabled and the UI should show that uploaded courses are driving section generation.
- The Snapshot tab should show recognized datasets and row counts.
- The app should make the two primary use cases clear: full fallback generation and existing-school extension.

## Operational Notes

- Uploaded files in Streamlit are snapshots of browser-uploaded bytes, not live references to files on disk.
- If a local CSV changes after upload, it must be removed and uploaded again in the UI.
- Reusing the same filename is acceptable, but the file must still be re-uploaded to send the new bytes.
- If the app appears to be using stale behavior after code changes, restart the Streamlit process and re-upload the files.
