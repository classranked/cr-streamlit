from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from sis_generator.catalog import (
    ACADEMIC_UNIT_CATALOG_PATH,
    ATTRIBUTE_CATALOG_PATH,
    COURSE_CATALOG_PATH,
    ensure_catalogs_exist,
    load_academic_unit_catalog,
    load_attribute_catalog,
    load_course_catalog,
    save_catalog,
)
from sis_generator.constants import (
    ACADEMIC_UNIT_CATALOG_COLUMNS,
    ATTRIBUTE_CATALOG_COLUMNS,
    CATALOG_DISPLAY_NAMES,
    COURSE_CATALOG_COLUMNS,
    DISPLAY_NAMES,
    FILE_STEMS,
)
from sis_generator.data_loader import load_metadata
from sis_generator.generator import (
    GenerationConfig,
    TERM_SYSTEM_PATTERNS,
    generate_package,
    plan_generation,
)
from sis_generator.snapshot import SnapshotContext, load_snapshot_files


st.set_page_config(
    page_title="SIS Test Data Generator",
    page_icon="🧪",
    layout="wide",
)

st.title("SIS Test Data Generator")
st.caption(
    "Generate coherent SIS import packages from scratch or from an uploaded snapshot, "
    "with separate course, attribute, and academic unit catalogs."
)


def _load_snapshot_from_state() -> SnapshotContext:
    uploaded = st.session_state.get("snapshot_uploads") or []
    signature = _snapshot_signature(uploaded)
    cached = st.session_state.get("snapshot_context")
    cached_signature = st.session_state.get("snapshot_signature")
    if isinstance(cached, SnapshotContext) and cached_signature == signature:
        return cached
    if not uploaded:
        context = SnapshotContext()
    else:
        context = load_snapshot_files(uploaded)
    st.session_state.snapshot_context = context
    st.session_state.snapshot_signature = signature
    return context


def _snapshot_signature(uploaded_files: list | None) -> tuple[tuple[str, int], ...]:
    if not uploaded_files:
        return ()
    signature = []
    for uploaded_file in uploaded_files:
        try:
            size = len(uploaded_file.getvalue())
        except Exception:
            size = 0
        signature.append((uploaded_file.name, size))
    return tuple(sorted(signature))


def _catalog_editor(
    title: str,
    description: str,
    df: pd.DataFrame,
    state_key: str,
    path: Path,
    columns: list[str],
    sort_columns: list[str],
) -> None:
    editor_key = f"{state_key}_editor"
    st.subheader(title)
    st.write(description)
    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        key=editor_key,
    )
    st.caption(f"Catalog file: {path}")
    save_col, reload_col, download_col = st.columns(3)
    with save_col:
        if st.button(f"Save {title}", use_container_width=True):
            to_save = edited.copy()
            for column in columns:
                if column not in to_save.columns:
                    to_save[column] = ""
            to_save = to_save[columns].fillna("")
            if sort_columns and columns != ACADEMIC_UNIT_CATALOG_COLUMNS:
                to_save = to_save.sort_values(sort_columns)
            save_catalog(to_save, path, columns)
            st.session_state[state_key] = (
                load_academic_unit_catalog(path)
                if columns == ACADEMIC_UNIT_CATALOG_COLUMNS
                else load_course_catalog(path)
                if columns == COURSE_CATALOG_COLUMNS
                else pd.read_csv(path, dtype=str).fillna("")[columns]
            )
            st.success(f"{title} saved.")
    with reload_col:
        if st.button(f"Reload {title}", use_container_width=True):
            st.session_state[state_key] = (
                load_academic_unit_catalog(path)
                if columns == ACADEMIC_UNIT_CATALOG_COLUMNS
                else load_course_catalog(path)
                if columns == COURSE_CATALOG_COLUMNS
                else pd.read_csv(path, dtype=str).fillna("")[columns]
            )
            st.success(f"{title} reloaded.")
    with download_col:
        st.download_button(
            f"Download {title}",
            data=edited.to_csv(index=False),
            file_name=path.name,
            mime="text/csv",
            use_container_width=True,
        )


def _snapshot_status_summary(snapshot: SnapshotContext, structure_mode: str) -> str:
    plan = plan_generation(structure_mode, snapshot)
    labels = {key: f"`{value}`" for key, value in FILE_STEMS.items()}
    lines: list[str] = []
    for key in FILE_STEMS:
        if plan.is_authoritative(key):
            lines.append(f"Using uploaded {labels[key]} as authoritative input")

    source_messages = {
        "generated:catalog": "the fallback course catalog",
        "generated:courses": "generated courses",
        "generated:course_sections": "generated course sections",
        "generated:fallback_hierarchy": "the fallback hierarchy",
        "generated:hierarchy": "the active hierarchy",
        "generated:terms": "newly generated terms",
        "generated:students": "generated students",
        "generated:instructors": "generated instructors",
        "generated:catalog+students": "fallback sections and generated students",
        "generated:catalog+instructors": "fallback sections and generated instructors",
        "generated:courses+students": "course-derived sections and generated students",
        "generated:courses+instructors": "course-derived sections and generated instructors",
        "generated:course_sections+students": "generated sections and generated students",
        "generated:course_sections+instructors": "generated sections and generated instructors",
        "uploaded:terms": "uploaded `terms.csv`",
        "uploaded:courses": "uploaded `courses.csv`",
        "uploaded:course_sections": "uploaded `course-sections.csv`",
        "uploaded:hierarchy": "uploaded `hierarchy.csv`",
    }
    for key in FILE_STEMS:
        if not plan.should_generate(key):
            continue
        source = plan.source_for(key)
        if source:
            lines.append(
                f"Generating {labels[key]} from {source_messages.get(source, source.replace('_', ' '))}"
            )

    if structure_mode == "hierarchy":
        if "course_sections" in snapshot.tables:
            lines.append("Uploaded `course-sections.csv` already defines the structural context for this run")
        elif "hierarchy" in snapshot.tables:
            lines.append("Uploaded `hierarchy.csv` will be reused as the authoritative hierarchy")
        else:
            lines.append("No hierarchy upload detected; the app-managed academic unit catalog will be used as fallback")
    elif "hierarchy" in snapshot.tables:
        lines.append("Flat mode will derive hierarchy-related section fields from uploaded `hierarchy.csv`")
    else:
        lines.append("Flat mode will leave hierarchy-related section fields blank unless `hierarchy.csv` is uploaded")

    if "courses" in snapshot.tables:
        sample_course_ids = ", ".join(snapshot.tables["courses"]["Abbreviation"].astype(str).head(5).tolist())
        course_text = f"Uploaded `courses.csv` detected with {len(snapshot.tables['courses'])} course definitions"
        if sample_course_ids:
            course_text += f" (sample IDs: {sample_course_ids})"
        lines.append(course_text)

    return "Snapshot plan: " + "; ".join(lines) + "."


ensure_catalogs_exist()

if "course_catalog_df" not in st.session_state:
    st.session_state.course_catalog_df = load_course_catalog()
if "attribute_catalog_df" not in st.session_state:
    st.session_state.attribute_catalog_df = load_attribute_catalog()
if "academic_unit_catalog_df" not in st.session_state:
    st.session_state.academic_unit_catalog_df = load_academic_unit_catalog()
if "result" not in st.session_state:
    st.session_state.result = None
if "is_generating_package" not in st.session_state:
    st.session_state.is_generating_package = False
if "generation_notice" not in st.session_state:
    st.session_state.generation_notice = None
if "generation_error" not in st.session_state:
    st.session_state.generation_error = None
if "last_generation_config" not in st.session_state:
    st.session_state.last_generation_config = None

metadata = load_metadata()
tab_generate, tab_snapshot, tab_units, tab_courses, tab_attributes, tab_review = st.tabs(
    [
        "Generate",
        "Snapshot Context",
        "Academic Unit Catalog",
        "Course Catalog",
        "Attribute Catalog",
        "Review & Export",
    ]
)

st.info(
    f"Seed assets loaded: {metadata['catalog_rows']} legacy catalog templates, "
    f"{metadata['first_name_rows']} first names, {metadata['last_name_rows']} last names."
)


with tab_generate:
    st.subheader("Run Configuration")
    snapshot_file_list = ", ".join(FILE_STEMS.values())
    current_year = datetime.now().year
    st.markdown("#### Snapshot Context")
    st.write(
        "Upload any combination of existing SIS CSVs here. Uploaded datasets stay attached to this run configuration and are treated as authoritative references."
    )
    st.file_uploader(
        "Upload existing SIS files",
        type=["csv"],
        accept_multiple_files=True,
        key="snapshot_uploads",
    )
    snapshot = _load_snapshot_from_state()
    generation_plan = plan_generation("hierarchy", snapshot)

    if st.session_state.generation_notice:
        st.success(st.session_state.generation_notice)
        st.session_state.generation_notice = None
    if st.session_state.generation_error:
        st.error(st.session_state.generation_error)
        st.session_state.generation_error = None
    if snapshot.errors:
        for error in snapshot.errors:
            st.error(error)

    st.markdown("#### General")
    general_col1, general_col2, general_col3 = st.columns(3)
    with general_col1:
        mode = st.radio(
            "Generation mode",
            options=["full_package", "delta"],
            format_func=lambda value: "Full Package" if value == "full_package" else "Delta from Snapshot",
            help=(
                "Full Package generates a complete SIS dataset from the app-managed catalogs. "
                "Delta from Snapshot uses uploaded SIS files as context and generates the next set of data around that baseline."
            ),
        )
        structure_mode = st.radio(
            "Structure mode",
            options=["hierarchy", "flat"],
            format_func=lambda value: "Hierarchy" if value == "hierarchy" else "Flat",
            help=(
                "Hierarchy mode exports a `hierarchy.csv` and keeps academic unit assignments aligned to that hierarchy. "
                "Flat mode omits `hierarchy.csv` and `courses.csv`, and collapses section reporting to the highest academic level."
            ),
        )
        generation_plan = plan_generation(structure_mode, snapshot)
        flat_variance_rate = st.slider(
            "Flat-mode variance rate",
            min_value=0.0,
            max_value=0.5,
            value=0.15,
            step=0.01,
            disabled=structure_mode != "flat",
            help=(
                "In Flat mode, this controls how often a generated section intentionally varies its reporting hierarchy. "
                "Variance can remove Division, remove both Department and Division, or swap the section to a different department chain."
            ),
        )
    with general_col2:
        seed = st.number_input(
            "Seed",
            min_value=1,
            value=20260313,
            step=1,
            help="Controls the random generation. Reuse the same seed to reproduce the same dataset with the same settings.",
        )
        email_domain = st.text_input("Email domain", value="classranked.edu")
    with general_col3:
        institution_name = st.text_input("Institution name", value="ClassRanked University")
        institution_abbreviation = st.text_input(
            "University abbreviation",
            value="CRU",
            help="Used for the fallback hierarchy root academic unit abbreviation.",
        )

    if mode == "delta":
        st.info(
            "Delta from Snapshot requires your existing SIS files in the "
            f"**Snapshot Context** tab before generation: {snapshot_file_list}."
        )
    st.info(_snapshot_status_summary(snapshot, structure_mode))

    st.markdown("#### Terms")
    term_col1, term_col2, term_col3, term_col4 = st.columns(4)
    with term_col1:
        term_count = st.number_input(
            "Terms to generate",
            min_value=0,
            value=3,
            step=1,
            disabled=not generation_plan.should_generate("terms"),
        )
    if generation_plan.is_authoritative("terms"):
        term_system = "semester"
        start_term_label = "Spring"
        start_term_year = current_year
        with term_col2:
            st.text_input("Academic calendar", value="Uploaded terms.csv", disabled=True)
        with term_col3:
            st.text_input("Starting term", value="From uploaded terms", disabled=True)
        with term_col4:
            st.text_input("Starting year", value="From uploaded terms", disabled=True)
    elif not generation_plan.should_generate("terms"):
        term_system = "semester"
        start_term_label = "Spring"
        start_term_year = current_year
        with term_col2:
            st.text_input("Academic calendar", value="Derived from uploaded course sections", disabled=True)
        with term_col3:
            st.text_input("Starting term", value="Not generating terms", disabled=True)
        with term_col4:
            st.text_input("Starting year", value="Not generating terms", disabled=True)
    elif mode == "full_package":
        with term_col2:
            term_system = st.selectbox(
                "Academic calendar",
                options=["semester", "quarter"],
                format_func=lambda value: value.capitalize(),
            )
        with term_col3:
            start_term_label = st.selectbox(
                "Starting term",
                options=[label for label, _, _ in TERM_SYSTEM_PATTERNS[term_system]],
            )
        with term_col4:
            start_term_year = st.number_input(
                "Starting year",
                min_value=2000,
                value=current_year,
                step=1,
            )
    else:
        term_system = "semester"
        start_term_label = "Spring"
        start_term_year = current_year
        with term_col2:
            st.text_input("Academic calendar", value="Inherited from snapshot", disabled=True)
        with term_col3:
            st.text_input("Starting term", value="Latest snapshot term", disabled=True)
        with term_col4:
            st.text_input("Starting year", value="Derived automatically", disabled=True)

    st.markdown("#### Population")
    population_col1, population_col2, population_col3 = st.columns(3)
    with population_col1:
        if "course_sections" in snapshot.tables:
            course_source_count = len(snapshot.tables["course_sections"])
            course_source_label = "uploaded course-sections.csv"
            st.text_input(
                "Course source",
                value=f"{course_source_count} section rows from {course_source_label}",
                disabled=True,
                help="Uploaded course sections are authoritative for this run, so course generation is skipped.",
            )
            courses_count = 0
        elif "courses" in snapshot.tables:
            course_source_count = (
                len(snapshot.tables["courses"])
            )
            course_source_label = "uploaded courses.csv"
            st.text_input(
                "Course source",
                value=f"{course_source_count} courses from {course_source_label}",
                disabled=True,
                help="Uploaded courses are authoritative for this run and will drive section generation directly.",
            )
            courses_count = course_source_count
        else:
            catalog_course_count = len(st.session_state.course_catalog_df)
            default_course_count = min(120, catalog_course_count)
            courses_count = st.number_input(
                "Courses to generate",
                min_value=0,
                max_value=int(catalog_course_count),
                value=int(default_course_count),
                step=10,
                disabled=not generation_plan.should_generate("course_sections"),
                help="Controls how many courses are selected from the managed course catalog for each generated term. Total sections across the package scale with the number of generated terms.",
            )
            course_source_count = int(courses_count)
    with population_col2:
        instructor_count = st.number_input(
            "Instructors to generate",
            min_value=0,
            value=80,
            step=5,
            disabled=not generation_plan.should_generate("instructors"),
        )
    with population_col3:
        student_count = st.number_input(
            "Students to generate",
            min_value=0,
            value=500,
            step=25,
            disabled=not generation_plan.should_generate("students"),
        )

    st.markdown("#### Section Load")
    range_col1, range_col2, range_col3 = st.columns(3)
    with range_col1:
        sections_per_course_range = st.slider(
            "Sections per course per term",
            min_value=1,
            max_value=10,
            value=(1, 3),
            step=1,
            disabled=not generation_plan.should_generate("course_sections"),
        )
        if generation_plan.should_generate("course_sections") and "course_sections" not in snapshot.tables:
            min_sections_per_term = int(course_source_count) * int(sections_per_course_range[0])
            max_sections_per_term = int(course_source_count) * int(sections_per_course_range[1])
            if generation_plan.should_generate("terms"):
                st.caption(
                    f"Estimated generated sections: {min_sections_per_term}-{max_sections_per_term} per term, "
                    f"{min_sections_per_term * int(term_count)}-{max_sections_per_term * int(term_count)} total across {int(term_count)} term(s)."
                )
            else:
                st.caption(
                    f"Estimated generated sections: {min_sections_per_term}-{max_sections_per_term} per term."
                )
    with range_col2:
        enrollments_per_section_range = st.slider(
            "Enrollments per section",
            min_value=1,
            max_value=100,
            value=(1, 26),
            step=1,
        )
    with range_col3:
        instructors_per_section_range = st.slider(
            "Instructors assigned per section",
            min_value=1,
            max_value=20,
            value=(1, 2),
            step=1,
        )

    st.markdown("#### Data Quality")
    quality_col1, quality_col2 = st.columns(2)
    with quality_col1:
        edge_case_rate = st.slider("Edge-case name rate", min_value=0.0, max_value=0.25, value=0.07, step=0.01)
    with quality_col2:
        include_section_attributes = st.toggle(
            "Include section attributes",
            value=True,
            help="When off, generated course sections keep hierarchy fields but leave Program, Campus, Session, Course Level, Course Type, Delivery Method, and Course Attributes blank.",
        )
        duplicate_mode = st.toggle("Inject intentional duplicates", value=False)
        duplicate_count = st.number_input(
            "Duplicate rows",
            min_value=0,
            value=5 if duplicate_mode else 0,
            step=1,
            disabled=not duplicate_mode,
        )

    generate_requested = st.button(
        "Generating SIS Package..." if st.session_state.is_generating_package else "Generate SIS Package",
        type="primary",
        use_container_width=True,
        disabled=st.session_state.is_generating_package,
    )
    if generate_requested:
        st.session_state.is_generating_package = True
        st.rerun()

    if st.session_state.is_generating_package:
        if snapshot.errors:
            st.session_state.generation_error = "Resolve snapshot upload errors before generating. The app will not fall back to the managed course catalog while uploads are invalid."
            st.session_state.is_generating_package = False
            st.rerun()
        snapshot_context = (
            snapshot
        )
        config = GenerationConfig(
            mode=mode,
            structure_mode=structure_mode,
            seed=int(seed),
            term_count=int(term_count),
            term_system=term_system,
            start_term_label=start_term_label,
            start_term_year=int(start_term_year),
            courses_count=course_source_count,
            sections_per_course_min=int(sections_per_course_range[0]),
            sections_per_course_max=int(sections_per_course_range[1]),
            student_count=int(student_count),
            instructor_count=int(instructor_count),
            enrollments_per_section_min=int(enrollments_per_section_range[0]),
            enrollments_per_section_max=int(enrollments_per_section_range[1]),
            instructors_per_section_min=int(instructors_per_section_range[0]),
            instructors_per_section_max=int(instructors_per_section_range[1]),
            duplicate_mode=bool(duplicate_mode),
            duplicate_count=int(duplicate_count),
            edge_case_rate=float(edge_case_rate),
            email_domain=email_domain.strip(),
            institution_name=institution_name.strip(),
            institution_abbreviation=institution_abbreviation.strip().upper(),
            flat_variance_rate=float(flat_variance_rate),
            include_section_attributes=bool(include_section_attributes),
        )
        try:
            with st.spinner("Generating SIS Package..."):
                st.session_state.result = generate_package(
                    config=config,
                    course_catalog=st.session_state.course_catalog_df,
                    attribute_catalog=st.session_state.attribute_catalog_df,
                    academic_unit_catalog=st.session_state.academic_unit_catalog_df,
                    snapshot=snapshot_context,
                )
                st.session_state.last_generation_config = {
                    "mode": mode,
                    "structure_mode": structure_mode,
                    "courses_count": int(course_source_count),
                    "term_count": int(term_count),
                    "sections_per_course_min": int(sections_per_course_range[0]),
                    "sections_per_course_max": int(sections_per_course_range[1]),
                }
            st.session_state.generation_notice = "Generated package is ready for review and download."
        except ValueError as exc:
            st.session_state.generation_error = str(exc)
        finally:
            st.session_state.is_generating_package = False
        st.rerun()


with tab_snapshot:
    st.subheader("Snapshot Context")
    st.write(
        "Snapshot uploads now live on the Generate tab so the upload state and run configuration stay together. This tab is a read-only view of the currently loaded snapshot context."
    )
    snapshot = _load_snapshot_from_state()
    if snapshot.errors:
        for error in snapshot.errors:
            st.error(error)
    if snapshot.tables:
        st.dataframe(snapshot.summary(), use_container_width=True, hide_index=True)
    else:
        st.info("No snapshot files loaded.")


with tab_units:
    _catalog_editor(
        CATALOG_DISPLAY_NAMES["academic_unit_catalog"],
        "Fallback hierarchy used in hierarchy mode when no uploaded `hierarchy.csv` is provided.",
        st.session_state.academic_unit_catalog_df.copy(),
        "academic_unit_catalog_df",
        ACADEMIC_UNIT_CATALOG_PATH,
        ACADEMIC_UNIT_CATALOG_COLUMNS,
        ["Academic Unit Type", "Title", "Abbreviation"],
    )


with tab_courses:
    _catalog_editor(
        CATALOG_DISPLAY_NAMES["course_catalog"],
        "Canonical course catalog. Required columns are Title, Abbreviation, Parent Academic Unit, and Academic Unit Type. Subject Code and Course Number are optional supplemental columns.",
        st.session_state.course_catalog_df.copy(),
        "course_catalog_df",
        COURSE_CATALOG_PATH,
        COURSE_CATALOG_COLUMNS,
        ["Subject Code", "Course Number", "Abbreviation"],
    )


with tab_attributes:
    _catalog_editor(
        CATALOG_DISPLAY_NAMES["attribute_catalog"],
        "Section-level attribute values used for primary assignments and overflow `Course Attributes` generation.",
        st.session_state.attribute_catalog_df.copy(),
        "attribute_catalog_df",
        ATTRIBUTE_CATALOG_PATH,
        ATTRIBUTE_CATALOG_COLUMNS,
        ["attribute_name", "value"],
    )


with tab_review:
    st.subheader("Review & Export")
    result = st.session_state.result
    last_generation_config = st.session_state.last_generation_config
    if result is None:
        st.info("Generate a package to review files, collisions, and downloads.")
    else:
        if last_generation_config:
            st.caption(
                "Last generated package used "
                f"{last_generation_config['courses_count']} course(s), "
                f"{last_generation_config['term_count']} term(s), and "
                f"{last_generation_config['sections_per_course_min']}-{last_generation_config['sections_per_course_max']} section(s) per course per term "
                f"in {last_generation_config['structure_mode']} mode."
            )
        sum_col, collision_col, dup_col = st.columns(3)
        with sum_col:
            st.metric("Datasets", len(result.files))
        with collision_col:
            st.metric("Collision regenerations", len(result.collision_report))
        with dup_col:
            st.metric("Injected duplicates", len(result.duplicate_report))

        st.dataframe(result.summary, use_container_width=True, hide_index=True)
        st.info("Download the entire generated package as a single ZIP below. Individual CSV downloads are optional.")

        download_col1, download_col2 = st.columns(2)
        with download_col1:
            st.download_button(
                "Download Entire Package (.zip)",
                data=result.zip_bytes,
                file_name="sis-test-data-package.zip",
                mime="application/zip",
                use_container_width=True,
            )
        with download_col2:
            st.download_button(
                "Download Fallback Hierarchy Catalog",
                data=st.session_state.academic_unit_catalog_df.to_csv(index=False),
                file_name=ACADEMIC_UNIT_CATALOG_PATH.name,
                mime="text/csv",
                use_container_width=True,
            )

        if not result.collision_report.empty:
            st.markdown("**Collision Report**")
            st.dataframe(result.collision_report, use_container_width=True, hide_index=True)

        if not result.duplicate_report.empty:
            st.markdown("**Duplicate Injection Report**")
            st.dataframe(result.duplicate_report, use_container_width=True, hide_index=True)

        st.markdown("**Generated Files**")
        preview_name = st.selectbox(
            "Preview a dataset",
            options=list(result.files.keys()),
            format_func=lambda value: DISPLAY_NAMES[value],
        )
        preview_df = result.files[preview_name]
        st.dataframe(preview_df.head(200), use_container_width=True, hide_index=True)
        st.download_button(
            f"Download {FILE_STEMS[preview_name]}",
            data=preview_df.to_csv(index=False),
            file_name=FILE_STEMS[preview_name],
            mime="text/csv",
        )
