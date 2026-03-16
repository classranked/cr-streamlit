import io
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Resolve Missing Enrollments", page_icon="📧", layout="wide")
st.title("Resolve Missing Enrollments via School ID → Faculty Email")

st.markdown(
    "Upload three CSVs: the 'missing enrollments' export, 'students.csv', and 'faculty-and-staff.csv'.\n"
    "The app maps users by shared School ID and outputs the email stored in your system (faculty email).\n"
    "If the 'missing' file doesn't contain School ID, we can infer it from student email using students.csv."
)


def _detect_id_columns(columns: List[str]) -> List[str]:
    candidates = []
    lowered = [c.lower() for c in columns]
    for c, lc in zip(columns, lowered):
        if "school id" in lc or lc in {"school_id", "schoolid", "sis id", "sis_id", "id"}:
            candidates.append(c)
        elif ("school" in lc and "id" in lc) or ("sis" in lc and "id" in lc):
            candidates.append(c)
    # Return unique while preserving order
    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out or columns


def _detect_email_columns(columns: List[str]) -> List[str]:
    candidates = []
    for c in columns:
        lc = c.lower()
        if "email" in lc:
            candidates.append(c)
        elif lc in {"e-mail", "primary email", "primary_email"}:
            candidates.append(c)
    # Fallback to all columns if none detected
    return candidates or columns


@st.cache_data(show_spinner=False)
def _read_csv(file) -> pd.DataFrame:
    # Read CSV as strings to preserve leading zeros in IDs
    return pd.read_csv(file, dtype=str, keep_default_na=False, na_values=[""])  # empty strings preserved


def _normalize_email_series(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip().str.lower()


def _normalize_id_series(s: pd.Series) -> pd.Series:
    # Keep as string, preserve leading zeros, trim spaces
    return s.fillna("").astype(str).str.strip()


def _default_index(cols: List[str], candidates: List[str]) -> int:
    if not cols:
        return 0
    for c in candidates:
        if c in cols:
            return list(cols).index(c)
    return 0


with st.sidebar:
    st.header("Inputs")
    missing_file = st.file_uploader("Missing enrollments CSV", type=["csv"], key="missing")
    students_file = st.file_uploader("students.csv", type=["csv"], key="students")
    faculty_file = st.file_uploader("faculty-and-staff.csv", type=["csv"], key="faculty")

    fallback_to_student = st.toggle(
        "Fallback to student email if no faculty email",
        value=True,
        help="If a faculty email isn't found for a School ID, use the student email when available."
    )


if not (missing_file and students_file and faculty_file):
    st.info("Upload all three CSVs in the sidebar to continue.")
    st.stop()


with st.spinner("Reading CSVs..."):
    df_missing = _read_csv(missing_file)
    df_students = _read_csv(students_file)
    df_faculty = _read_csv(faculty_file)


st.subheader("Map Columns")
col1, col2, col3 = st.columns(3)

with col1:
    st.caption("students.csv")
    stud_id_col = st.selectbox(
        "Students School ID column",
        options=list(df_students.columns),
        index=_default_index(list(df_students.columns), _detect_id_columns(list(df_students.columns))),
        format_func=lambda x: x,
        key="stud_id_col",
    )
    stud_email_col = st.selectbox(
        "Students Email column",
        options=list(df_students.columns),
        index=_default_index(list(df_students.columns), _detect_email_columns(list(df_students.columns))),
        key="stud_email_col",
    )

with col2:
    st.caption("faculty-and-staff.csv")
    fac_id_col = st.selectbox(
        "Faculty School ID column",
        options=list(df_faculty.columns),
        index=_default_index(list(df_faculty.columns), _detect_id_columns(list(df_faculty.columns))),
        key="fac_id_col",
    )
    fac_email_col = st.selectbox(
        "Faculty Email column",
        options=list(df_faculty.columns),
        index=_default_index(list(df_faculty.columns), _detect_email_columns(list(df_faculty.columns))),
        key="fac_email_col",
    )

with col3:
    st.caption("Missing enrollments CSV")
    missing_id_candidate_cols = _detect_id_columns(list(df_missing.columns))
    missing_email_candidate_cols = _detect_email_columns(list(df_missing.columns))

    missing_id_col: Optional[str] = None
    missing_email_col: Optional[str] = None

    if df_missing.shape[1] > 0:
        missing_id_col = st.selectbox(
            "Missing file School ID column (preferred)",
            options=["<none>"] + list(df_missing.columns),
            index=(missing_id_candidate_cols and (1 + df_missing.columns.get_loc(missing_id_candidate_cols[0]))) or 0,
            help="If available, select the School ID column. We'll use this first.",
            key="missing_id_col",
        )
        if missing_id_col == "<none>":
            missing_id_col = None

        missing_email_col = st.selectbox(
            "Missing file Student Email column (optional)",
            options=["<none>"] + list(df_missing.columns),
            index=(missing_email_candidate_cols and (1 + df_missing.columns.get_loc(missing_email_candidate_cols[0]))) or 0,
            help="If no School ID is present, we can infer School ID by matching this email against students.csv.",
            key="missing_email_col",
        )
        if missing_email_col == "<none>":
            missing_email_col = None


if missing_id_col is None and missing_email_col is None:
    st.error("Select at least a School ID or Student Email column in the 'Missing enrollments' file.")
    st.stop()


def resolve_emails(
    df_missing: pd.DataFrame,
    df_students: pd.DataFrame,
    df_faculty: pd.DataFrame,
    missing_id_col: Optional[str],
    missing_email_col: Optional[str],
    stud_id_col: str,
    stud_email_col: str,
    fac_id_col: str,
    fac_email_col: str,
    fallback_to_student: bool,
) -> Tuple[pd.DataFrame, dict]:
    # Prepare normalized columns
    students = df_students.copy()
    faculty = df_faculty.copy()
    missing = df_missing.copy()

    students["_stud_id"] = _normalize_id_series(students[stud_id_col])
    students["_stud_email"] = _normalize_email_series(students[stud_email_col])

    faculty["_fac_id"] = _normalize_id_series(faculty[fac_id_col])
    faculty["_fac_email"] = _normalize_email_series(faculty[fac_email_col])

    if missing_id_col:
        missing["_missing_id"] = _normalize_id_series(missing[missing_id_col])
    else:
        missing["_missing_id"] = ""
    if missing_email_col:
        missing["_missing_email"] = _normalize_email_series(missing[missing_email_col])
    else:
        missing["_missing_email"] = ""

    # Build lookup maps
    # Map student email -> School ID (drop duplicates keeping first)
    stud_email_to_id = students[["_stud_email", "_stud_id"]].dropna().drop_duplicates("_stud_email")
    # Map School ID -> faculty email (keep first occurrence)
    fac_id_to_email = faculty[["_fac_id", "_fac_email"]].dropna().drop_duplicates("_fac_id")
    # Also map School ID -> student email for fallback
    stud_id_to_email = students[["_stud_id", "_stud_email"]].dropna().drop_duplicates("_stud_id")

    # Merge path A: direct School ID in missing
    merged = missing.merge(
        fac_id_to_email, how="left", left_on="_missing_id", right_on="_fac_id", suffixes=("", "")
    )

    # Track resolution stats
    stats = {
        "total_rows": len(missing),
        "have_missing_id": int((missing["_missing_id"] != "").sum()),
        "have_missing_email": int((missing["_missing_email"] != "").sum()),
        "matched_by_id": 0,
        "inferred_id_from_student_email": 0,
        "resolved_via_faculty": 0,
        "fallback_to_student": 0,
        "unresolved": 0,
        "duplicate_ids_students": int(students.duplicated("_stud_id").sum()),
        "duplicate_ids_faculty": int(faculty.duplicated("_fac_id").sum()),
        "duplicate_student_emails": int(students.duplicated("_stud_email").sum()),
    }

    # Mark those matched directly by ID to faculty
    direct_match_mask = merged["_fac_email"].notna() & (merged["_fac_email"] != "")
    stats["matched_by_id"] = int(direct_match_mask.sum())

    # For rows not matched by direct ID, try inferring ID from student email
    need_infer_mask = ~direct_match_mask
    if missing_email_col is not None and (need_infer_mask.any()):
        to_infer = merged.loc[need_infer_mask].merge(
            stud_email_to_id, how="left", left_on="_missing_email", right_on="_stud_email"
        )
        stats["inferred_id_from_student_email"] = int(to_infer["_stud_id"].notna().sum())

        # Attach inferred IDs back
        merged.loc[to_infer.index, "_inferred_id"] = to_infer["_stud_id"].fillna("")

        # Try faculty match again using inferred ID
        to_infer2 = to_infer.merge(
            fac_id_to_email, how="left", left_on="_stud_id", right_on="_fac_id", suffixes=("", "")
        )
        merged.loc[to_infer2.index, "_fac_email"] = (
            merged.loc[to_infer2.index, "_fac_email"].where(
                merged.loc[to_infer2.index, "_fac_email"].notna() & (merged.loc[to_infer2.index, "_fac_email"] != ""),
                to_infer2["_fac_email"],
            )
        )

        # For diagnostics, also set _missing_id if it was blank and we inferred one
        merged.loc[(merged["_missing_id"] == "") & to_infer2["_stud_id"].notna(), "_missing_id"] = (
            to_infer2["_stud_id"].fillna("")
        )

    # Decide resolved email
    merged["resolved_email"] = merged["_fac_email"].fillna("")
    merged["resolved_source"] = "unresolved"

    fac_good = merged["resolved_email"].astype(str).str.strip() != ""
    merged.loc[fac_good, "resolved_source"] = "faculty"
    stats["resolved_via_faculty"] = int(fac_good.sum())

    if fallback_to_student:
        # Fill with student email using either known missing_id or inferred id
        merged = merged.merge(
            stud_id_to_email.rename(columns={"_stud_id": "_sid_for_fallback", "_stud_email": "_stud_email_fb"}),
            how="left",
            left_on="_missing_id",
            right_on="_sid_for_fallback",
        )
        no_fac_mask = merged["resolved_email"].astype(str).str.strip() == ""
        merged.loc[no_fac_mask, "resolved_email"] = merged.loc[no_fac_mask, "_stud_email_fb"].fillna("")
        merged.loc[no_fac_mask & (merged["resolved_email"].astype(str).str.strip() != ""), "resolved_source"] = (
            "student_fallback"
        )
        stats["fallback_to_student"] = int((no_fac_mask & (merged["resolved_email"].astype(str).str.strip() != "")).sum())

    stats["unresolved"] = int((merged["resolved_email"].astype(str).str.strip() == "").sum())

    # Cleanup helper columns for output clarity
    output_cols = list(df_missing.columns) + [
        "resolved_email",
        "resolved_source",
    ]
    # Ensure presence and uniqueness
    output_cols = [c for c in output_cols if c is not None]
    out = merged.copy()
    # If the missing file didn't have an ID column, include the inferred one
    if missing_id_col is None:
        out.rename(columns={"_missing_id": "Matched School ID"}, inplace=True)
        output_cols.append("Matched School ID")

    return out[output_cols], stats


resolved_df, stats = resolve_emails(
    df_missing=df_missing,
    df_students=df_students,
    df_faculty=df_faculty,
    missing_id_col=missing_id_col,
    missing_email_col=missing_email_col,
    stud_id_col=stud_id_col,
    stud_email_col=stud_email_col,
    fac_id_col=fac_id_col,
    fac_email_col=fac_email_col,
    fallback_to_student=fallback_to_student,
)


st.subheader("Results")
met1, met2, met3, met4, met5, met6 = st.columns(6)
met1.metric("Total rows", f"{stats['total_rows']}")
met2.metric("Matched by ID", f"{stats['matched_by_id']}")
met3.metric("Inferred ID via student email", f"{stats['inferred_id_from_student_email']}")
met4.metric("Resolved via faculty email", f"{stats['resolved_via_faculty']}")
met5.metric("Student fallback used", f"{stats['fallback_to_student']}")
met6.metric("Unresolved", f"{stats['unresolved']}")

with st.expander("Diagnostics"):
    st.write(
        {
            "have_missing_id": stats["have_missing_id"],
            "have_missing_email": stats["have_missing_email"],
            "duplicate_ids_students": stats["duplicate_ids_students"],
            "duplicate_ids_faculty": stats["duplicate_ids_faculty"],
            "duplicate_student_emails": stats["duplicate_student_emails"],
        }
    )

st.dataframe(resolved_df.head(100), use_container_width=True)

csv_buf = io.StringIO()
resolved_df.to_csv(csv_buf, index=False)
st.download_button(
    label="Download resolved CSV",
    data=csv_buf.getvalue(),
    file_name="resolved_missing_enrollments.csv",
    mime="text/csv",
)

st.caption(
    "Notes: Faculty email is assumed authoritative for dual-enrolled users. \n"
    "If both School ID and Student Email are available, School ID is used first. \n"
    "All matching is case-insensitive for emails and preserves leading zeros for IDs."
)
