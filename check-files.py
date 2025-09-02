import io
import os
import re
import zipfile
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# -----------------------------
# Helpers: file detection/mapping
# -----------------------------

EXPECTED_CATEGORIES = [
    "instructors",
    "students",
    "terms",
    "courses",
    "course sections",
    "student enrollments",
    "instructor assignments",
]

# Display order for grouped error output
DISPLAY_ORDER = [
    "courses",
    "terms",
    "course sections",
    "instructors",
    "instructor assignments",
    "students",
    # The spec mentioned "student assignments"; we assume it means enrollments
    "student enrollments",
]

# Strict filename stem → category mapping
STEM_TO_CATEGORY: Dict[str, str] = {
    "faculty-and-staff": "instructors",
    "students": "students",
    "terms": "terms",
    "courses": "courses",
    "course-sections": "course sections",
    "student-enrollments": "student enrollments",
    "instructor-assignments": "instructor assignments",
}


def guess_category(filename: str) -> Optional[str]:
    # Handle both '/' and '\\' in archive paths
    base = re.split(r"[\\\\/]", str(filename))[-1]
    stem, _ = os.path.splitext(base)
    stem = stem.strip().lower()
    return STEM_TO_CATEGORY.get(stem)


def is_zip(file_name: str, head_bytes: bytes) -> bool:
    return file_name.lower().endswith(".zip") or head_bytes.startswith(
        b"PK\x03\x04")


def read_tabular_bytes(name: str, data: bytes) -> Optional[pd.DataFrame]:
    ext = os.path.splitext(name)[1].lower()
    try:
        if ext in {".csv", ".txt"}:
            # Try comma first; fallback to tab if it looks TSV-like
            sample = data[:4096].decode(errors="ignore")
            sep = "," if sample.count(",") >= sample.count("\t") else "\t"
            return pd.read_csv(io.BytesIO(data),
                               sep=sep,
                               dtype=str,
                               keep_default_na=False)
        if ext in {".tsv"}:
            return pd.read_csv(io.BytesIO(data),
                               sep="\t",
                               dtype=str,
                               keep_default_na=False)
        if ext in {".xlsx", ".xls"}:
            return pd.read_excel(io.BytesIO(data), dtype=str)
    except Exception as e:
        st.warning(f"Failed to parse '{name}': {e}")
    return None


def ensure_details_column(
        df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
    cols = {c: c for c in df.columns}
    # Normalize columns to lowercase trimmed
    normalized = {
        c: re.sub(r"\s+", " ", str(c)).strip().lower()
        for c in df.columns
    }
    df = df.rename(columns={
        old: new
        for old, new in zip(df.columns, normalized.values())
    })
    details_col = None
    for c in df.columns:
        if c.lower() == "details":
            details_col = c
            break
    return df, details_col


def filter_error_rows(df: pd.DataFrame, details_col: str) -> pd.DataFrame:
    # Exact phrases (including punctuation, case). Accept both spellings.
    ok_vals = {"Successfully Created!", "Successfully Updated!"}
    col = df[details_col].fillna("").astype(str).str.strip()
    mask = ~col.isin(ok_vals)
    return df[mask]


@st.cache_data(show_spinner=False)
def parse_uploads(files: List[st.runtime.uploaded_file_manager.UploadedFile]):
    extracted: List[Tuple[str, bytes]] = []
    for f in files:
        head = f.getvalue()[:8]
        if is_zip(f.name, head):
            try:
                with zipfile.ZipFile(io.BytesIO(f.getvalue())) as zf:
                    for zi in zf.infolist():
                        if zi.is_dir():
                            continue
                        name = zi.filename
                        # Skip hidden/system files
                        if os.path.basename(name).startswith("."):
                            continue
                        data = zf.read(zi)
                        extracted.append((name, data))
            except zipfile.BadZipFile:
                st.error(f"'{f.name}' looks like a ZIP but could not be read.")
        else:
            extracted.append((f.name, f.getvalue()))

    frames: List[Tuple[str, Optional[str],
                       pd.DataFrame]] = []  # (filename, category, df)
    for name, data in extracted:
        df = read_tabular_bytes(name, data)
        if df is None:
            continue
        cat = guess_category(name)
        frames.append((name, cat, df))
    return frames


def main():
    st.set_page_config(page_title="ClassRanked SIS Receipt Checker",
                       page_icon="🧾",
                       layout="wide")
    st.title("SIS Receipt Checker")
    st.caption(
        "Upload your ZIP or individual receipt files. We'll auto-match by name and surface rows where the 'details' column does not say 'Succesfully Created!' or 'Successfully Updated!'."
    )

    with st.sidebar:
        st.header("Upload")
        files = st.file_uploader(
            "Upload ZIP or CSV/TSV/XLSX receipts",
            type=["zip", "csv", "tsv", "txt", "xlsx", "xls"],
            accept_multiple_files=True,
        )
        st.markdown("Expected categories:")
        for cat in EXPECTED_CATEGORIES:
            st.markdown(f"- {cat}")

    if not files:
        st.info("Drop a ZIP or one/more files to begin.")
        return

    parsed = parse_uploads(files)
    if not parsed:
        st.error("No readable tabular files found in your upload.")
        return

    st.subheader("File Mapping")
    mapping_cols = st.columns([3, 2, 2, 1], vertical_alignment="center")
    mapping_cols[0].write("File")
    mapping_cols[1].write("Auto Category")
    mapping_cols[2].write("Override")
    mapping_cols[3].write("Rows")

    overrides: Dict[int, Optional[str]] = {}
    for idx, (name, cat, df) in enumerate(parsed):
        with st.container():
            c1, c2, c3, c4 = st.columns([3, 2, 2, 1], vertical_alignment="center")
            c1.write(name)
            c2.write(cat or "—")
            override = c3.selectbox(
                "",
                options=["(auto)"] + EXPECTED_CATEGORIES + ["(ignore)"],
                index=0,
                key=f"override_{idx}",
                label_visibility="collapsed",
            )
            c4.write(len(df))
            if override == "(ignore)":
                overrides[idx] = None
            elif override == "(auto)":
                overrides[idx] = cat
            else:
                overrides[idx] = override

    # Build results and summaries
    st.subheader("Error Rows")
    errors_by_category: Dict[str, List[Tuple[str, pd.DataFrame]]] = {}
    summary_rows: List[Tuple[str, str, int,
                             int]] = []  # (file, category, total, errors)

    for idx, (name, auto_cat, df) in enumerate(parsed):
        category = overrides.get(idx, auto_cat)
        if category is None:
            continue  # ignored

        df_norm, details_col = ensure_details_column(df)
        if not details_col:
            st.warning(f"'{name}' has no 'details' column. Skipping.")
            continue

        errors_df = filter_error_rows(df_norm, details_col)
        total_rows = len(df_norm)
        error_count = len(errors_df)
        summary_rows.append((name, category
                             or "(unknown)", total_rows, error_count))

        if error_count > 0 and category:
            errors_by_category.setdefault(category, []).append((name, errors_df))

    # Summary table
    if summary_rows:
        st.markdown("### Summary")
        summary_df = pd.DataFrame(
            summary_rows,
            columns=["source_file", "category", "total_rows",
                     "error_rows"]).sort_values(["category", "source_file"
                                                 ]).reset_index(drop=True)
        st.dataframe(summary_df, use_container_width=True, height=240)

    # Errors display per category and per file, in requested order
    if errors_by_category:
        shown_any = False
        # Categories in requested order, then any remaining
        ordered_cats = [c for c in DISPLAY_ORDER if c in errors_by_category]
        remaining = [c for c in errors_by_category.keys()
                     if c not in DISPLAY_ORDER]
        for cat in ordered_cats + remaining:
            st.markdown(f"### {cat.title()}")
            for fname, edf in errors_by_category[cat]:
                st.markdown(f"- File: `{fname}` — {len(edf)} error rows")
                st.dataframe(edf, use_container_width=True)
                shown_any = True
        if not shown_any:
            st.success("No errors found in the uploaded receipts.")
    else:
        st.success("No errors found in the uploaded receipts.")


if __name__ == "__main__":
    main()
