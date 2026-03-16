import io
from collections import Counter
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import streamlit as st


# -----------------------------------------------------------------------------
# Streamlit app: Validate an academic reporting hierarchy CSV
#
# The uploaded CSV MUST contain these headers (exact names):
#   - Title
#   - Abbreviation
#   - Academic Unit Type
#   - Parent Academic Unit
#
# This app enforces a set of rules to ensure the hierarchy forms a valid,
# balanced tree where academic unit types form a single linear chain of levels
# (e.g., University > College > Department > Course):
#
# 1) Required fields: Every row must have a non-empty Title, Abbreviation,
#    and Academic Unit Type. Abbreviations must be unique across the file.
#
# 2) Parent existence: Every non-top unit must have a Parent Academic Unit
#    that exists in the file (by Abbreviation). The parent row must be of the
#    type immediately above the child's type.
#
# 3) Consistent parent type per child type: All academic units of the same
#    Academic Unit Type must map to parents of the SAME Academic Unit Type.
#    (e.g., if Course maps to Subject Area, then ALL Course rows must map to
#    Subject Area; cannot mix Department and Subject Area.)
#
# 4) Single type per level (linear type chain): There cannot be more than one
#    Academic Unit Type at the same level. Equivalently, the type graph must
#    form a single chain (no branching) from a single top type down to the
#    lowest type. If two different child types share the same parent type,
#    that creates two types at the same level and is invalid.
#
# 5) Top-level: Exactly one row is allowed to have an empty Parent Academic
#    Unit (the top node). Its Academic Unit Type is the top type in the chain.
#    All other rows must have a parent.
#
# The app annotates each row with any errors found and provides a downloadable
# CSV receipt with an Errors column and useful diagnostic columns.
# -----------------------------------------------------------------------------


REQUIRED_COLUMNS = [
    "Title",
    "Abbreviation",
    "Academic Unit Type",
    "Parent Academic Unit",
]


def _normalize_str_series(s: pd.Series) -> pd.Series:
    """Normalize a string series: coerce to string, strip whitespace.

    We do not change case for Titles. For abbreviations and types, callers can
    decide whether to further enforce case if desired.
    """
    return s.fillna("").astype(str).str.strip()


@st.cache_data(show_spinner=False)
def _read_csv(file) -> pd.DataFrame:
    """Read CSV with all columns as strings and preserve empty values.

    - dtype=str preserves leading zeros in codes/IDs.
    - keep_default_na=False prevents treating empty strings as NaN.
    """
    return pd.read_csv(file, dtype=str, keep_default_na=False, na_values=[""])
def validate_hierarchy(df_in: pd.DataFrame, type_order: List[str]) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """Validate the hierarchy and return (annotated_df, summary).

    annotated_df has additional diagnostic columns and an Errors column.
    summary contains counters and type-level diagnostics.
    """
    df = df_in.copy()
    # Ensure a simple 0..N-1 index so row-level error flags by position are stable
    df.reset_index(drop=True, inplace=True)

    # Normalize core columns for safe comparisons (trim spaces).
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            # Early return with a helpful summary; Streamlit UI will show this.
            return df, {
                "ok": False,
                "fatal_error": f"Missing required column: {col}",
            }

    df["Title"] = _normalize_str_series(df["Title"])
    df["Abbreviation"] = _normalize_str_series(df["Abbreviation"])  # primary key-like
    df["Academic Unit Type"] = _normalize_str_series(df["Academic Unit Type"])  # type name
    df["Parent Academic Unit"] = _normalize_str_series(df["Parent Academic Unit"])  # parent abbreviation

    # Prepare error tracking for each row (both human-readable and filterable flags)
    errors_per_row: List[List[str]] = [[] for _ in range(len(df))]
    # Define per-rule boolean error flags (easier to filter in spreadsheets/BI tools)
    ERR_COLS = [
        "err_missing_title",
        "err_missing_abbreviation",
        "err_missing_academic_unit_type",
        "err_duplicate_abbreviation",
        "err_multi_parent_for_abbreviation",
        "err_parent_not_found",
        "err_inconsistent_parent_type_for_child_type",
        "err_multiple_child_types_share_parent_type",
        "err_top_level_count_not_one",
        "err_no_top_level",
        "err_top_row_wrong_type",
        "err_top_type_has_parent",
        "err_parent_type_mismatch",
        "err_ambiguous_parent_type_for_child_type",
        "err_type_not_in_provided_order",
    ]
    err_flags: Dict[str, List[bool]] = {c: [False] * len(df) for c in ERR_COLS}

    def _add_err(idx: int, key: str, msg: str) -> None:
        if 0 <= idx < len(df):
            errors_per_row[idx].append(msg)
            if key in err_flags:
                err_flags[key][idx] = True

    # Basic required values per row
    missing_title = df["Title"] == ""
    missing_abbrev = df["Abbreviation"] == ""
    missing_type = df["Academic Unit Type"] == ""
    for idx in df.index[missing_title]:
        _add_err(idx, "err_missing_title", "Missing Title")
    for idx in df.index[missing_abbrev]:
        _add_err(idx, "err_missing_abbreviation", "Missing Abbreviation")
    for idx in df.index[missing_type]:
        _add_err(idx, "err_missing_academic_unit_type", "Missing Academic Unit Type")

    # Abbreviation uniqueness
    dup_mask = df.duplicated("Abbreviation", keep=False) & (df["Abbreviation"] != "")
    for idx in df.index[dup_mask]:
        _add_err(idx, "err_duplicate_abbreviation", "Duplicate Abbreviation")

    # For potential additional signal: same Abbreviation assigned to different parents
    # Build mapping Abbreviation -> set(Parent Abbreviation)
    abbrev_to_parents: Dict[str, Set[str]] = {}
    for abbr, parent in zip(df["Abbreviation"], df["Parent Academic Unit"]):
        if abbr:
            abbrev_to_parents.setdefault(abbr, set()).add(parent)
    multi_parents = {a: ps for a, ps in abbrev_to_parents.items() if len(ps - {""}) > 1}
    if multi_parents:
        for a, parents in multi_parents.items():
            mask = df["Abbreviation"] == a
            for idx in df.index[mask]:
                _add_err(
                    idx,
                    "err_multi_parent_for_abbreviation",
                    f"Abbreviation maps to multiple parents: {', '.join(sorted(p for p in parents if p))}",
                )

    # Parent existence and record parent's type per row
    abbrev_to_type: Dict[str, str] = {}
    for a, t in zip(df["Abbreviation"], df["Academic Unit Type"]):
        if a:
            abbrev_to_type[a] = t

    parent_exists: List[bool] = []
    parent_type_for_row: List[Optional[str]] = []
    for idx, parent_abbr in enumerate(df["Parent Academic Unit"].tolist()):
        if parent_abbr == "":
            parent_exists.append(False)
            parent_type_for_row.append(None)
        else:
            exists = parent_abbr in abbrev_to_type
            parent_exists.append(exists)
            parent_type_for_row.append(abbrev_to_type.get(parent_abbr))
            if not exists:
                _add_err(idx, "err_parent_not_found", "Parent Academic Unit not found in file")

    df["_Parent Exists"] = parent_exists
    df["_Parent Type (actual)"] = parent_type_for_row

    valid_types = {t for t in type_order}
    invalid_type_mask = (df["Academic Unit Type"] != "") & (~df["Academic Unit Type"].isin(valid_types))
    for idx in df.index[invalid_type_mask]:
        _add_err(
            idx,
            "err_type_not_in_provided_order",
            f"Academic Unit Type '{df.loc[idx, 'Academic Unit Type']}' not in provided order",
        )

    # Determine mapping of child type -> expected parent type based on provided order
    child_to_parent_type_unique: Dict[str, str] = {
        type_order[i]: type_order[i - 1] for i in range(1, len(type_order))
    }
    ordered_types = list(type_order)
    chain_errors: List[str] = []

    # Determine the top type from the provided order
    top_type: Optional[str] = type_order[0] if type_order else None

    # Exactly one row should be top-level (no parent)
    top_level_mask = df["Parent Academic Unit"] == ""
    top_level_count = int(top_level_mask.sum())
    if top_level_count != 1:
        for idx in df.index[top_level_mask]:
            _add_err(
                idx,
                "err_top_level_count_not_one",
                f"Invalid number of top-level rows: expected 1, found {top_level_count}",
            )
        # Also mark all rows if count is zero
        if top_level_count == 0:
            for idx in df.index:
                _add_err(idx, "err_no_top_level", "No top-level row (a single root is required)")

    # If we know the top type, enforce that only top rows have no parent, and
    # that top rows are of that type.
    if top_type is not None:
        # Rows with no parent must be of top_type
        wrong_top_type_mask = top_level_mask & (df["Academic Unit Type"] != top_type)
        for idx in df.index[wrong_top_type_mask]:
            _add_err(
                idx,
                "err_top_row_wrong_type",
                f"Top-level row must be of top type '{top_type}'",
            )

        # Rows of top_type should not have a parent
        top_type_with_parent = (df["Academic Unit Type"] == top_type) & (~top_level_mask)
        for idx in df.index[top_type_with_parent]:
            _add_err(
                idx,
                "err_top_type_has_parent",
                f"Units of top type '{top_type}' must not have a parent",
            )

    # For each row with a parent, check that parent type matches the provided order
    expected_parent_types_for_child_type = {
        ct: pt for ct, pt in child_to_parent_type_unique.items()
    }
    expected_parent_type_col: List[Optional[str]] = []
    for idx, (ct, parent_abbr, p_exists, p_type) in enumerate(
        zip(
            df["Academic Unit Type"],
            df["Parent Academic Unit"],
            df["_Parent Exists"],
            df["_Parent Type (actual)"],
        )
    ):
        exp_pt = expected_parent_types_for_child_type.get(ct)
        expected_parent_type_col.append(exp_pt)
        if parent_abbr != "" and exp_pt is not None:
            if p_exists and p_type != exp_pt:
                _add_err(
                    idx,
                    "err_parent_type_mismatch",
                    f"Parent type mismatch: expected '{exp_pt}', found '{p_type}'",
                )

    df["_Expected Parent Type"] = expected_parent_type_col

    # Aggregate row errors into a single column
    errors_joined: List[str] = ["; ".join(errs) if errs else "" for errs in errors_per_row]
    # Attach per-rule error flags as columns
    for col, values in err_flags.items():
        df[col] = values
    df["Errors"] = errors_joined  # keep for readability/compat
    any_err = pd.DataFrame(err_flags).any(axis=1)
    df["Row Valid"] = ~any_err

    # Build summary
    summary: Dict[str, object] = {
        "ok": bool(df["Row Valid"].all() and not chain_errors),
        "total_rows": int(len(df)),
        "unique_abbreviations": int(df["Abbreviation"].nunique(dropna=True)),
        "duplicate_abbreviations": int(dup_mask.sum()),
        "distinct_types": sorted([t for t in df["Academic Unit Type"].unique() if t != ""]),
        "child_to_parent_type_unique": child_to_parent_type_unique,
        "ordered_types": ordered_types,
        "chain_errors": chain_errors,
        "top_type": top_type,
        "top_level_count": top_level_count,
        "rows_with_errors": int((df["Row Valid"] == False).sum()),
        "provided_type_order": ordered_types,
    }

    return df, summary


# --------------------------- Streamlit UI -------------------------------------

st.set_page_config(page_title="Validate Reporting Hierarchy", page_icon="🌲", layout="wide")
st.title("Validate Reporting Hierarchy")

st.subheader("Step 1: Define academic unit type order (top → bottom)")

TYPE_LEVELS_STATE_KEY = "hierarchy_type_levels"
if TYPE_LEVELS_STATE_KEY not in st.session_state:
    st.session_state[TYPE_LEVELS_STATE_KEY] = [""]

controls = st.columns([1, 1, 4])
if controls[0].button("Add level", key="type_level_add"):
    st.session_state[TYPE_LEVELS_STATE_KEY].append("")
if controls[1].button("Remove last level", key="type_level_remove"):
    if len(st.session_state[TYPE_LEVELS_STATE_KEY]) > 1:
        removed_index = len(st.session_state[TYPE_LEVELS_STATE_KEY]) - 1
        st.session_state[TYPE_LEVELS_STATE_KEY].pop()
        st.session_state.pop(f"type_level_{removed_index}", None)

for idx in range(len(st.session_state[TYPE_LEVELS_STATE_KEY])):
    key = f"type_level_{idx}"
    default_value = st.session_state[TYPE_LEVELS_STATE_KEY][idx]
    placeholder = "Top level (e.g., University)" if idx == 0 else "Next level (e.g., College)"
    new_value = st.text_input(
        f"Level {idx + 1}",
        key=key,
        value=default_value,
        placeholder=placeholder,
    )
    normalized_value = new_value.strip()
    st.session_state[TYPE_LEVELS_STATE_KEY][idx] = normalized_value

type_levels = list(st.session_state[TYPE_LEVELS_STATE_KEY])
empty_levels = [idx + 1 for idx, val in enumerate(type_levels) if not val]
type_order = [val for val in type_levels if val]
duplicate_types = [t for t, count in Counter(type_order).items() if count > 1]

type_order_valid = True
if not type_order:
    st.info("Add at least one academic unit type to begin.")
    type_order_valid = False

if empty_levels:
    st.error(
        "Fill in a name for each hierarchy level or remove unused entries: "
        + ", ".join(f"Level {lvl}" for lvl in empty_levels)
    )
    type_order_valid = False
if duplicate_types:
    st.error("Duplicate type names provided: " + ", ".join(duplicate_types))
    type_order_valid = False

if type_order_valid:
    st.caption("Current type order: " + " → ".join(type_order))
else:
    st.caption("Current type order: (incomplete)")

st.subheader("Step 2: Upload reporting hierarchy CSV")
st.markdown(
    "Upload a CSV describing your school's reporting hierarchy. Required headers:"
)
st.code(
    ", ".join(REQUIRED_COLUMNS),
)

uploaded = st.file_uploader(
    "Upload reporting hierarchy CSV",
    type=["csv"],
    key="hier_csv",
    disabled=not type_order_valid,
)

if not type_order_valid:
    st.info("Resolve the type order issues above to enable file upload.")
    st.stop()

if not uploaded:
    st.info("Upload a CSV to begin validation.")
    st.stop()

with st.spinner("Reading and validating CSV..."):
    df_raw = _read_csv(uploaded)
    annotated_df, summary = validate_hierarchy(df_raw, type_order)

if summary.get("fatal_error"):
    st.error(summary["fatal_error"])
    st.stop()

st.subheader("Results")

met1, met2, met3, met4 = st.columns(4)
met1.metric("Total rows", f"{summary['total_rows']}")
met2.metric("Rows with errors", f"{summary['rows_with_errors']}")
met3.metric("Distinct types", f"{len(summary['distinct_types'])}")
met4.metric("Duplicate abbreviations", f"{summary['duplicate_abbreviations']}")

if summary["chain_errors"]:
    st.error("Type chain issues detected:")
    for msg in summary["chain_errors"]:
        st.write("- ", msg)

st.markdown("Provided type order (top → bottom):")
st.write(" → ".join(summary.get("ordered_types") or []))

if summary.get("top_type"):
    st.caption(
        f"Top type: {summary['top_type']}  | Top-level rows: {summary['top_level_count']}"
    )
else:
    st.caption("Top type not provided.")

with st.expander("Type mapping (child type → expected parent type)"):
    mapping = summary.get("child_to_parent_type_unique", {})
    if mapping:
        st.table(pd.DataFrame(
            [(c, p) for c, p in mapping.items()], columns=["Child Type", "Parent Type"]
        ))
    else:
        st.write("No unique type mapping detected (possible conflicts or missing parents).")

err_cols = [c for c in annotated_df.columns if c.startswith("err_")]

with st.sidebar:
    st.header("View Filters")
    only_errs = st.toggle("Show only rows with any error", value=False)
    selected_errs = st.multiselect(
        "Filter by specific error types",
        options=sorted(err_cols),
        help="Show rows where these error flags are true.",
    )
    match_mode = "Any selected"
    if selected_errs:
        match_mode = st.radio(
            "Match mode",
            options=["Any selected", "All selected"],
            index=0,
            horizontal=True,
        )

# Build filtered view
filtered_df = annotated_df.copy()
mask = pd.Series([True] * len(filtered_df))
if only_errs and err_cols:
    mask = mask & filtered_df[err_cols].any(axis=1)
if selected_errs:
    if match_mode == "All selected":
        mask = mask & filtered_df[selected_errs].all(axis=1)
    else:
        mask = mask & filtered_df[selected_errs].any(axis=1)
filtered_df = filtered_df[mask].reset_index(drop=True)

st.subheader("Annotated Rows (first 200)")
display_cols = [
    "Title",
    "Abbreviation",
    "Academic Unit Type",
    "Parent Academic Unit",
    "_Parent Exists",
    "_Parent Type (actual)",
    "_Expected Parent Type",
    "Row Valid",
    "Errors",
] + sorted(err_cols)
st.caption(f"Showing {len(filtered_df)} of {len(annotated_df)} rows (first 200 below)")
st.dataframe(filtered_df.head(200)[display_cols], use_container_width=True)

# Downloadable receipt with errors
csv_buf_all = io.StringIO()
annotated_df.to_csv(csv_buf_all, index=False)
st.download_button(
    label="Download full validation receipt (CSV)",
    data=csv_buf_all.getvalue(),
    file_name="reporting_hierarchy_validated.csv",
    mime="text/csv",
)

# Download filtered view, if any filters applied
filters_applied = only_errs or bool(selected_errs)
if filters_applied:
    csv_buf_f = io.StringIO()
    filtered_df.to_csv(csv_buf_f, index=False)
    st.download_button(
        label="Download filtered rows (CSV)",
        data=csv_buf_f.getvalue(),
        file_name="reporting_hierarchy_filtered.csv",
        mime="text/csv",
    )

st.caption(
    "Rules enforced: unique abbreviations; parent existence; exactly one top-level row;"
    " type names present in the provided order; and parent types aligned with that order."
)
