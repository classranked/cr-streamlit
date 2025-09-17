import io
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


def _compute_type_chain(child_to_parent_type: Dict[str, str]) -> Tuple[List[str], List[str]]:
    """Compute a linear type chain from mapping child_type -> parent_type.

    Returns a tuple: (ordered_types, chain_errors)
    - ordered_types: top type first down to leaf type if chain can be built;
      otherwise best-effort ordering of discovered types.
    - chain_errors: list of error messages if the mapping is not a simple chain
      (e.g., multiple roots, repeated parents at a level, or cycles).
    """
    errors: List[str] = []

    # All types seen in the mapping (as child or parent)
    child_types = set(child_to_parent_type.keys())
    parent_types = set(child_to_parent_type.values())
    all_types = child_types | parent_types

    # A proper chain should have exactly one root (a type that is never a child)
    roots = sorted(parent_types - child_types)
    if len(roots) != 1:
        if len(roots) == 0:
            errors.append("Type chain error: no unique top type (cycle or all types are children).")
        else:
            errors.append(
                f"Type chain error: multiple candidate top types: {', '.join(roots)}."
            )

    # In a linear chain, each parent_type should appear as a parent of at most one child_type
    parent_counts: Dict[str, int] = {}
    for pt in child_to_parent_type.values():
        parent_counts[pt] = parent_counts.get(pt, 0) + 1
    same_level = [pt for pt, c in parent_counts.items() if c > 1]
    if same_level:
        errors.append(
            "Type chain error: multiple child types share the same parent type (more than one type at a level): "
            + ", ".join(f"{pt} -> {parent_counts[pt]} children" for pt in same_level)
        )

    # Try to build an ordered chain (best-effort even if errors exist)
    ordered: List[str] = []
    if roots:
        # Start at (first) root and walk down following the unique child (if any)
        # Build reverse index: parent_type -> child_type (should be unique for a valid chain)
        parent_to_child: Dict[str, str] = {}
        for child, parent in child_to_parent_type.items():
            # the same parent may appear more than once; we only keep the first for ordering purposes
            parent_to_child.setdefault(parent, child)

        cur = roots[0]
        seen: Set[str] = set()
        while cur and cur not in seen:
            ordered.append(cur)
            seen.add(cur)
            cur = parent_to_child.get(cur, None)

        # If we didn't cover all types, append any leftover types (best-effort)
        leftovers = [t for t in all_types if t not in ordered]
        ordered.extend(leftovers)
    else:
        ordered = list(all_types)

    return ordered, errors


def validate_hierarchy(df_in: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, object]]:
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

    # Per child type, collect counts of actual parent types seen (where parent exists)
    type_to_parent_counts: Dict[str, Dict[str, int]] = {}
    for child_type, p_exists, p_type in zip(
        df["Academic Unit Type"], df["_Parent Exists"], df["_Parent Type (actual)"]
    ):
        if p_exists and p_type:
            d = type_to_parent_counts.setdefault(child_type, {})
            d[p_type] = d.get(p_type, 0) + 1

    # Decide the expected parent type per child type:
    # - If a single parent type is observed, that's the expected type.
    # - If multiple are observed, use the unique mode (strict majority) if present.
    # - If there is a tie for the mode, mark the child type as ambiguous.
    child_to_parent_type_unique: Dict[str, str] = {}
    ambiguous_child_types: Set[str] = set()
    for child_type, counts in type_to_parent_counts.items():
        if not counts:
            continue
        # Determine mode(s)
        max_count = max(counts.values())
        modes = [pt for pt, c in counts.items() if c == max_count]
        if len(modes) == 1:
            child_to_parent_type_unique[child_type] = modes[0]
            # We will flag only the deviating rows later via parent_type_mismatch
        else:
            # Tie → can't infer a single expected type
            ambiguous_child_types.add(child_type)

    # Detect type-chain errors (multiple root types, same-level conflicts, cycles)
    ordered_types, chain_errors = _compute_type_chain(child_to_parent_type_unique)

    # Apply same-level error annotation to involved rows when relevant
    # If multiple child types share the same parent type, flag all rows of those child types
    parent_to_children: Dict[str, List[str]] = {}
    for c, p in child_to_parent_type_unique.items():
        parent_to_children.setdefault(p, []).append(c)
    for p, children in parent_to_children.items():
        if len(children) > 1:
            for ct in children:
                for idx in df.index[df["Academic Unit Type"] == ct]:
                    _add_err(
                        idx,
                        "err_multiple_child_types_share_parent_type",
                        f"Multiple child types share parent type '{p}' (violates single type per level)",
                    )

    # Determine the top type and enforce top-level parent rules
    # Top type is any parent type that is not a child in the unique mapping.
    candidates_top_types = sorted(set(child_to_parent_type_unique.values()) - set(child_to_parent_type_unique.keys()))
    top_type: Optional[str] = candidates_top_types[0] if len(candidates_top_types) == 1 else None

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

    # For each row with a parent, check that parent type equals the unique allowed parent type for the row's child type
    expected_parent_types_for_child_type = {
        ct: pt for ct, pt in child_to_parent_type_unique.items()
    }
    expected_parent_type_col: List[Optional[str]] = []
    for idx, (ct, p_exists, p_type) in enumerate(
        zip(df["Academic Unit Type"], df["_Parent Exists"], df["_Parent Type (actual)"])
    ):
        exp_pt = expected_parent_types_for_child_type.get(ct)
        expected_parent_type_col.append(exp_pt)
        if df.loc[idx, "Parent Academic Unit"] != "":
            # Has a parent; enforce expected type when we know it
            if exp_pt is not None and p_exists and p_type != exp_pt:
                _add_err(
                    idx,
                    "err_parent_type_mismatch",
                    f"Parent type mismatch: expected '{exp_pt}', found '{p_type}'",
                )
            # Only flag ambiguity when the child type truly has a tie among parent types
            if exp_pt is None and p_exists and ct in ambiguous_child_types:
                modes = []
                if ct in type_to_parent_counts:
                    # collect tied modes for the message
                    counts = type_to_parent_counts[ct]
                    max_count = max(counts.values())
                    modes = sorted([pt for pt, c in counts.items() if c == max_count])
                _add_err(
                    idx,
                    "err_ambiguous_parent_type_for_child_type",
                    (
                        "Ambiguous parent type for this child type (tie among: "
                        + ", ".join(modes)
                        + ")"
                    ),
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
    }

    return df, summary


# --------------------------- Streamlit UI -------------------------------------

st.set_page_config(page_title="Validate Reporting Hierarchy", page_icon="🌲", layout="wide")
st.title("Validate Reporting Hierarchy (Academic Units)")

st.markdown(
    "Upload a CSV describing your school's reporting hierarchy. Required headers:"
)
st.code(
    ", ".join(REQUIRED_COLUMNS),
)

uploaded = st.file_uploader("Upload reporting hierarchy CSV", type=["csv"], key="hier_csv")

if not uploaded:
    st.info("Upload a CSV to begin validation.")
    st.stop()

with st.spinner("Reading and validating CSV..."):
    df_raw = _read_csv(uploaded)
    annotated_df, summary = validate_hierarchy(df_raw)

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

st.markdown("Suggested type order (top → bottom):")
st.write(" → ".join(summary.get("ordered_types") or []))

if summary.get("top_type"):
    st.caption(f"Detected top type: {summary['top_type']}  | Top-level rows: {summary['top_level_count']}")
else:
    st.caption("Top type not uniquely determined from data.")

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
    "Rules enforced: unique abbreviations; consistent parent type per child type;"
    " exactly one top-level row; parent existence; and a single linear type chain."
)
