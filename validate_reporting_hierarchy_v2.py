
import io
from typing import List, Dict, Tuple

import pandas as pd
import streamlit as st

REQUIRED_COLS = [
    "Title",
    "Abbreviation",
    "Academic Unit Type",
    "Parent Academic Unit",
]

# -----------------------------
# Helpers
# -----------------------------

def _clean(s: str) -> str:
    if pd.isna(s):
        return ""
    return str(s).strip()


def load_data(file) -> pd.DataFrame:
    try:
        df = pd.read_csv(file, dtype=str).fillna("")
    except Exception as e:
        st.error(f"Failed to read CSV: {e}")
        raise

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        st.error(
            "Missing required columns: " + ", ".join(missing) +
            " — expected exactly: Title, Abbreviation, Academic Unit Type, Parent Academic Unit"
        )
        st.stop()

    # Normalize whitespace only; keep case as provided for reporting
    for c in REQUIRED_COLS:
        df[c] = df[c].map(_clean)

    return df


def parse_ladder(raw: str) -> List[str]:
    # Accept comma- or newline-separated; preserve order; drop empties; de-dup preserving order
    items = []
    for token in (raw or "").replace("\n", ",").split(","):
        t = token.strip()
        if t and t not in items:
            items.append(t)
    return items


def ladder_map(ladder: List[str]) -> Dict[str, int]:
    return {t: i for i, t in enumerate(ladder)}  # 0 is top-most


def build_indices(df: pd.DataFrame) -> Tuple[Dict[str, list], Dict[str, dict]]:
    """Return (abbr_index, title_index).
    abbr_index: abbr -> list of row dicts {title, type, parent}
    title_index: title -> row dict {title, abbr, type, parent}
    """
    abbr_idx: Dict[str, list] = {}
    title_idx: Dict[str, dict] = {}

    for _, r in df.iterrows():
        title = r["Title"]
        abbr = r["Abbreviation"]
        au_type = r["Academic Unit Type"]
        parent = r["Parent Academic Unit"]

        abbr_idx.setdefault(abbr, []).append({
            "Title": title,
            "Academic Unit Type": au_type,
            "Parent Academic Unit": parent,
        })
        # If duplicate titles exist, last write wins; parent/type checks rely on presence, not uniqueness by title
        title_idx[title] = {
            "Title": title,
            "Abbreviation": abbr,
            "Academic Unit Type": au_type,
            "Parent Academic Unit": parent,
        }
    return abbr_idx, title_idx


# -----------------------------
# Validation Passes
# -----------------------------

def validate_abbreviation_uniqueness(df: pd.DataFrame) -> pd.DataFrame:
    """Categorize into Collision A/B/C.
    - A: Same Abbreviation across different Types
    - B: Same Abbreviation + same Type appears multiple times (likely dup/typo)
    - C: Same Abbreviation + same Type maps to >1 distinct Parent titles
    Note: A row can theoretically hit more than one; we prefer C, then B, then A to keep root-cause focused.
    """
    records = []
    abbr_idx, _ = build_indices(df)

    for abbr, rows in abbr_idx.items():
        types = {r["Academic Unit Type"] for r in rows}
        # Group rows by (abbr, type)
        type_groups: Dict[str, list] = {}
        for r in rows:
            t = r["Academic Unit Type"]
            type_groups.setdefault(t, []).append(r)

        # Priority: C (multi-parent) then B (dup same type) then A (cross-type collision)
        for t, group in type_groups.items():
            parents = sorted({_clean(g["Parent Academic Unit"]) for g in group if _clean(g["Parent Academic Unit"])})
            if len(parents) > 1:
                for g in group:
                    records.append({
                        "Issue Category": "Collision C: Multi-parent for same Abbreviation+Type",
                        "Title": g["Title"],
                        "Abbreviation": abbr,
                        "Academic Unit Type": t,
                        "Parent Academic Unit": g["Parent Academic Unit"],
                        "Parent Resolved Title": g["Parent Academic Unit"],
                        "Parent Resolved Type": "",
                        "Details": f"'{abbr}' ({t}) maps to multiple parents: {parents}",
                    })
        for t, group in type_groups.items():
            if len(group) > 1:
                # If it was already flagged as C (multi-parent), skip double-reporting here
                parents = {_clean(g["Parent Academic Unit"]) for g in group if _clean(g["Parent Academic Unit"]) }
                if len(parents) <= 1:
                    titles = sorted({g["Title"] for g in group})
                    for g in group:
                        records.append({
                            "Issue Category": "Collision B: Duplicate Abbreviation within same Type",
                            "Title": g["Title"],
                            "Abbreviation": abbr,
                            "Academic Unit Type": t,
                            "Parent Academic Unit": g["Parent Academic Unit"],
                            "Parent Resolved Title": g["Parent Academic Unit"],
                            "Parent Resolved Type": "",
                            "Details": f"'{abbr}' ({t}) appears multiple times (Titles: {titles}). Likely duplicate/typo.",
                        })
        if len(types) > 1:
            # Cross-type collision
            type_list = sorted(types)
            for r in rows:
                records.append({
                    "Issue Category": "Collision A: Same Abbreviation across different Types",
                    "Title": r["Title"],
                    "Abbreviation": abbr,
                    "Academic Unit Type": r["Academic Unit Type"],
                    "Parent Academic Unit": r["Parent Academic Unit"],
                    "Parent Resolved Title": r["Parent Academic Unit"],
                    "Parent Resolved Type": "",
                    "Details": f"Abbreviation '{abbr}' is used for multiple types: {type_list}. Make abbreviations unique.",
                })

    return pd.DataFrame.from_records(records, columns=[
        "Issue Category",
        "Title",
        "Abbreviation",
        "Academic Unit Type",
        "Parent Academic Unit",
        "Parent Resolved Title",
        "Parent Resolved Type",
        "Details",
    ])


def validate_presence_and_types(df: pd.DataFrame, ladder: List[str]) -> pd.DataFrame:
    """Checks: Has Type (and valid), Has Parent (except top). Parent existence/type is handled in validate_parent_type.
    """
    lm = ladder_map(ladder)
    top_type = ladder[0] if ladder else ""
    issues = []

    for _, r in df.iterrows():
        title = r["Title"]
        abbr = r["Abbreviation"]
        t = r["Academic Unit Type"]
        parent = r["Parent Academic Unit"]

        if not t:
            issues.append({
                "Issue Category": "Missing Type",
                "Title": title,
                "Abbreviation": abbr,
                "Academic Unit Type": t,
                "Parent Academic Unit": parent,
                "Parent Resolved Title": "",
                "Parent Resolved Type": "",
                "Details": "Academic Unit Type is blank.",
            })
            continue
        if t not in lm:
            issues.append({
                "Issue Category": "Invalid Type",
                "Title": title,
                "Abbreviation": abbr,
                "Academic Unit Type": t,
                "Parent Academic Unit": parent,
                "Parent Resolved Title": "",
                "Parent Resolved Type": "",
                "Details": f"Type '{t}' not found in provided ladder {ladder}.",
            })
            continue

        if t != top_type and not parent:
            issues.append({
                "Issue Category": "Missing Parent",
                "Title": title,
                "Abbreviation": abbr,
                "Academic Unit Type": t,
                "Parent Academic Unit": parent,
                "Parent Resolved Title": "",
                "Parent Resolved Type": "",
                "Details": f"Units of type '{t}' must have a parent (top type is '{top_type}').",
            })

    return pd.DataFrame(issues, columns=[
        "Issue Category",
        "Title",
        "Abbreviation",
        "Academic Unit Type",
        "Parent Academic Unit",
        "Parent Resolved Title",
        "Parent Resolved Type",
        "Details",
    ])


def validate_parent_type(df: pd.DataFrame, ladder: List[str]) -> pd.DataFrame:
    """Ensures parent exists and is exactly one level higher than child.
    """
    lm = ladder_map(ladder)
    abbr_idx, title_idx = build_indices(df)

    issues = []
    for _, r in df.iterrows():
        title = r["Title"]
        abbr = r["Abbreviation"]
        t = r["Academic Unit Type"]
        parent_title = r["Parent Academic Unit"]

        if not t or t not in lm:
            # Already handled in presence/invalid type pass
            continue
        if not parent_title:
            # Missing parent handled earlier (unless it's top type)
            continue

        parent_row = title_idx.get(parent_title)
        if not parent_row:
            issues.append({
                "Issue Category": "Parent Not Found",
                "Title": title,
                "Abbreviation": abbr,
                "Academic Unit Type": t,
                "Parent Academic Unit": parent_title,
                "Parent Resolved Title": "",
                "Parent Resolved Type": "",
                "Details": f"Parent title '{parent_title}' not found in file.",
            })
            continue

        parent_type = parent_row["Academic Unit Type"]
        if parent_type not in lm:
            issues.append({
                "Issue Category": "Parent Has Invalid Type",
                "Title": title,
                "Abbreviation": abbr,
                "Academic Unit Type": t,
                "Parent Academic Unit": parent_title,
                "Parent Resolved Title": parent_title,
                "Parent Resolved Type": parent_type,
                "Details": f"Parent type '{parent_type}' not in ladder {ladder}.",
            })
            continue

        expected_parent_level = lm[t] - 1
        if expected_parent_level < 0:
            # This would mean child is top-most but has a parent; flag as incorrect
            issues.append({
                "Issue Category": "Top Type Should Not Have Parent",
                "Title": title,
                "Abbreviation": abbr,
                "Academic Unit Type": t,
                "Parent Academic Unit": parent_title,
                "Parent Resolved Title": parent_title,
                "Parent Resolved Type": parent_type,
                "Details": f"Type '{t}' is top of ladder and must not have a parent.",
            })
            continue

        if lm[parent_type] != expected_parent_level:
            exp_type = ladder[expected_parent_level]
            issues.append({
                "Issue Category": "Parent Type Mismatch",
                "Title": title,
                "Abbreviation": abbr,
                "Academic Unit Type": t,
                "Parent Academic Unit": parent_title,
                "Parent Resolved Title": parent_title,
                "Parent Resolved Type": parent_type,
                "Details": f"Expected parent type '{exp_type}' for child type '{t}', but found '{parent_type}'.",
            })

    return pd.DataFrame(issues, columns=[
        "Issue Category",
        "Title",
        "Abbreviation",
        "Academic Unit Type",
        "Parent Academic Unit",
        "Parent Resolved Title",
        "Parent Resolved Type",
        "Details",
    ])


def validate_multi_parent(df: pd.DataFrame) -> pd.DataFrame:
    groups = (
        df.groupby(["Abbreviation", "Academic Unit Type"], dropna=False)["Parent Academic Unit"]
        .nunique(dropna=False)
        .reset_index()
    )
    offenders = groups[groups["Parent Academic Unit"] > 1]

    idx = df.set_index(["Abbreviation", "Academic Unit Type"])  # for quick filter
    records = []
    for _, row in offenders.iterrows():
        abbr = row["Abbreviation"]
        t = row["Academic Unit Type"]
        subset = df[(df["Abbreviation"] == abbr) & (df["Academic Unit Type"] == t)]
        parents = sorted({p for p in subset["Parent Academic Unit"].map(_clean) if p})
        for _, g in subset.iterrows():
            records.append({
                "Issue Category": "Multi-parent",
                "Title": g["Title"],
                "Abbreviation": abbr,
                "Academic Unit Type": t,
                "Parent Academic Unit": g["Parent Academic Unit"],
                "Parent Resolved Title": g["Parent Academic Unit"],
                "Parent Resolved Type": "",
                "Details": f"'{abbr}' ({t}) maps to multiple parents: {parents}",
            })
    return pd.DataFrame.from_records(records, columns=[
        "Issue Category",
        "Title",
        "Abbreviation",
        "Academic Unit Type",
        "Parent Academic Unit",
        "Parent Resolved Title",
        "Parent Resolved Type",
        "Details",
    ])


def build_summary(*issue_dfs: pd.DataFrame) -> dict:
    total = 0
    by_category = {}
    for df in issue_dfs:
        if df is None or df.empty:
            continue
        total += len(df)
        for cat, n in df["Issue Category"].value_counts().items():
            by_category[cat] = by_category.get(cat, 0) + n
    return {"total_issues": total, "by_category": by_category}


# -----------------------------
# Streamlit App
# -----------------------------

st.set_page_config(page_title="Reporting Hierarchy Validator", layout="wide")
st.title("Reporting Hierarchy Validator")

st.markdown(
    "Provide your Academic Unit Types in order, then upload a CSV with columns: "
    "**Title, Abbreviation, Academic Unit Type, Parent Academic Unit**."
)

with st.expander("Step 1 — Define your Academic Unit Types (top → bottom)", expanded=True):
    st.write("Enter a comma- or newline-separated list. Example: `Institution, College, Division, Department, Subject Area, Course`.")
    raw_ladder = st.text_area("Academic Unit Types (ordered)", height=100)
    ladder = parse_ladder(raw_ladder)
    col_a, col_b = st.columns([1,1])
    with col_a:
        st.markdown("**Detected order:** " + (" → ".join(ladder) if ladder else "(none)"))
    with col_b:
        valid_ladder = st.checkbox("Confirm type order", value=False, help="Check to confirm this order is correct.")

if not ladder or not valid_ladder:
    st.info("Define and confirm your Academic Unit Types to enable file upload.")
    st.stop()

with st.expander("Step 2 — Upload CSV and Validate", expanded=True):
    uploaded = st.file_uploader("Upload CSV", type=["csv"], accept_multiple_files=False)

    if uploaded is None:
        # Offer a sample CSV built dynamically from the provided ladder
        sample_rows = []
        # Build a tiny, generic, ladder-based sample
        top = ladder[0]
        sample_rows.append({
            "Title": f"Example {top}",
            "Abbreviation": f"{top[:3].upper()}",
            "Academic Unit Type": top,
            "Parent Academic Unit": "",
        })
        if len(ladder) >= 2:
            t1 = ladder[1]
            sample_rows.append({
                "Title": f"Example {t1}",
                "Abbreviation": f"{t1[:3].upper()}",
                "Academic Unit Type": t1,
                "Parent Academic Unit": f"Example {top}",
            })
        if len(ladder) >= 3:
            t2 = ladder[2]
            sample_rows.append({
                "Title": f"Example {t2}",
                "Abbreviation": f"{t2[:3].upper()}",
                "Academic Unit Type": t2,
                "Parent Academic Unit": f"Example {ladder[1]}",
            })
        sample_df = pd.DataFrame(sample_rows, columns=REQUIRED_COLS)
        csv_buf = io.StringIO()
        sample_df.to_csv(csv_buf, index=False)
        st.download_button("Download sample CSV (based on your ladder)", data=csv_buf.getvalue(), file_name="sample_reporting_hierarchy.csv")
        st.stop()

    df = load_data(uploaded)

    # ---- Run validations ----
    presence_df = validate_presence_and_types(df, ladder)
    abbr_df = validate_abbreviation_uniqueness(df)
    parent_type_df = validate_parent_type(df, ladder)
    multi_parent_df = validate_multi_parent(df)

    # Consolidate issues (avoid empty concat warnings)
    issue_parts = [d for d in [presence_df, abbr_df, parent_type_df, multi_parent_df] if d is not None and not d.empty]
    issues_consolidated = pd.concat(issue_parts, ignore_index=True) if issue_parts else pd.DataFrame(columns=[
        "Issue Category","Title","Abbreviation","Academic Unit Type","Parent Academic Unit","Parent Resolved Title","Parent Resolved Type","Details"
    ])

    # ---- Summary ----
    summary = build_summary(presence_df, abbr_df, parent_type_df, multi_parent_df)
    st.subheader("Summary")
    cols = st.columns(2)
    with cols[0]:
        st.metric("Rows processed", len(df))
        st.metric("Unique abbreviations", df["Abbreviation"].nunique())
    with cols[1]:
        st.metric("Total issues", summary["total_issues"]) 
        if summary["by_category"]:
            st.write({k: int(v) for k, v in sorted(summary["by_category"].items(), key=lambda kv: kv[0])})

    # ---- Issue sections ----
    def _issues_section(title: str, issues_df: pd.DataFrame, key: str):
        with st.expander(title, expanded=not issues_df.empty):
            if issues_df.empty:
                st.success("No issues in this category.")
            else:
                st.dataframe(issues_df, use_container_width=True)
                csv_buf = io.StringIO()
                issues_df.to_csv(csv_buf, index=False)
                st.download_button(
                    f"Download {key}.csv",
                    data=csv_buf.getvalue(),
                    file_name=f"{key}.csv",
                )

    _issues_section("Presence & Type Validations", presence_df, "issues_presence_and_type")
    _issues_section("Abbreviation Collisions (A/B/C)", abbr_df, "issues_abbreviation_collisions")
    _issues_section("Parent Type Checks", parent_type_df, "issues_parent_type")
    _issues_section("Multi-parent Violations", multi_parent_df, "issues_multi_parent")

    # Consolidated download
    st.subheader("Consolidated Issues")
    if issues_consolidated.empty:
        st.success("No violations detected. Hierarchy conforms to rules.")
    else:
        st.dataframe(issues_consolidated, use_container_width=True)
        out = io.StringIO()
        issues_consolidated.to_csv(out, index=False)
        st.download_button(
            "Download issues_consolidated.csv",
            data=out.getvalue(),
            file_name="issues_consolidated.csv",
        )
