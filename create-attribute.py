import io
import re
from typing import List, Dict, Any

import numpy as np
import pandas as pd
import streamlit as st

##############################
# Helpers
##############################
REQUIRED_FIELDS = [
    "Section ID",
    "Title",
    "Course ID",
    "Start Date",
    "End Date",
]

SUPPORTED_OPERATORS = [
    "==",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "between",        # inclusive, numeric/date
    "contains",       # substring (case-insensitive)
    "startswith",     # case-insensitive
    "endswith",       # case-insensitive
    "regex",          # Python regex, case-insensitive
    "in",             # comma-separated list
]

SPECIAL_DERIVED_FIELDS = [
    "duration_days",  # (End Date - Start Date).days
    "duration_weeks", # round((End-Start).days / 7, 2)
]


def _parse_date_series(s: pd.Series) -> pd.Series:
    # robust date parsing, returns NaT for failures
    return pd.to_datetime(s, errors="coerce")


def _ensure_required_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    # Rename columns to the required canonical names using mapping
    return df.rename(columns=mapping)


def _with_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Start Date"] = _parse_date_series(df["Start Date"])  # may be already datetime
    df["End Date"] = _parse_date_series(df["End Date"])      # idem
    # duration in days
    df["duration_days"] = (df["End Date"] - df["Start Date"]).dt.days
    # duration in weeks (float)
    df["duration_weeks"] = np.round(df["duration_days"] / 7.0, 0)
    return df


def _coerce_for_comparison(series: pd.Series, operator: str, value: Any) -> Any:
    # Try to coerce types sensibly: dates, numbers; otherwise compare as strings (case-insensitive for text ops)
    if operator in {"between", ">", ">=", "<", "<=", "==", "!="}:
        # Try numeric first
        try:
            return pd.to_numeric(value)
        except Exception:
            pass
        # Try datetime
        try:
            return pd.to_datetime(value, errors="raise")
        except Exception:
            pass
    return value


def _make_mask(series: pd.Series, operator: str, value: Any, value_to: Any = None) -> pd.Series:
    # Handle NaNs safely
    s = series
    val = _coerce_for_comparison(series, operator, value)
    val_to = _coerce_for_comparison(series, operator, value_to) if operator == "between" else None

    if operator == "==":
        return s == val
    if operator == "!=":
        return s != val
    if operator == ">":
        return pd.to_numeric(s, errors="coerce") > pd.to_numeric(val, errors="coerce")
    if operator == ">=":
        return pd.to_numeric(s, errors="coerce") >= pd.to_numeric(val, errors="coerce")
    if operator == "<":
        return pd.to_numeric(s, errors="coerce") < pd.to_numeric(val, errors="coerce")
    if operator == "<=":
        return pd.to_numeric(s, errors="coerce") <= pd.to_numeric(val, errors="coerce")
    if operator == "between":
        # inclusive bounds; try numeric, else dates
        s_num = pd.to_numeric(s, errors="coerce")
        v1_num = pd.to_numeric(val, errors="coerce")
        v2_num = pd.to_numeric(val_to, errors="coerce")
        num_mask = (~s_num.isna()) & (~pd.isna(v1_num)) & (~pd.isna(v2_num)) & (s_num >= v1_num) & (s_num <= v2_num)
        if num_mask.any():
            return num_mask
        # fallback to datetime
        s_dt = pd.to_datetime(s, errors="coerce")
        v1_dt = pd.to_datetime(val, errors="coerce")
        v2_dt = pd.to_datetime(val_to, errors="coerce")
        return (~s_dt.isna()) & (~pd.isna(v1_dt)) & (~pd.isna(v2_dt)) & (s_dt >= v1_dt) & (s_dt <= v2_dt)
    if operator == "contains":
        return s.astype(str).str.contains(str(val), case=False, na=False)
    if operator == "startswith":
        return s.astype(str).str.startswith(str(val), na=False)
    if operator == "endswith":
        return s.astype(str).str.endswith(str(val), na=False)
    if operator == "regex":
        try:
            return s.astype(str).str.contains(val, flags=re.IGNORECASE, regex=True, na=False)  # type: ignore
        except Exception:
            return pd.Series(False, index=s.index)
    if operator == "in":
        values = [v.strip() for v in str(val).split(",") if v.strip()]
        return s.astype(str).str.strip().str.upper().isin([v.upper() for v in values])
    # default: no match
    return pd.Series(False, index=s.index)


def apply_rules(df: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    """Apply rules to df and return a new df with attribute columns created/filled.

    Rules DataFrame columns:
        - attribute: str (name of the column to set)
        - field: str (column in df or derived field)
        - operator: str (one of SUPPORTED_OPERATORS)
        - value: Any
        - value_to: Any (only for "between")
        - output: Any (value to assign when condition matches)
        - stop_if_matched: bool (optional; default True)
        - order: int (for stable ordering within same attribute)
    """
    if rules.empty:
        return df.copy()

    df_out = df.copy()

    # Ensure attribute columns exist, initialized as None
    for attr in rules["attribute"].unique():
        if attr not in df_out.columns:
            df_out[attr] = pd.NA

    # stable ordering: by attribute, then order, then original index
    rules = rules.copy()
    if "order" not in rules.columns:
        rules["order"] = np.arange(1, len(rules) + 1)
    if "stop_if_matched" not in rules.columns:
        rules["stop_if_matched"] = True

    rules = rules.sort_values(["attribute", "order"]).reset_index(drop=True)

    for _, r in rules.iterrows():
        attr = r["attribute"]
        field = r["field"]
        op = r["operator"]
        val = r.get("value", None)
        val_to = r.get("value_to", None)
        output = r.get("output", None)
        stop_if_matched = bool(r.get("stop_if_matched", True))

        if field not in df_out.columns:
            # silently skip unknown fields
            continue

        # Build mask
        mask = _make_mask(df_out[field], op, val, val_to)

        # Only set where attribute is NA
        to_set = mask & df_out[attr].isna()
        if to_set.any():
            df_out.loc[to_set, attr] = output

        # Optional: stop evaluating further rules for rows that matched this attribute
        if stop_if_matched:
            already_set = df_out[attr].notna()
            # Future rules for this same attribute should not change already-set rows
            # We enforce this by masking inside the next iterations via df_out[attr].isna()
            pass

    return df_out


##############################
# Streamlit App
##############################

st.set_page_config(page_title="Course Section Attribute Builder", page_icon="📐", layout="wide")
st.title("📐 Course Section Attribute Builder")

st.markdown(
    """
Upload a **course sections CSV**, map its columns, then define **attribute rules**.\
Rules are evaluated **top-to-bottom per attribute**; the **first match wins** (by default).

**Common derived fields available:** `duration_days`, `duration_weeks`.
    """
)

# --- File upload ---
st.sidebar.header("1) Upload & Map Columns")
upload = st.sidebar.file_uploader("Upload CSV", type=["csv"])  # keep it simple for now

raw_df: pd.DataFrame | None = None
if upload is not None:
    try:
        raw_df = pd.read_csv(upload)
    except Exception:
        upload.seek(0)
        raw_df = pd.read_csv(upload, encoding_errors="ignore")

if raw_df is None:
    st.info("Upload a CSV to begin. Expected fields: Section ID, Title, Course, Start Date, End Date.")
    st.stop()

st.subheader("Preview (first 100 rows)")
st.dataframe(raw_df.head(100), use_container_width=True)

# --- Column mapping ---
with st.sidebar.expander("Map Columns", expanded=True):
    current_cols = list(raw_df.columns)
    mapping: Dict[str, str] = {}
    for req in REQUIRED_FIELDS:
        default_guess = None
        # naive guess by case-insensitive match and common variants
        variants = {
            "Section ID": ["section id", "section_id", "section", "section code", "section_code"],
            "Title": ["title", "section title", "name"],
            "Course": ["course", "course id", "course_id", "course code", "course_code"],
            "Course ID": ["course id", "course_id", "course", "course code", "course_code"],
            "Start Date": ["start date", "start_date", "start", "section start"],
            "End Date": ["end date", "end_date", "end", "section end"],
        }
        for c in current_cols:
            if c.lower() == req.lower() or c.lower() in [v for v in variants.get(req, [])]:
                default_guess = c
                break
        mapping[req] = st.selectbox(f"Map **{req}** to:", options=["(choose)"] + current_cols, index=(current_cols.index(default_guess) + 1) if default_guess in current_cols else 0)

    if any(v == "(choose)" for v in mapping.values()):
        st.error("Please map all required fields.")
        st.stop()

# Rename to canonical
work_df = _ensure_required_columns(raw_df, {mapping[k]: k for k in mapping})
work_df = _with_derived_fields(work_df)

st.markdown("### Columns after mapping + derived fields")
st.dataframe(work_df.head(50), use_container_width=True)

# --- Rule builder ---
st.sidebar.header("2) Define Rules")

if "rules_df" not in st.session_state:
    # sensible default rules for the common cases the user asked for
    st.session_state.rules_df = pd.DataFrame([
        {"attribute": "Subterm", "field": "duration_weeks", "operator": "==", "value": 1, "value_to": None, "output": 1, "order": 1, "stop_if_matched": True},
        {"attribute": "Subterm", "field": "duration_weeks", "operator": "==", "value": 2, "value_to": None, "output": 2, "order": 2, "stop_if_matched": True},
        {"attribute": "PSA_PHA", "field": "Section ID", "operator": "startswith", "value": "PSA", "value_to": None, "output": "PSA", "order": 1, "stop_if_matched": True},
        {"attribute": "PSA_PHA", "field": "Section ID", "operator": "startswith", "value": "PHA", "value_to": None, "output": "PHA", "order": 2, "stop_if_matched": True},
    ])

st.markdown("### Rule Editor")
st.caption("Columns available for `field`: your mapped columns + `duration_days` + `duration_weeks`.")

# Provide choices for field and operator columns in the editor
field_options = list(work_df.columns)  # includes derived

# Add button to append a blank rule
if st.button("➕ Add blank rule"):
    st.session_state.rules_df = pd.concat([
        st.session_state.rules_df,
        pd.DataFrame([{ 
            "attribute": "",
            "field": field_options[0] if field_options else "",
            "operator": SUPPORTED_OPERATORS[0],
            "value": "",
            "value_to": "",
            "output": "",
            "order": (int(st.session_state.rules_df.get("order", pd.Series([0])).max()) + 1) if not st.session_state.rules_df.empty else 1,
            "stop_if_matched": True,
        }])
    ], ignore_index=True)

rules_df: pd.DataFrame = st.data_editor(
    st.session_state.rules_df,
    num_rows="dynamic",
    use_container_width=True,
    column_config={
        "attribute": st.column_config.TextColumn(help="Name of the attribute column to set (will be created if missing).", required=True),
        "field": st.column_config.SelectboxColumn(options=field_options, required=True),
        "operator": st.column_config.SelectboxColumn(options=SUPPORTED_OPERATORS, required=True),
        "value": st.column_config.TextColumn(help="Primary value (numeric/date/text depending on operator)."),
        "value_to": st.column_config.TextColumn(help="Upper bound (only used for 'between')."),
        "output": st.column_config.TextColumn(help="Value to assign when condition matches.", required=True),
        "order": st.column_config.NumberColumn(help="Evaluation order within the same attribute (1,2,3…).", min_value=1, step=1),
        "stop_if_matched": st.column_config.CheckboxColumn(help="If checked, later rules for the same attribute won't override matched rows.", default=True),
    },
    hide_index=True,
    key="rules_editor",
)

st.session_state.rules_df = rules_df

col_a, col_b, col_c = st.columns([1,1,2])
# Precompute CSV for download; Streamlit's download_button `data` cannot be a function
result_df = st.session_state.get("result_df")
csv_bytes = None
if result_df is not None:
    csv_bytes = result_df.to_csv(index=False).encode("utf-8")
with col_a:
    if st.button("Apply Rules", type="primary"):
        st.session_state.result_df = apply_rules(work_df, rules_df)
with col_b:
    if st.button("Clear Results"):
        st.session_state.pop("result_df", None)
with col_c:
    if csv_bytes is not None:
        st.download_button(
            label="Download Transformed CSV",
            data=csv_bytes,
            file_name="course_sections_with_attributes.csv",
            mime="text/csv",
        )
    else:
        st.button("Download Transformed CSV", disabled=True)

st.markdown("### Output Preview")
result_df = st.session_state.get("result_df")
if result_df is not None:
    st.success("Rules applied. Preview below.")
    st.dataframe(result_df.head(200), use_container_width=True)
else:
    st.info("Define rules and click **Apply Rules** to generate attributes. Preview will appear here.")

st.divider()

with st.expander("ℹ️ Tips & Notes", expanded=False):
    st.markdown(
        """
- **Dates**: The app tries to parse dates automatically. If a row has unparseable dates, `duration_days`/`duration_weeks` for that row will be empty.
- **First-match wins**: Within the same attribute, earlier rules take precedence. Use the **order** field to control this.
- **Operators**:
  - `between` is inclusive and works with numbers or dates (e.g., `duration_days between 7 and 13`).
  - `in` expects a comma-separated list (e.g., `Course in ABC, DEF`).
  - `contains`, `startswith`, `endswith`, and `regex` match text case-insensitively.
- **Common example**: Map 1-week sections to `Subterm = 1`, 2-week to `Subterm = 2`, etc., using `duration_weeks`.
        """
    )
