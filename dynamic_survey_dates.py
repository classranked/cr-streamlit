import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(page_title="Survey Date Generator", page_icon="📅", layout="wide")
st.title("📅 Custom Survey Date Generator")

st.markdown("Upload your course section CSV with `Section ID`, `Start Date`, and `End Date`. Define custom rules based on course duration.")

# --- Upload CSV File ---
uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file, parse_dates=["Start Date", "End Date"])
    
    required_cols = {"Section ID", "Start Date", "End Date"}
    if not required_cols.issubset(set(df.columns)):
        st.error(f"CSV must contain the following columns: {', '.join(required_cols)}")
    else:
        st.success("File uploaded successfully.")

        # --- Rule Definition ---
        st.header("Define Rules")
        num_rules = st.number_input("How many rules do you want to define?", min_value=1, max_value=10, value=1, step=1)

        rules = []

        for i in range(num_rules):
            with st.expander(f"Rule {i+1}"):
                min_weeks = st.number_input(f"Rule {i+1} - Min Course Duration (weeks)", min_value=1, value=1)
                max_weeks = st.number_input(f"Rule {i+1} - Max Course Duration (weeks)", min_value=min_weeks, value=min_weeks+4)

                rule_mode = st.selectbox(f"Rule {i+1} - Date Mode", ["Percentage-based", "Day-based (relative to End Date)"], key=f"mode_{i}")

                if rule_mode == "Percentage-based":
                    survey_start_val = st.number_input(f"Start at X% through the course", key=f"start_val_{i}", step=1)
                    survey_end_val = st.number_input(f"End at Y% through the course", key=f"end_val_{i}", step=1)
                else:
                    survey_start_val = st.number_input(f"Survey Start: Days before course end", key=f"start_val_{i}", step=1)
                    survey_end_val = st.number_input(f"Survey End: Days before course end", key=f"end_val_{i}", step=1)

                admin_release_offset = st.number_input(f"When should the admin see the results? (days after survey ends)", value=3, key=f"admin_{i}", step=1)
                instructor_release_offset = st.number_input(f"When should instructors see the results? (days after survey ends)", value=7, key=f"instr_{i}", step=1)

                rules.append({
                    "min_days": min_weeks * 7,
                    "max_days": max_weeks * 7,
                    "mode": rule_mode,
                    "survey_start_val": survey_start_val,
                    "survey_end_val": survey_end_val,
                    "admin_release": admin_release_offset,
                    "instructor_release": instructor_release_offset
                })

        # --- Helper Functions ---
        def apply_rule(row, rules):
            duration = (row["End Date"] - row["Start Date"]).days
            for rule in rules:
                if rule["min_days"] <= duration <= rule["max_days"]:
                    start = row["Start Date"]
                    end = row["End Date"]

                    if rule["mode"] == "Percentage-based":
                        survey_start = start + timedelta(days=round(duration * rule["survey_start_val"] / 100))
                        survey_end = start + timedelta(days=round(duration * rule["survey_end_val"] / 100))
                        survey_end = survey_end.replace(hour=23, minute=59, second=59)
                    else:
                        survey_end = end - timedelta(days=rule["survey_end_val"])
                        survey_end = survey_end.replace(hour=23, minute=59, second=59)
                        survey_start = end - timedelta(days=rule["survey_start_val"])

                    admin_release = survey_end.replace(hour=0, minute=0, second=0) + timedelta(days=rule["admin_release"])
                    instructor_release = survey_end.replace(hour=0, minute=0, second=0) + timedelta(days=rule["instructor_release"])

                    return pd.Series([survey_start, survey_end, admin_release, instructor_release])

            return pd.Series([None, None, None, None])

        # --- Apply Rules ---
        st.header("Apply Rules")
        if st.button("Generate Survey Dates"):
            date_cols = df.apply(lambda row: apply_rule(row, rules), axis=1)
            date_cols.columns = ["Survey Start Date", "Survey End Date", "Admin Release Date", "Instructor Release Date"]
            result_df = pd.concat([df[["Section ID"]], date_cols], axis=1)

            st.success("Survey dates generated.")
            st.dataframe(result_df)

            csv_out = result_df.to_csv(index=False).encode("utf-8")
            st.download_button("Download Result CSV", csv_out, file_name="survey_dates_output.csv", mime="text/csv")
