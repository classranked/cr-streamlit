import streamlit as st
import pandas as pd

st.title("Repeat Per Instructor Survey Processing")

st.markdown("""
Upload the three CSV files: Course Sections, Instructor Assignments, and Student Enrollments.
Sections taught by multiple instructors will be duplicated into instructor-specific shells.
""")

# File upload
sections_file = st.file_uploader("Upload Course Sections CSV", type="csv")
instructors_file = st.file_uploader("Upload Instructor Assignments CSV", type="csv")
enrollments_file = st.file_uploader("Upload Student Enrollments CSV", type="csv")

variant = st.selectbox(
    "Select Enrollment Variant",
    ["Variant 1: Repeat all enrollments", "Variant 2: Enroll per 'Instructor' column"]
)

def get_email_prefix(email: str) -> str:
    return email.split("@")[0]

def process_sections(
    sections_df: pd.DataFrame,
    instructors_df: pd.DataFrame,
    enrollments_df: pd.DataFrame,
    variant: str
):
    # --- Sanitize inputs ---
    sections_df = sections_df.drop_duplicates()
    instructors_df = instructors_df.drop_duplicates()
    enrollments_df = enrollments_df.drop_duplicates()

    # --- Build section → instructors mapping ---
    mapping = (
        instructors_df
        .groupby("Academic Unit")["Email"]
        .apply(list)
        .reset_index()
        .rename(columns={"Email": "EmailList"})
    )
    mapping["NumInstructors"] = mapping["EmailList"].apply(len)

    # --- Merge mapping into sections ---
    sections = sections_df.merge(
        mapping, left_on="Section ID", right_on="Academic Unit", how="left"
    )
    sections["NumInstructors"] = sections["NumInstructors"].fillna(0).astype(int)
    sections["EmailList"] = sections["EmailList"].apply(lambda x: x if isinstance(x, list) else [])

    # --- Split unchanged vs multi-instructor ---
    unchanged = sections[sections["NumInstructors"] <= 1]
    multi = sections[sections["NumInstructors"] > 1]

    # Prepare outputs
    out_sections = []
    out_instructors = []
    out_enrollments = []
    removed_sections = multi[sections_df.columns].copy()

    # --- Process unchanged sections ---
    for _, row in unchanged.iterrows():
        secid = row["Section ID"]
        out_sections.append(row[sections_df.columns].to_dict())
        # Preserve full instructor assignment records
        for email in row["EmailList"]:
            instr_rows = instructors_df[
                (instructors_df["Academic Unit"] == secid) &
                (instructors_df["Email"] == email)
            ]
            for _, instr_row in instr_rows.iterrows():
                rec = instr_row.to_dict()
                rec["Academic Unit"] = secid
                out_instructors.append(rec)

    # --- Process multi-instructor sections ---
    section_map = {}
    for _, row in multi.iterrows():
        original_id = row["Section ID"]
        for email in row["EmailList"]:
            prefix = get_email_prefix(email)
            new_id = f"{original_id} ({prefix})"
            section_map[(original_id, email)] = new_id

            new_row = row[sections_df.columns].to_dict()
            new_row["Section ID"] = new_id
            out_sections.append(new_row)

            # Preserve full instructor assignment records for each shell
            instr_rows = instructors_df[
                (instructors_df["Academic Unit"] == original_id) &
                (instructors_df["Email"] == email)
            ]
            for _, instr_row in instr_rows.iterrows():
                rec = instr_row.to_dict()
                rec["Academic Unit"] = new_id
                out_instructors.append(rec)

    # --- Process enrollments ---
    for _, row in enrollments_df.iterrows():
        secid = row["Academic Unit"]
        instr_col = row.get("Instructor", None)
        # Determine emails for this section
        mask = (mapping["Academic Unit"] == secid)
        emails = mapping.loc[mask, "EmailList"].iloc[0] if mask.any() else []

        if secid in multi["Section ID"].values:
            if variant.startswith("Variant 1") or pd.isna(instr_col):
                # Duplicate for all instructor shells
                for email in emails:
                    new_row = row.copy()
                    new_row["Academic Unit"] = section_map.get((secid, email), secid)
                    out_enrollments.append(new_row.to_dict())
            else:
                # Variant 2: use specified Instructor column
                if (secid, instr_col) in section_map:
                    new_row = row.copy()
                    new_row["Academic Unit"] = section_map[(secid, instr_col)]
                    out_enrollments.append(new_row.to_dict())
                else:
                    # Fallback: keep as-is
                    out_enrollments.append(row.to_dict())
        else:
            out_enrollments.append(row.to_dict())

    # --- Build DataFrames and drop duplicates ---
    new_sections_df = pd.DataFrame(out_sections).drop_duplicates(subset=["Section ID"])
    new_instructors_df = pd.DataFrame(out_instructors).drop_duplicates()
    new_enrollments_df = pd.DataFrame(out_enrollments).drop_duplicates()

    return new_sections_df, new_instructors_df, new_enrollments_df, removed_sections

# --- Main flow ---
if sections_file and instructors_file and enrollments_file:
    # Read and de-duplicate Course Sections, then report counts
    raw_sections_df = pd.read_csv(sections_file)
    deduped_sections_df = raw_sections_df.drop_duplicates()
    st.write(f"Sections after de-duplication: {len(deduped_sections_df)} (removed {len(raw_sections_df) - len(deduped_sections_df)} duplicate rows)")
    sections_df = deduped_sections_df
    instructors_df = pd.read_csv(instructors_file)
    enrollments_df = pd.read_csv(enrollments_file)

    all_sections = []
    all_instructors = []
    all_enrollments = []

    def to_csv(df: pd.DataFrame) -> bytes:
        return df.to_csv(index=False).encode("utf-8")

    terms = sections_df["Term"].unique()
    for term in terms:
        term_secs = sections_df[sections_df["Term"] == term]
        term_instr = instructors_df[instructors_df["Term"] == term]
        term_enr = enrollments_df[enrollments_df["Term"] == term]
        new_s, new_i, new_e, removed = process_sections(term_secs, term_instr, term_enr, variant)
        # Truncate Section ID to 32 chars, ensuring it ends with ')'
        if 'Section ID' in new_s.columns:
            new_s['Section ID'] = new_s['Section ID'].astype(str).apply(
                lambda x: x if len(x) <= 32 else x[:31] + ')'
            )

        # Truncate Academic Unit in instructor assignments
        if 'Academic Unit' in new_i.columns:
            new_i['Academic Unit'] = new_i['Academic Unit'].astype(str).apply(
                lambda x: x if len(x) <= 32 else x[:31] + ')'
            )

        # Truncate Academic Unit in student enrollments
        if 'Academic Unit' in new_e.columns:
            new_e['Academic Unit'] = new_e['Academic Unit'].astype(str).apply(
                lambda x: x if len(x) <= 32 else x[:31] + ')'
            )
        st.subheader(f"Term: {term}")
        st.download_button(f"Download Course Sections {term}", to_csv(new_s), file_name=f"updated_course_sections_{term}.csv", mime="text/csv")
        st.download_button(f"Download Instructor Assignments {term}", to_csv(new_i), file_name=f"updated_instructor_assignments_{term}.csv", mime="text/csv")
        st.download_button(f"Download Student Enrollments {term}", to_csv(new_e), file_name=f"updated_student_enrollments_{term}.csv", mime="text/csv")

        all_sections.append(new_s)
        all_instructors.append(new_i)
        all_enrollments.append(new_e)

    # Merged outputs across all terms
    st.subheader("Merged Outputs")
    merged_sections_df = pd.concat(all_sections, ignore_index=True)
    merged_instructors_df = pd.concat(all_instructors, ignore_index=True)
    merged_enrollments_df = pd.concat(all_enrollments, ignore_index=True)

    st.download_button(
        "Download All Course Sections",
        to_csv(merged_sections_df),
        file_name="all_updated_course_sections.csv",
        mime="text/csv"
    )
    st.download_button(
        "Download All Instructor Assignments",
        to_csv(merged_instructors_df),
        file_name="all_updated_instructor_assignments.csv",
        mime="text/csv"
    )
    st.download_button(
        "Download All Student Enrollments",
        to_csv(merged_enrollments_df),
        file_name="all_updated_student_enrollments.csv",
        mime="text/csv"
    )
