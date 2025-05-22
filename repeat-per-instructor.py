import streamlit as st
import pandas as pd
import io

st.title("Repeat Per Instructor Survey Processing")

st.markdown("""
Upload the three CSV files: Course Sections, Instructor Assignments, and Student Enrollments.
Select a processing option and download the updated files.
""")

sections_file = st.file_uploader("Upload Course Sections CSV", type="csv")
instructors_file = st.file_uploader("Upload Instructor Assignments CSV", type="csv")
enrollments_file = st.file_uploader("Upload Student Enrollments CSV", type="csv")

processing_option = st.selectbox(
    "Select Processing Option",
    [
        "1. One survey per instructor for each student",
        "2. One survey per instructor but assigned only to matched students",
        "3. All instructors share one survey (no change)"
    ]
)

def get_email_prefix(email):
    return email.split("@")[0]

def process_option_1_2(sections_df, instructors_df, enrollments_df, option):
    # Detect sections with multiple instructors
    # Group instructors by Academic Unit
    section_instructors = instructors_df.groupby('Academic Unit')['Email'].apply(list).reset_index()
    multi_instructor_sections = section_instructors[section_instructors['Email'].apply(len) > 1]

    # Create a mapping of old_section_id to new section ids per instructor
    section_instructor_map = {}

    # New sections list
    new_sections = sections_df.copy()

    # New instructors list
    new_instructors_rows = []

    # For enrollments
    new_enrollments_rows = []

    # Set of sections with multiple instructors
    multi_section_ids = set(multi_instructor_sections['Academic Unit'])

    # Process each section
    for idx, row in sections_df.iterrows():
        section_id = row['Section ID']
        if section_id in multi_section_ids:
            # Multiple instructors
            instructors_list = section_instructors[section_instructors['Academic Unit'] == section_id]['Email'].values[0]
            for instructor_email in instructors_list:
                email_prefix = get_email_prefix(instructor_email)
                new_section_id = f"{section_id} ({email_prefix})"
                section_instructor_map[(section_id, instructor_email)] = new_section_id

                # Add new section row
                new_row = row.copy()
                new_row['Section ID'] = new_section_id
                new_sections = pd.concat([new_sections, pd.DataFrame([new_row])], ignore_index=True)

                # Add instructor assignment row
                new_instructors_rows.append({
                    'Academic Unit': new_section_id,
                    'Email': instructor_email
                })

            # Remove original section row
            new_sections = new_sections[new_sections['Section ID'] != section_id]

        else:
            # Single instructor section, keep as is
            instructors_for_section = instructors_df[instructors_df['Academic Unit'] == section_id]
            for _, instr_row in instructors_for_section.iterrows():
                new_instructors_rows.append({
                    'Academic Unit': section_id,
                    'Email': instr_row['Email']
                })

    new_instructors_df = pd.DataFrame(new_instructors_rows)

    if option == "1. One survey per instructor for each student":
        # For each enrollment in a multi-instructor section, duplicate for each instructor-specific section
        for idx, enr_row in enrollments_df.iterrows():
            section_id = enr_row['Academic Unit']
            if section_id in multi_section_ids:
                instructors_list = section_instructors[section_instructors['Academic Unit'] == section_id]['Email'].values[0]
                for instructor_email in instructors_list:
                    new_section_id = section_instructor_map[(section_id, instructor_email)]
                    new_enr_row = enr_row.copy()
                    new_enr_row['Academic Unit'] = new_section_id
                    new_enrollments_rows.append(new_enr_row)
            else:
                new_enrollments_rows.append(enr_row)

    elif option == "2. One survey per instructor but assigned only to matched students":
        # We expect a column 'Email' in enrollments to specify matched instructors
        if 'Email' not in enrollments_df.columns:
            st.error("Enrollments file must include an 'Email' column for Option 2.")
            return None, None, None

        for idx, enr_row in enrollments_df.iterrows():
            section_id = enr_row['Academic Unit']
            student_instructor_email = enr_row['Email']
            if section_id in multi_section_ids:
                # Assign to the instructor-specific section only if instructor matches
                if (section_id, student_instructor_email) in section_instructor_map:
                    new_section_id = section_instructor_map[(section_id, student_instructor_email)]
                    new_enr_row = enr_row.copy()
                    new_enr_row['Academic Unit'] = new_section_id
                    new_enrollments_rows.append(new_enr_row)
                else:
                    # Student assigned to instructor not in section, skip
                    pass
            else:
                new_enrollments_rows.append(enr_row)

    new_enrollments_df = pd.DataFrame(new_enrollments_rows)

    # Remove duplicates if any
    new_sections = new_sections.drop_duplicates(subset=['Section ID'])
    new_instructors_df = new_instructors_df.drop_duplicates()
    new_enrollments_df = new_enrollments_df.drop_duplicates()

    return new_sections, new_instructors_df, new_enrollments_df

if sections_file and instructors_file and enrollments_file:
    sections_df = pd.read_csv(sections_file)
    instructors_df = pd.read_csv(instructors_file)
    enrollments_df = pd.read_csv(enrollments_file)

    if processing_option == "3. All instructors share one survey (no change)":
        st.write("No changes made for Option 3.")
        new_sections_df = sections_df
        new_instructors_df = instructors_df
        new_enrollments_df = enrollments_df
    else:
        result = process_option_1_2(sections_df, instructors_df, enrollments_df, processing_option)
        if all(x is None for x in result):
            st.stop()
        new_sections_df, new_instructors_df, new_enrollments_df = result

    def convert_df_to_csv(df):
        return df.to_csv(index=False).encode('utf-8')

    st.download_button(
        label="Download Updated Course Sections CSV",
        data=convert_df_to_csv(new_sections_df),
        file_name='updated_course_sections.csv',
        mime='text/csv'
    )
    st.download_button(
        label="Download Updated Instructor Assignments CSV",
        data=convert_df_to_csv(new_instructors_df),
        file_name='updated_instructor_assignments.csv',
        mime='text/csv'
    )
    st.download_button(
        label="Download Updated Student Enrollments CSV",
        data=convert_df_to_csv(new_enrollments_df),
        file_name='updated_student_enrollments.csv',
        mime='text/csv'
    )
