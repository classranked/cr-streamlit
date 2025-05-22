import streamlit as st
from find_duplicates import find_duplicates
import os
import tempfile

# Dictionary to map script names to their corresponding functions and required number of files
scripts = {
    "Find Duplicate Sections": {
        "function": find_duplicates,
        "num_files": 2,
        "columns": ["Section ID"]
    },
    "Find Duplicate Instructors": {
        "function": find_duplicates,
        "num_files": 2,
        "columns": ["First Name", "Last Name", "School ID"]
    },
}

# Streamlit app
st.title("File Processing App")

# Script selection
script_name = st.selectbox("Select a script", list(scripts.keys()))

# Get the number of files required for the selected script
num_files = scripts[script_name]["num_files"]

# File upload
uploaded_files = []
for i in range(num_files):
    uploaded_file = st.file_uploader(f"Upload file {i+1}", type="csv", key=f"file_{i}")
    if uploaded_file is not None:
        uploaded_files.append(uploaded_file)

# Process files
if len(uploaded_files) == num_files:
    if st.button("Process Files"):
        # Save uploaded files to temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            file_paths = []
            for i, uploaded_file in enumerate(uploaded_files):
                file_path = os.path.join(temp_dir, f"uploaded_file_{i}.csv")
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                file_paths.append(file_path)

            # Call the corresponding script function with the uploaded files and additional parameters
            csv_data = scripts[script_name]["function"](
                file_paths[0],
                file_paths[1],
                *scripts[script_name]["columns"]
            )

            if csv_data:
                # Provide a download button for the output file
                st.download_button(
                    label="Download Output File",
                    data=csv_data,
                    file_name=f"{script_name.replace(' ', '_').lower()}.csv",
                    mime="text/csv"
                )
