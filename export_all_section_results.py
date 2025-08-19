"""
===============================================================================
Survey Report Downloader Script
===============================================================================

Description:
------------
This script connects to a REST API to download survey reports (PDF or CSV) for
course sections under a selected survey project (distribution series) and term.

Features:
- Authenticates using tenant-based headers.
- Lets users interactively select a survey project and term (slot).
- Supports downloads by reporting hierarchy or instructor.
- Saves reports with readable filenames and compresses them into a ZIP file.
- Handles pagination for large numbers of sections.

Inputs:
-------
- AUTH_TOKEN: API bearer token (set via environment variable or hardcoded)
- AWS_TENANT_KEY: Custom tenant header (set via env or constant)
- BASE_URL: Base API URL (e.g., "https://api.example.com")
- OUTPUT_DIR: Local path to store reports
- ZIP_FILENAME: Name of the final ZIP file

Outputs:
--------
- PDF or CSV files saved in OUTPUT_DIR
- A final ZIP file containing all downloaded reports
- Console logs for progress, pagination, and saved files

Execution Instructions:
-----------------------
1. Ensure Python 3.7+ is installed.
2. Install required dependencies:
    pip install requests
3. Set the following constants at the top of the script or load them via env:
    - AUTH_TOKEN = "your-auth-token"
    - AWS_TENANT_KEY = "your-tenant-key"
    - BASE_URL = "https://your-api-url"
    - OUTPUT_DIR = "./downloads"
    - ZIP_FILENAME = "survey_reports.zip"
4. Run the script:
    python survey_report_downloader.py
5. Follow CLI prompts to select:
    - Survey project
    - Term (slot)
    - Export format (PDF or CSV)
    - Download method (by reporting hierarchy or instructor)
    - Sleep duration between downloads (to avoid API rate limits)

Author: [Your Name]
Date: [YYYY-MM-DD]
"""

import os
import requests
import zipfile
from io import BytesIO
import time
from uuid import UUID
from dataclasses import dataclass

# Configuration
## Base URL of the API endpoint.
BASE_URL = "https://insights.classranked.com/api"
## Bearer token used for API authorization.
AUTH_TOKEN = "eyJraWQiOiIyMzlrTFU5OER4dzBKcDNpZGJRR1hWSG5HdXBtakNkXC9QOW9sTGY0bWNvaz0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhNDM4MTQ5OC1kMGMxLTcwMWMtZDMzOC01N2JjMDcwZjU0ZjMiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnVzLWVhc3QtMS5hbWF6b25hd3MuY29tXC91cy1lYXN0LTFfS2pXVjIwc1NNIiwiY29nbml0bzp1c2VybmFtZSI6IjcwNTBjN2Q1LTlhZjQtNDFhYi1hNjFhLTlhNTgyNmNhZGZjMiIsImdpdmVuX25hbWUiOiJBZG1pbiIsIm9yaWdpbl9qdGkiOiIyY2U0MWEyYS0wYzczLTQ5ZGUtOWM3NC0yZGQ5MjQ4NTk5MDIiLCJhdWQiOiIyMWw1anVuZHN2b3JxdGI3YW0xMWcxbDZuYyIsImV2ZW50X2lkIjoiM2FiMjA4ZDAtMGEyMS00YTM2LTk3NGEtMmFkMzMzNWEzMWNiIiwidG9rZW5fdXNlIjoiaWQiLCJhdXRoX3RpbWUiOjE3NTM4MDE1MTksImV4cCI6MTc1MzgxMjc4NiwiaWF0IjoxNzUzODA5MTg2LCJmYW1pbHlfbmFtZSI6IkNsYXNzcmFua2VkIiwianRpIjoiMzc5Nzc4MzYtYTUxNi00ZjU2LWFkZTMtMjcyOTNhNTJhZWIyIiwiZW1haWwiOiJhZG1pbkBzbWFpbC5hbmMuZWR1In0.OALRWPlIM2MlonLjPJB93qDtCg1DlIMijSUX5PltkAbdhGcD5NkpcN_gIEm1OCk0SCrU00inQgX1EwjgKRFhZe4fsv2FV1ymqLhHVB8yYW_YKUN0wQC0HBhkV5dSLBR0Dzeqkh3gl5CwxbpoPxjzFafvNJ5RkIuctPjWyko2Xra6An0Rs3jatfwCrqHhRqyykdg4SocZ2vo0_DbsedDR0-YxZewmnBDAx61wrhts416RmmNBrWvgjNbdnkGBPox2c6OX28tV9BoQFFbq5up89DD6NHMgQyFPX0BnaR3cSQ0GZyIsJOkMj3N05BuN9rRHgMZk03efTo9LhtbCWPF6pA"
## Custom header for tenant separation.
AWS_TENANT_KEY = "smail.anc.edu"
## Local directory path to store downloaded reports.
OUTPUT_DIR = "exported_pdfs"
## Name of the ZIP file to be created with all downloaded reports.
ZIP_FILENAME = "sections_pdfs.zip"

# Headers for API requests
HEADERS = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "Aws-Tenant-Key": AWS_TENANT_KEY,
}

@dataclass
class DistributionSeries:
    distribution_series_id: UUID
    title: str

@dataclass
class SlotTerm:
    slot_id: UUID
    slot_status: str
    term_name: str
    term_id: UUID


@dataclass
class Instructor:
    name: str
    user_id: UUID

@dataclass
class CourseSection:
    section_id: UUID
    abbreviation: str
    instructors: list[Instructor]


def fetch_distribution_series() -> list[DistributionSeries]:
    """
    Fetch available survey projects (distribution series) from the API.

    Returns:
        list[DistributionSeries]: A list of survey projects with metadata including 'title' and 'distribution_series_id'.
    """
    response = requests.get(f"{BASE_URL}/distribution_series/", headers=HEADERS)
    response.raise_for_status()
    raw_data = response.json()
    return [
        DistributionSeries(
            distribution_series_id=UUID(item["distribution_series_id"]),
            title=item["title"]
        ) for item in raw_data
    ]


def fetch_terms(series_id: UUID) -> list[SlotTerm]:
    """
    Fetch terms (slots) associated with a survey project.

    Args:
        series_id (UUID): The ID of the distribution series.

    Returns:
        list[dict]: A list of slot/term records for the series.
    """
    response = requests.get(f"{BASE_URL}/distribution_series/{series_id}/slots/", headers=HEADERS)
    response.raise_for_status()
    raw_data = response.json()
    return [
        SlotTerm(
            slot_id=UUID(slot["slot_id"]),
            slot_status=slot["slot_node_details"]["slot_status"],
            term_name=slot["term"]["term_name"],
            term_id=UUID(slot["term"]["term_id"])
        )
        for slot in raw_data
        if slot.get("slot_node_details", {}).get("slot_status") == "Closed"
    ]


def fetch_sections(slot_id: UUID):
    """Fetch sections for a specific term from the API."""
    
    headers = {**HEADERS, "Accept": "application/cdg"}
    all_sections = []
    page_number = 1  # Start with the first page

    while True:
        # Include pagination in the API call
        params = {"page_number": page_number}
        response = requests.get(f"{BASE_URL}/slots/{slot_id}/sections/", headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        # Check pagination object
        pagination = data.get("pagination", {})
        print(f"Pagination Info: {pagination}")
        
        for item in data.get("data", []):
            raw = item["data"]
            linked = raw["linked_section"]
            instructors = raw.get("instructors", [])
            section_obj = CourseSection(
                    section_id=UUID(linked["section_id"]),
                    abbreviation=linked["abbreviation"],
                    instructors=[
                        Instructor(name=instr["title"], user_id=UUID(instr["id"]))
                        for instr in instructors
                    ]
                )
            all_sections.append(section_obj)
        if pagination.get("page_number") == pagination.get("num_pages"):
            break  # Exit the loop if we have reached the last page

        # Increment the page number for the next API call
        page_number += 1

    return all_sections


def download_report(section_id: UUID, term_id: UUID, series_id: UUID, export_type: str):
    """
    Download a section's report (PDF or CSV) as binary content.

    Args:
        section_id (UUID): The section ID.
        term_id (UUID): The selected term's ID.
        series_id (UUID): The selected distribution series ID.
        export_type (str): Report format - 'pdf' or 'csv'.

    Returns:
        bytes: Binary content of the downloaded report file.
    """
    params = {
        "term_id": term_id,
        "distribution_series_id": series_id
    }
    if export_type == "pdf":
        url = f"{BASE_URL}/divisions/{section_id}/reports/export/pdf/"
    else:
        url = f"{BASE_URL}/divisions/{section_id}/reports/export/csv/"
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    print(f"Report downloaded for section_id: {section_id}")
    return response.content


def fetch_section_path(section_id: UUID) -> list[dict]:
    response = requests.get(f"{BASE_URL}/divisions/path/{section_id}", headers=HEADERS)
    response.raise_for_status()
    return response.json()


def create_hierarchy_folder_structure(base_path, division_path_response, filename, export_type):
    current_path = base_path
    for entry in division_path_response:
        if entry["model"] == "DIVISION":
            folder_name = entry["title"].replace(" ", "_")
            current_path = os.path.join(current_path, folder_name)
            os.makedirs(current_path, exist_ok=True)
    final_path = os.path.join(current_path, filename + "." + export_type)
    return final_path


def save_report(content, filepath) -> str:
    """
    Save a binary report to a local file.

    Args:
        content (bytes): The file content.
        filename (str): The output filename.
        export_type (str): Report format ('pdf' or 'csv').

    Returns:
        str: Full path to the saved file.
    """
    # Save the file
    with open(filepath, "wb") as pdf_file:
        pdf_file.write(content)
    print(f"Saved Report: {filepath}")
    return filepath


def create_zip_file(directory, zip_filename):
    with zipfile.ZipFile(zip_filename, "w") as zipf:
        for root, _, files in os.walk(directory):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), directory))
    print(f"Created ZIP file: {zip_filename}")


def create_instructor_folder_structure(
        base_path: str,
        instructors: list[Instructor],
        filename: str,
        export_type: str) -> list[str]:
    """
    Creates a folder for each instructor under the base path and returns the full path for saving the report file.
    The report will be saved under the first instructor's folder (assuming single ownership for a section report).

    Args:
        base_path (str): Root directory where folders should be created.
        instructor_names (list[str]): List of instructor names for the section.
        filename (str): Base filename (without extension).
        export_type (str): File extension (e.g., 'pdf' or 'csv').

    Returns:
        list[str]: Full path to where the report should be saved.
    """
    saved_paths = []

    if not instructors:
        folder = os.path.join(base_path, "Unknown_Instructor")
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"{filename}.{export_type}")
        saved_paths.append(path)
    else:
        for instr in instructors:
            instructor_name = instr.name.replace(" ", "_")
            folder = os.path.join(base_path, instructor_name)
            os.makedirs(folder, exist_ok=True)
            path = os.path.join(folder, f"{filename}.{export_type}")
            saved_paths.append(path)

    return saved_paths


def main():
    # Step 1: Fetch and display survey projects
    series = fetch_distribution_series()
    
    print("\nAvailable Survey Projects")
    for idx, project in enumerate(series, start=1):
        print(f"{idx}. {project.title} (ID: {project.distribution_series_id})")
    
    # Step 2: Ask the user to select a survey project
    try:
        series_choice = int(input("Enter the choice of the Survey Project to download the report: "))
        if 1 <= series_choice <= len(series):
            selected_series = series[series_choice - 1]
        else:
            print("Invalid choice for Survey Project")
            exit()
    except ValueError:
        print("Invalid input. Please enter a number.")
    
    # Step 3: Fetch and display terms for the selected survey project
    slots = fetch_terms(selected_series.distribution_series_id)
    
    print(f"\nAvailable Terms for Survey Project: {selected_series.title}:")
    for idx, slot in enumerate(slots, start=1):
        print(f"{idx}. {slot.term_name} (ID: {slot.term_id})")

    # Step 4: Ask the user to select a term
    try:
        term_choice = int(input("Enter the choice of the Term to download the report: "))
        if 1 <= term_choice <= len(slots):
            selected_term = slots[term_choice - 1]
        else:
            print("Invalid choice for Term")
            exit()
    except ValueError:
        print("Invalid input. Please enter a number.")

    #Step 5: Download Preferences
    print("\nAvailable Download Preferences\n1: Download by Reporting Hierarchy\n2: Download by Instructor")
    try:
        download_preference = int(input("Enter the choice of the Download Preference: "))
        if download_preference < 1 or download_preference > 2:
            print("Invalid choice for Download Preference")
            exit()
    except ValueError:
        print("Invalid input. Please enter a number.")

    # Step 5: Fetch sections for the selected term
    sections = fetch_sections(selected_term.slot_id)
    print("\nTotal Sections::", len(sections))

    # Step 6: Create the output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    export_options = ["pdf", "csv"]
    print("\nAvailable Report Export Formats")
    for idx, export in enumerate(export_options, start=1):
        print(f"{idx}. {export}")
    try:
        export_choice = int(input("Enter the choice of the export Type: "))
        if 1 <= export_choice <= 2:
            selected_export_type = export_options[export_choice-1]
        else:
            print("Invalid choice for Export Option")
            exit()
    except ValueError:
        print("Invalid input. Please enter a number.")
    

    # Prompt user for sleep duration
    sleep_input = input("Enter the sleep duration between downloads in seconds (default is 30): ").strip()
    sleep_duration = int(sleep_input) if sleep_input.isdigit() else 30
    print(f"Selected sleep duration: {sleep_duration} seconds")
    
    # Step 7: Download PDFs for each section, if not already downloaded
    for section in sections:
        filename = f"{section.abbreviation}_{'_'.join(instr.name for instr in section.instructors)}_{selected_term.term_name}".replace(" ", "_")
        if download_preference == 1:
            division_path = fetch_section_path(section.section_id)
            filepath = create_hierarchy_folder_structure(OUTPUT_DIR, division_path, filename, selected_export_type)
            if os.path.exists(filepath):
                print(f"Report already exists: {filepath}")
            else:
                content = download_report(
                    section_id=section.section_id,
                    term_id=selected_term.term_id,
                    series_id=selected_series.distribution_series_id,
                    export_type=selected_export_type
                )
                save_report(content, filepath)
                time.sleep(sleep_duration)
        else:
            filepaths = create_instructor_folder_structure(
                OUTPUT_DIR,
                section.instructors,
                filename,
                selected_export_type
            )
            all_exist = all(os.path.exists(path) for path in filepaths)

            if all_exist:
                print(f"Report already exists for all instructors: {filename}")
            else:
                content = download_report(
                    section_id=section.section_id,
                    term_id=selected_term.term_id,
                    series_id=selected_series.distribution_series_id,
                    export_type=selected_export_type
                )
                for path in filepaths:
                    if not os.path.exists(path):
                        save_report(content, path)
                time.sleep(sleep_duration)

    # Step 8: Create a ZIP file containing all the downloaded reports
    create_zip_file(OUTPUT_DIR, ZIP_FILENAME)

    print(f"All PDFs are zipped and ready to download: {ZIP_FILENAME}")


if __name__ == "__main__":
    main()
