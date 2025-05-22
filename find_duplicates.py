import pandas as pd
from io import StringIO

def find_duplicates(csv1_path, csv2_path, *columns):
    """
    Finds duplicates between two CSVs based on specified columns and returns the CSV data.

    Parameters:
    - csv1_path: Path to the first CSV file.
    - csv2_path: Path to the second CSV file.
    - columns: Columns to compare for duplicates.
    """
    try:
        # Load the CSV files into pandas DataFrames
        df1 = pd.read_csv(csv1_path)
        df2 = pd.read_csv(csv2_path)
    except Exception as e:
        print(f"Error reading the CSV files: {e}")
        return None

    # Ensure the columns exist in both DataFrames
    for column in columns:
        if column not in df1.columns or column not in df2.columns:
            print(f"Error: Column '{column}' not found in one or both CSV files.")
            return None

    # Find duplicates based on the specified columns
    duplicates = pd.merge(df1, df2, on=list(columns))

    if duplicates.empty:
        print("No duplicates found.")
        return None
    else:
        print(f"Found {len(duplicates)} duplicate rows.")

        # Convert the duplicates DataFrame to CSV format
        csv_data = StringIO()
        duplicates.to_csv(csv_data, index=False)
        csv_data.seek(0)
        return csv_data.getvalue()


# Example usage:
if __name__ == "__main__":
    # Input CSV file paths
    csv1_path = "./new-online_v_existing/newSections.csv"  # TODO: Replace with your first CSV file path
    csv2_path = "./new-online_v_existing/existingSections.csv"  # TODO: Replace with your second CSV file path

    # Columns to check for duplicates
    columns = ["Section ID"]  # Replace with the exact column names

    # Call the function
    csv_data = find_duplicates(csv1_path, csv2_path, *columns)
    if csv_data:
        with open("../new-online_v_existing/duplicates.csv", "w") as f:
            f.write(csv_data)
