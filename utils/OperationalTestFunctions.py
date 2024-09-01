# Define CountRecords function
import pandas as pd

# Compare the record count in the DataFrame with the count provided in the control file.Test is passed.
def count_records(df: pd.DataFrame, range_between):
    records_count = len(df)
    if range_between[0] < records_count < range_between[1]:
        return True, f"Equal, {records_count}"
    else:
        return False, f"Out of the expected range, count records in this file: {records_count}"


# Compare the sum of a specific column in the DataFrame with the sum provided in the control file. Test is passed.
def validate_sum(df: pd.DataFrame, column, expected_sum):
    actual_sum = df[column].sum()
    if actual_sum == expected_sum:
        return True, "The actual sum is the expected sum"
    else:
        return False, f"Expected sum {expected_sum}, but got {actual_sum}"


# ensures that numeric values fall within a specific range - business requirement. Test is failed.
def validate_numeric_range(df: pd.DataFrame, column_name, min_value, max_value):
    out_of_range = df[(df[column_name] < min_value) | (df[column_name] > max_value)]
    if out_of_range.empty:
        return True, f"All values in '{column_name}' are within the range [{min_value}, {max_value}]."
    else:
        return False, f"Out of range values in '{column_name}': {out_of_range[column_name].tolist()}"


# a check to see if all FK values exist in the PK or reference column
def check_foreign_key(df: pd.DataFrame, column_name, reference_pk):
    invalid_values = df[~df[column_name].isin(reference_pk)]
    if invalid_values.empty:
        return True, "The foreign key values are valid"
    else:
        return False, f"Invalid foreign key values in {column_name}: {invalid_values[column_name].tolist()}"


# ensure that the data is recent by comparing the timestamp\date column with the current date
def file_freshness(df: pd.DataFrame, date_column, current_date):
    if df[date_column].max().date() == current_date:
        return True, "The file is updated for today"
    else:
        return False, f"Data is not fresh. Latest date: {df[date_column].max().date()}"


# verifying that the data is updated according to requirements
def validate_date_range(df: pd.DataFrame, date_column, start_date, end_date):
    invalid_dates = df[date_column][(df[date_column] < start_date) | (df[date_column] > end_date)]
    if not invalid_dates.empty:
        return False, f"The invalid dates are: {invalid_dates.tolist()}"
    else:
        return True, "All dates match the start and end dates"


# check if unique columns containing nulls
def check_nulls(df: pd.DataFrame, column):
    null_count = df[column].isnull().sum()  # Count nulls in the specified column
    if null_count > 0:
        return False, f"Has nulls: {null_count}"
    else:
        return True, f"The column: {column} has no nulls"


# check if unique columns containing duplicates

def check_duplicates(df: pd.DataFrame, key_columns):
    duplicate_count = df.duplicated(subset=key_columns).sum()
    if duplicate_count > 0:
        return False, f"Duplicate records found based on keys {key_columns}: {duplicate_count} duplicates"
    else:
        return True, "No duplicates found in the specified key columns."


# Verifies if there are any missing files compared to a list of expected file names
def check_missing_files(df:pd.DataFrame, files_names: list, column_name: str) -> tuple:
    """
    :param df: the file with the files names
    :param files_names: A list of expected file names.
    :param column_name: The name of the column that contains the file names in the DataFrame.
    :return: A tuple (is_matched, missing_files).
             is_matched is True if all expected files are received, False otherwise.
             missing_files is a list of missing file names if any files are missing, otherwise None.
    """

    # Get the set of received file names from the specified column
    received_files = set(df[column_name])

    # Convert the expected file names to a set
    expected_files = set(files_names)

    # Determine the missing files by calculating the difference
    missing_files = expected_files - received_files

    # Check if all expected files are received
    is_matched = len(missing_files) == 0

    if is_matched:
        return True, "All expected files are received"
    else:
        return False, f"Missing files: {', '.join(missing_files)}"
