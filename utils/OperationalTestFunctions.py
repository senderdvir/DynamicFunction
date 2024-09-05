# Define CountRecords function
import pandas as pd
from datetime import datetime


# def function_template(df: pd.DataFrame, other_params):
#     logs_value = []  # Initialize logs list
#
#     # Example operation
#     logs_value.append("Starting function XYZ")
#     # Perform some operation
#     result = some_operation()
#
#     if some_condition:
#         logs_value.append("Condition met, returning success")
#
#     else:
#         logs_value.append("Condition not met, returning failure")
#         return False, "\n".join(logs_value)

# Compare the record count in the DataFrame with the count provided in the control file.
def count_records(df: pd.DataFrame, range_between):
    logs_value = []  # Initialize logs list
    records_count = len(df)
    logs_value.append( f"Number of records: {records_count}" )
    if range_between[0] < records_count < range_between[1]:
        logs_value.append( "Result: according to expected" )
        return True, "\n ".join( logs_value )  # Join logs list into a single string

    else:
        logs_value.append( "Result: Out of the expected range" )
        logs_value.append( f"Count records in this file: {records_count}" )
        return False, "\n".join( logs_value )


# Compare the sum of a specific column in the DF with the sum provided in the control file.
def validate_sum(df: pd.DataFrame, column, expected_sum):
    logs_value = []
    actual_sum = df[column].sum()
    logs_value.append( f"Actual sum of {column}: {actual_sum}" )
    if actual_sum == expected_sum:
        logs_value.append( f"Result: The actual sum is the expected sum: {expected_sum}" )
        return True, "\n".join( logs_value )  # Return tuple (result, logs)
    else:
        logs_value.append( f"Result: Expected sum {expected_sum}, but got {actual_sum}" )
        return False, "\n".join( logs_value )  # Return tuple (result, logs)


# ensures that numeric values fall within a specific range - business requirement.
def validate_numeric_range(df: pd.DataFrame, column_name, min_value, max_value):
    logs_value = []
    out_of_range = df[(df[column_name] < min_value) | (df[column_name] > max_value)]
    if out_of_range.empty:
        logs_value.append( f"All values in '{column_name}' are within the range [{min_value}, {max_value}]." )
        return True, "\n".join( logs_value )
    else:
        logs_value.append( f"Out of range values in '{column_name}': {out_of_range[column_name].tolist()}" )
        return False, "\n ".join( logs_value )


# a check to see if all FK values exist in the PK or reference column
def check_foreign_key(df: pd.DataFrame, column_name, reference_pk):
    logs_value = []
    invalid_values = df[~df[column_name].isin(reference_pk)]
    if invalid_values.empty:
        logs_value.append( "The foreign key values are valid" )
        return True, "\n".join( logs_value )
    else:
        logs_value.append( f"Invalid foreign key values in {column_name}: {invalid_values[column_name].tolist()}" )
        return False, "\n".join( logs_value )


# ensure that the data is recent by comparing the date column with the current date
def file_freshness(df: pd.DataFrame, date_column):
    logs_value = []
    current_date = datetime.now().date()
    if df[date_column].max().date() == current_date:
        logs_value.append( "The file is updated for today" )
        return True, "\n".join( logs_value )
    else:
        logs_value.append( f"Data is not fresh. Latest date: {df[date_column].max().date()}" )
        return False, "\n".join( logs_value )


# ensure that the data is recent by comparing the timestamp column (not date). Should consider timezone differences
def file_timestamp(df: pd.DataFrame, timestamp_column: str):
    logs_value = []
    current_timestamp = datetime.now()
    latest_timestamp = df[timestamp_column].max()  # the most recent timestamp from the specified column
    if latest_timestamp == current_timestamp:
        logs_value.append( "The file is updated to the current timestamp" )
        return True, "\n".join( logs_value )
    else:
        logs_value.append( f"Data is not fresh. Latest timestamp: {latest_timestamp}" )
        return False, "\n".join( logs_value )

# verifying that the data is updated according to requirements
def validate_date_range(df: pd.DataFrame, date_column, start_date, end_date):
    logs_value = []
    invalid_dates = df[date_column][(df[date_column] < start_date) | (df[date_column] > end_date)]
    if not invalid_dates.empty:
        logs_value.append( f"The invalid dates are: {invalid_dates.tolist()}" )
        return False, "\n".join( logs_value )
    else:
        logs_value.append( "All dates match the start and end dates" )
        return True, "\n".join( logs_value )


# check if unique columns containing nulls
def check_nulls(df: pd.DataFrame, column):
    logs_value = []
    null_count = df[column].isnull().sum()  # Count nulls in the specified column
    if null_count > 0:
        logs_value.append( f"column: {column} Has nulls: {null_count}" )
        return False, "\n".join( logs_value )
    else:
        logs_value.append( f"The column: {column} has no nulls" )
        return True, "\n".join( logs_value )


# check if unique columns containing duplicates

def check_duplicates(df: pd.DataFrame, key_columns):
    logs_value = []
    duplicate_count = df.duplicated(subset=key_columns).sum()
    if duplicate_count > 0:
        logs_value.append( f"Duplicate records found based on keys {key_columns}: {duplicate_count} duplicates" )
        return False, "\n".join( logs_value )
    else:
        logs_value.append( "No duplicates found in the specified key columns." )
        return True, "\n".join( logs_value )


# Verifies if there are any missing files compared to a list of expected file names
def check_missing_files(df:pd.DataFrame, files_names: list, column_name: str) -> tuple:
    logs_value = []
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
        logs_value.append( "All expected files are received" )
        return True, "\n".join( logs_value )
    else:
        logs_value.append( f"Missing files: {', '.join( missing_files )}" )
        return False, "\n".join( logs_value )
