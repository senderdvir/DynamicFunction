# Define CountRecords function
import pandas as pd


def count_records(df, range_between):
    print(len(df))
    if range_between[0] < len(df) < range_between[1]:
        return True
    else:
        return False


def say_hello(name):
    print(name)


# validate data types - consistency test
# def validate_data_types(df: pd.DataFrame, expected_types):
#     """
#     This function check data types at given df.
#
#     :param:
#         df - input df .....
#         expected_types - list\dict\string.....
#     :return: if ..... return true
#             else return false
#     """
#     mismatched_types = {}
#     for column, expected_type in expected_types.items():
#         if not df[column].dtype == expected_type:
#             mismatched_types[column] = (df[column].dtype, expected_type)
#     print(bool(not mismatched_types), mismatched_types)
#     return bool(not mismatched_types), mismatched_types


# verifying that the data is updated
def validate_date_range(df: pd.DataFrame, date_column, start_date, end_date):
    invalid_dates = df[date_column][(df[date_column] < start_date) | (df[date_column] > end_date)]
    print(f'The invalid dates: {invalid_dates}')
    return (not invalid_dates.empty, invalid_dates.tolist()
    if not invalid_dates.empty else None)


# check if unique columns containing nulls
def check_nulls(df: pd.DataFrame, column):
    null_count = df[column].isnull().sum()  # Count nulls in the specified column
    has_nulls = null_count > 0
    print(f'Has null: {has_nulls}')
    return has_nulls


def sayHello():
    print("hello")




# check if unique columns containing duplicates

def check_duplicates(df, columns):
    duplicates = {}
    has_duplicates = False

    for column in columns:
        duplicate_rows = df[df.duplicated(subset=[column], keep=False)]
        duplicate_count = len(duplicate_rows)

        if duplicate_count > 0:
            duplicates[column] = duplicate_count
            has_duplicates = True

    return has_duplicates, duplicates


# function to verify there are no missing files compared to control file

def check_missing_files(received_df, control_df):
    # Assume both DataFrames have a 'file_name' column
    received_files = set(received_df['file_name'])
    expected_files = set(control_df['file_name'])

    # Determine the missing files
    missing_files = expected_files - received_files

    # Check if all expected files are received
    is_matched = len(missing_files) == 0

    return is_matched, list(missing_files) if not is_matched else None
