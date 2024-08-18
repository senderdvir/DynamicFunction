import pandas as pd


# Define CountRecords function
def count_records(df: pd.DataFrame, range_between):
    print(len(df))
    if range_between[0] < len(df) < range_between[1]:
        return True
    else:
        return False

def say_hello(name):
    print(name)


# Business Tests
# validate data types and columns names  - The Schema Test - Currently TBD
# def validate_data_types(df: pd.DataFrame, expected_types):
# This function check data types at given df.
#
#    :param:
#        df - input df .....
#        expected_types - list\dict\string.....
#    :return: if ..... return true
#            else return false
#    """
#   mismatched_types = {}
#  for column, expected_type in expected_types.items():
#       if not df[column].dtype == expected_type:
#           mismatched_types[column] = (df[column].dtype, expected_type)
#   print(bool(not mismatched_types), mismatched_types)
#   return bool(not mismatched_types), mismatched_types

# verifying that the data is updated
def validate_date_range(df: pd.DataFrame, date_column, start_date, end_date):
    invalid_dates = df[date_column][(df[date_column] < start_date) | (df[date_column] > end_date)]
    return (not invalid_dates.empty, invalid_dates.tolist()
    if not invalid_dates.empty else None)


# check if unique columns containing nulls
def check_nulls(df: pd.DataFrame, column_names):
    null_count = df[column_names].isnull().sum()  # Count nulls in the specified column
    if null_count > 0:
        return True, null_count
    else:
        return False


# check if unique columns containing duplicates
def check_duplicates(df: pd.DataFrame, key_columns):
    duplicate_count = df.duplicated(subset=key_columns).sum()
    if duplicate_count > 0:
        return True
    else:
        return False, duplicate_count


# function to verify there are no missing files compared to control file - TBD

# function checks if the values in a required specific range
def validate_data_ranges(df: pd.DataFrame, column_name, min_value, max_value):
    out_of_range = df[(df[column_name] < min_value) | (df[column_name] > max_value)]
    if not out_of_range.empty:
        return True, out_of_range
    return False, None


#  Checks for missing (NaN) values in the specified columns of a DataFrame.
def validate_missing_values(df: pd.DataFrame, column_names: list) -> tuple:
    """
    :param df: The DataFrame to check for missing values.
    :param column_names: A list of column names to check for missing values.
    :return: A tuple containing a boolean and a dictionary:
             - The boolean is True if missing values are found, otherwise False.
             - The dictionary contains the count of missing values per column.
    """
    # Ensure that column_names is a list, even if a single column name is passed as a string
    if isinstance(column_names, str):
        column_names = [column_names]

    missing_values = df[column_names].isna().sum()

    # Ensure missing_values is a Pandas Series, then filter out columns with zero missing values
    if isinstance(missing_values, pd.Series):
        missing_values = missing_values[missing_values > 0].to_dict()
    else:
        missing_values = {}

    if missing_values:
        return True, missing_values
    else:
        return False, {}, print("No missing values detected.")
