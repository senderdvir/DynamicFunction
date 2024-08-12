# Define CountRecords function
import pandas as pd


def count_records(df: pd.DataFrame, range_between: list) -> bool:
    """
    Checks if the number of records in the DataFrame falls within a specified range.

    :param df: The pandas DataFrame whose records are to be counted.
    :param range_between: A list containing two elements: the minimum and maximum allowable record count.
    :return: True if the number of records falls within the specified range, otherwise False.
    """
    # Calculate the number of records in the DataFrame
    record_count = len(df)

    # Print the number of records for debugging or logging purposes
    print(record_count)

    # Check if the record count is within the specified range
    if range_between[0] < record_count < range_between[1]:
        return True
    else:
        return False


def data_quality_test_null_values(df: pd.DataFrame) -> bool:
    """
    Checks if there are any null values in the DataFrame.

    :param df: The pandas DataFrame whose null values are to be checked.
    :return: True if there are no null values, otherwise False.
    """
    # Check if there are any null values in the DataFrame
    if df.isnull().values.any():
        return False
    else:
        return True


def quality_test_schema_check(df: pd.DataFrame) -> bool:
    """
    Checks if the DataFrame has the expected schema.

    :param df: The pandas DataFrame whose schema is to be checked.
    :return: True if the DataFrame has the expected schema, otherwise False.
    """
    # Check if the DataFrame has the expected schema
    if df.columns.tolist() == ["Name", "Age"]:
        return True
    else:
        return False


def datatypes_validator_by_schema(df: pd.DataFrame, schema: dict) -> bool:
    """
    Checks if the datatypes in the DataFrame match the expected schema.

    :param df: The pandas DataFrame whose datatypes are to be checked.
    :param schema: A dictionary containing the expected datatypes for each column.
    :return: True if the datatypes in the DataFrame match the expected schema, otherwise False.
    """
    # Check if the datatypes in the DataFrame match the expected schema
    if df.dtypes.to_dict() == schema:
        return True
    else:
        return False


