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

