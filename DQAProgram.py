import pandas as pd
from utils.OperationalTestFunctions import *


def run_dynamic_function(function_name: str, *args, **kwargs):
    """
    Executes a function dynamically based on the function name provided.

    :param function_name: The name of the function to be executed.
    :param args: Positional arguments to pass to the function.
    :param kwargs: Keyword arguments to pass to the function.
    :return: The return value of the executed function.
    :raises ValueError: If the function is not found or not callable.
    :raises Exception: If an error occurs while trying to run the function.
    """
    try:
        # Get the function from the global namespace using its name
        func = globals().get(function_name)

        # Check if the function exists and is callable
        if callable(func):
            # Call the function with the provided arguments
            return func(*args, **kwargs)
        else:
            raise ValueError(f"Function {function_name} not found or not callable.")
    except Exception as e:
        print("Error while trying to run the dynamic function.")
        raise e


def read_csv_to_df(file_name: str) -> pd.DataFrame:
    """
    Reads a CSV file and returns its contents as a pandas DataFrame.

    :param file_name: The path to the CSV file to be read.
    :return: A pandas DataFrame containing the CSV data.
    :raises Exception: If an error occurs while reading the CSV file.
    """
    try:
        return pd.read_csv(file_name)
    except Exception as e:
        print(f"Error reading file {file_name}: {e}")
        raise e


def execute_functions(operational_df: pd.DataFrame) -> None:
    """
    Executes functions specified in the operational DataFrame based on the test configurations.

    :param operational_df: A DataFrame containing test configurations.
    :return: None
    """
    for index, row in operational_df.iterrows():
        if row.get("is_active") == 1:
            print(row.get("test"))
            # Read the corresponding data file
            bank_data = pd.read_csv("data/" + row.get("file_name"))
            # Get the function name to execute
            function_name = row.get("test")

            # Dynamically match the function name and execute the corresponding function
            match function_name:
                case 'count_records':
                    run_dynamic_function(function_name='count_records', df=bank_data, range_between=[1, 1048568])
                case 'validate_data_types':
                    run_dynamic_function(function_name='validate_data_types', df=bank_data, expected_types=bank_data.dtypes)
                case 'basic_tests':
                    run_dynamic_function(function_name='basic_tests', table_name="bank_transaction_1")


def main():
    """
    Main function to read the operational configuration and execute the specified functions.

    :return: None
    """
    operational_df = read_csv_to_df('data/operational_table.csv')
    execute_functions(operational_df)


if __name__ == '__main__':
    main()
