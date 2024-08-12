import pandas as pd
from utils.OperationalTestFunctions import *


def run_dynamic_function(function_name, *args, **kwargs):
    # Get the function from the global namespace
    func = globals().get(function_name)

    # Check if the function exists and is callable
    if callable(func):
        # Call the function with the DataFrame and additional arguments
        return func(*args, **kwargs)
    else:
        raise ValueError(f"Function {function_name} not found or not callable.")


def main():
    # Read your data
    operational_df = pd.read_csv('operational_table.csv')
    for index, row in operational_df.iterrows():
        if row.get("is_active") == 1:
            bank_data = pd.read_csv(row.get("file_name"))

        # List of functions
            function_name = row.get("test")

            # for function_name in list_of_functions:
            match function_name:
                case 'count_records':
                    run_dynamic_function(df=bank_data, function_name='count_records', range_between=[1, 1048568])
                case 'say_hello':
                    run_dynamic_function('say_hello', name='adi')


if __name__ == '__main__':
    main()
