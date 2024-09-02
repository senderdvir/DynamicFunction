import io  # allows the reading and writing to files
import sys  # access to variables and functions that interact with the Python interpreter.

from utils.Logger import write_to_status_table
from utils.OperationalTestFunctions import *

# Initialize the status table CSV file
def initialize_status_table(file_path: str):
    status_df = pd.DataFrame(columns=['TCID','status', 'logs'])
    status_df.to_csv(file_path, index=False)


def run_dynamic_function(function_name: str, *args, **kwargs):
    """
    Executes a function dynamically based on the function name provided and captures its print output.

    :param function_name: The name of the function to be executed.
    :param args: Positional arguments to pass to the function.
    :param kwargs: Keyword arguments to pass to the function.
    :return: boolean =result, and string =logs
    :raises ValueError: If the function is not found or not callable.
    :raises Exception: If an error occurs while trying to run the function.
    """

    try:
        # Get the function from the global namespace using its name
        func = globals().get(function_name)
        print(f"Attempting to run function: {function_name}")  # Debugging line

        # Check if the function exists and is callable
        if callable(func):
            captured_output = io.StringIO()  # Creates an in-memory file
            sys.stdout = captured_output  # Redirect standard output to capture it

            try:
                # Call the function with the provided arguments
                result = func(*args, **kwargs)

                # Check if the function returns a tuple with two elements
                if isinstance(result, tuple) and len(result) == 2:
                    result, logs = result
                else:
                    result, logs = False, "Function did not return (boolean, logs)."
            finally:
                sys.stdout = sys.__stdout__  # restore sys.stdout to its default value

            # Get logs from captured output
            logs = captured_output.getvalue()  # getvalue() method of the StringIO returns all the text that has been "written" to this in-memory file so far as a single string.

            return result, logs
        else:
            raise ValueError(f'Function {function_name} not found or not callable.')
    except Exception as e:
        sys.stdout = sys.__stdout__  ## restore sys.stdout to its default value
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
            print(f"Processing TCID: {row.get('TCID')} with function {row.get('test')}")  # Debugging line
            # Read the corresponding data file
            # data = pd.read_csv("data/" + row.get("file_name"))
            file_data = pd.read_parquet("data/" + row.get("file_name"))
            # Get the function name to execute
            function_name = row.get("test")
            TCID = row.get("TCID")

            # Dynamically match the function name and execute the corresponding function
            match function_name:
                case 'count_records':
                    success, logs = run_dynamic_function('count_records', df=file_data, range_between=[0, 1048500])
                case 'validate_sum':
                    success, logs = run_dynamic_function('validate_sum', df=file_data, column='LoanAmount',
                                                         expected_sum=180000)
                case 'validate_numeric_range':
                    success, logs = run_dynamic_function('validate_numeric_range', df=file_data,
                                                         column_name='CustAccountBalance', min_value=0, max_value=8000)
                case 'check_nulls':
                    success, logs = run_dynamic_function('check_nulls', df=file_data, column='Gender')
                case 'validate_date_range':
                    success, logs = run_dynamic_function('validate_date_range', df=file_data,
                                                         date_column='CustomerDOB', start_date='1/1/1700',
                                                         end_date='1/1/1850')
                case 'file_timestamp':
                    success, logs = run_dynamic_function('file_timestamp', df=file_data,
                                                         timestamp_column='TransactionTime')
                case _:
                    success, logs = False, f"Function {function_name} not recognized"
            # Explicitly convert the success boolean to an integer (1 for True, 0 for False)
            status = int(success)  # Convert to integer here
            operational_df.at[index, 'status'] = status



            # Create a dictionary to write to the status table
            data = {
                'TCID': TCID,
                'status': status,
                'logs': logs
            }
            # TODO: write the data into the csv log file
            print(f"Writing to status table: {data}")
            write_to_status_table(data)
    # Save the updated operational DataFrame back to the CSV file
    operational_df.to_csv('data/updated_operational_table.csv', index=False)


def main():
    """
    Main function to read the operational configuration and execute the specified functions.

    :return: None
    """
    initialize_status_table('data/status_table.csv')

    operational_df = read_csv_to_df('data/updated_operational_table.csv')
    print(operational_df.head())  # Debugging line to check data
    execute_functions(operational_df)
    # df = pd.read_csv('data/bank_transactions.csv')
    # df.to_parquet('data/bank_transactions.parquet', index=False)


if __name__ == '__main__':
    main()
