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
    :return: Tuple[boolean, string] = result and logs
    :raises ValueError: If the function is not found or not callable.
    :raises Exception: If an error occurs while trying to run the function.
    """

    try:
        # Get the function from the global namespace using its name
        func = globals().get(function_name)
        print(f"Attempting to run function: {function_name}")  # Debugging line

        if func is None:
            raise ValueError( f"Function '{function_name}' not found." )
        if not callable( func ):
            raise ValueError( f"'{function_name}' is not callable" )

        # Redirect standard output to capture it
        captured_output = io.StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            # Call the function with the provided arguments
            result = func( *args, **kwargs )

                # Check if the function returns a tuple with two elements
            if isinstance( result, tuple ) and len( result ) == 2:
                result, func_logs = result
            else:
                result, func_logs = False, "Function did not return (boolean, logs)."

            # Combine captured output with returned logs
            captured_logs = captured_output.getvalue()
            logs = f"{func_logs}\n{captured_logs}".strip() if captured_output else func_logs
        finally:
            sys.stdout = original_stdout  # Restore stdout

        return result, logs

    except Exception as e:
        print( "Error while trying to run the dynamic function." )
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
            data_file_path = "data/" + row.get( "file_name" )
            print( f"Loading data from {data_file_path}" )

            try:
                file_data = pd.read_parquet( data_file_path )
            except Exception as e:
                print( f"Error loading file {data_file_path}: {e}" )
                continue  # Skip this iteration if there's an error loading the data

            print( f"Data preview from {data_file_path}:" )
            print( file_data.head() )
            print( f"Columns in loaded data: {file_data.columns}" )


            # Get the function name to execute
            function_name = row.get("test")
            TCID = row.get("TCID")

            # Initialize variables for success and logs
            success, logs = False, ""

            # Dynamically match the function name and execute the corresponding function
            match function_name:
                case 'count_records':
                    print( "Executing count_records with parameters:" )
                    success, logs = run_dynamic_function( 'count_records', file_data, range_between=[0, 1048500] )

                case 'validate_sum':
                    column = row.get( 'tested_field' )
                    expected_sum = row.get( 'expected_value' )
                    success, logs = run_dynamic_function( 'validate_sum', file_data, column=column,
                                                          expected_sum=expected_sum )

                case 'validate_numeric_range':
                    success, logs = run_dynamic_function('validate_numeric_range', df=file_data,
                                                         column_name='CustAccountBalance', min_value=0, max_value=8000)
                case 'check_nulls':
                    success, logs = run_dynamic_function( 'check_nulls', df=file_data, column='file_date' )

                case 'file_timestamp':
                    success, logs = run_dynamic_function( 'file_timestamp', file_data,
                                                          timestamp_column='TransactionTime' )

                case _:
                    print( f"Unknown function {function_name}" )
                    continue

            print( f"Result of function {function_name}: Success: {success}, Logs: {logs}" )  # debugging line
            # Explicitly convert the success boolean to an integer (1 for True, 0 for False)
            status = int(success)  # Convert to integer here
            data = {'TCID': TCID, 'status': status, 'logs': logs}
            print( f"Writing to status table: {data}" )
            write_to_status_table( data )



    # Save the updated operational DataFrame back to the CSV file
    operational_df.to_csv('data/updated_operational_table.csv', index=False)


def main():
    """
    Main function to read the operational configuration and execute the specified functions.
    """
    status_table_path = 'data/status_table.csv'
    operational_table_path = 'data/updated_operational_table.csv'

    initialize_status_table( status_table_path )

    try:
        operational_df = read_csv_to_df( operational_table_path )
        print( f"Operational table loaded successfully:\n{operational_df.head()}" )  # Debugging line
    except Exception as e:
        print( f"Failed to load operational table: {e}" )
        return  # Exit if loading fails

        # Step 3: Execute functions based on the configuration
    execute_functions( operational_df )

    print( "DQA Program completed. Status table and logs updated." )


if __name__ == '__main__':
    main()