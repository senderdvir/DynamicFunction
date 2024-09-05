import pandas as pd

file_path = 'data/status_table.csv'
def write_to_status_table(data: dict):
    """
    Writes a new row into the status_table CSV file.

    :global var 'file_path': where the status_table.csv file is located
    :param data: A dictionary where keys are the column names, and values are the data to be written to the CSV.
    """
    # Convert the dictionary to a DataFrame
    df = pd.DataFrame([data])
    print( f"Writing data to status table: {data}" )
    try:
        # Try to append to the existing file without headers
        df.to_csv( file_path, mode='a', index=False, header=False )
    except FileNotFoundError:
        # If file does not exist, create it with headers
        print( f"File {file_path} not found. Creating new file." )
        df.to_csv( file_path, mode='w', index=False, header=True )
    except Exception as e:
        print( f"Error writing to file {file_path}: {e}" )
        raise  # Re-raise the exception after logging it
