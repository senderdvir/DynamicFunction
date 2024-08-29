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
    print(f'{data}')
    try:
        # Try to append to the existing file
        df.to_csv(file_path, mode='a', index=False, header=False)
    except FileNotFoundError:
        # If file does not exist, create it with headers
        df.to_csv(file_path, mode='w', index=False, header=True)
