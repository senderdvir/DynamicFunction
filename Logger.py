import pandas as pd


def write_to_status_table(file_path: str, data: dict):
    """
    Writes a new row into the status_table CSV file.

    :param file_path: where the status_table.csv file is located
    :param data: A dictionary where keys are the column names, and values are the data to be written to the CSV.
    """
    # Convert the dictionary to a DataFrame
    df = pd.DataFrame([data])

    try:
        # Try to append to the existing file
        df.to_csv(file_path, mode='a', index=False, header=False)
    except FileNotFoundError:
        # If file does not exist, create it with headers
        df.to_csv(file_path, mode='w', index=False, header=True)
