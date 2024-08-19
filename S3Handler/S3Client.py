from minio import Minio
import pandas as pd
import io

MINIO_ENDPOINT = "localhost:9001"
MINIO_ACCESS_KEY = "F0h3VEPPeiqTqllx"
MINIO_SECRET_KEY = "CrZwjDXeVcsI4vCKTerI4DktWx89t9X4"


class S3Client:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

        """
        Initialize the S3Client with the MinIO server configuration.
        """

        # Initialize MinIO connection details
        self.minio_endpoint = MINIO_ENDPOINT
        self.minio_access_key = MINIO_ACCESS_KEY
        self.minio_secret_key = MINIO_SECRET_KEY

        # Create a MinIO client
        self.client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False  # Set to True if using HTTPS
        )

    def upload_file(self, file_path: str, key: str):
        pass

    def download_file(self, key: str, file_path: str):
        pass

    def delete_file(self, key: str):
        pass

    def list_files(self):
        pass

    def get_csv_file_into_df(self, key: str) -> pd.DataFrame:
        data = self.client.get_object('bank-data', key)
        df = pd.read_csv(io.BytesIO(data.read()))
        dict_data = {
            "count": df['TransactionAmount (INR)'].count(),
            "sum": round(df['TransactionAmount (INR)'].sum())
        }

        res_data_df = pd.DataFrame(dict_data, index=[0])
        return res_data_df

    def get_txt_file_into_df(self, key: str) -> pd.DataFrame:
        # Fetch the TXT file from the S3 bucket
        file_data = self.client.get_object('bank-data', key)

        # Read the file data into a byte array
        byte_data = file_data.read()

        # Decode the byte data to a string
        decoded_string = byte_data.decode('utf-8')

        # Validate TXT file format (key-value pairs separated by ':')
        data_lines = decoded_string.split('\n')
        if not all(':' in line for line in data_lines):
            raise ValueError("TXT file must contain key-value pairs separated by ':' on each line.")

            # Convert the data lines to a dictionary
        data_dict = dict(line.split(':') for line in data_lines if line)

        # Ensure 'sum' and 'count' are in the data
        if 'sum' not in data_dict or 'count' not in data_dict:
            raise ValueError("TXT file must contain 'sum' and 'count' key-value pairs.")

            # Convert the dictionary to a DataFrame and cast types
        txt_file_data_as_df = pd.DataFrame([data_dict])
        txt_file_data_as_df['sum'] = txt_file_data_as_df['sum'].astype(int)
        txt_file_data_as_df['count'] = txt_file_data_as_df['count'].astype(int)

        # Return the DataFrame
        return txt_file_data_as_df
