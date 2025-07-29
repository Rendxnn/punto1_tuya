import pandas as pd

class DataSource:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_data(self, chunk_size: int = 10000):
        """Reads data from the CSV file in chunks."""
        try:
            for chunk in pd.read_csv(self.file_path, chunksize=chunk_size):
                yield chunk
        except FileNotFoundError:
            print(f"Error: The file {self.file_path} was not found.")
            yield pd.DataFrame() # Return an empty DataFrame to avoid further errors
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")
            yield pd.DataFrame() # Return an empty DataFrame to avoid further errors
