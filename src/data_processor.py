import pandas as pd
import re

class DataProcessor:
    def __init__(self):
        pass

    def clean_phone_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans and normalizes phone numbers."""
        # Fill NaN values with empty string to avoid errors in string operations
        df['phone_number'] = df['phone_number'].fillna('').astype(str)

        # Remove non-numeric characters
        df['phone_number'] = df['phone_number'].apply(lambda x: re.sub(r'[^0-9]', '', x))

        # Optional: Add a country code if missing and inferrable (example for Spain)
        # This is a simplification; real-world scenarios need more robust logic
        df['phone_number'] = df['phone_number'].apply(lambda x: '34' + x if len(x) == 9 and x.startswith(('6', '7')) else x)

        return df

    def validate_phone_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validates phone numbers based on a basic length check (e.g., 9-15 digits)."""
        # Assuming valid phone numbers are between 9 and 15 digits after cleaning
        df['is_valid_phone'] = df['phone_number'].apply(lambda x: 9 <= len(x) <= 15)
        return df

    def deduplicate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicates data based on phone_number, keeping the first occurrence."""
        # Drop duplicates based on the cleaned phone number
        df_deduplicated = df.drop_duplicates(subset=['phone_number'], keep='first')
        return df_deduplicated

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies cleaning, validation, and deduplication in sequence."""
        df = self.clean_phone_numbers(df)
        df = self.validate_phone_numbers(df)
        df = self.deduplicate_data(df)
        return df
