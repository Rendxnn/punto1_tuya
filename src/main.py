import argparse
import os
from .data_source import DataSource
from .data_processor import DataProcessor
from .data_sink import DataSink

def main():
    parser = argparse.ArgumentParser(description="Process customer phone numbers from a CSV file.")
    parser.add_argument('--file_path', type=str, required=True, help='Absolute path to the input CSV file.')
    args = parser.parse_args()

    # Database configuration (replace with your PostgreSQL details)
    db_config = {
        'host': 'localhost',
        'database': 'phone_numbers_db',
        'user': 'postgres',
        'password': 'dragonball2004'
    }
    table_name = "customer_phone_numbers"

    # Initialize components
    data_source = DataSource(args.file_path)
    data_processor = DataProcessor()
    data_sink = DataSink(db_config)

    # Ensure table exists
    data_sink.create_table(table_name)

    print(f"Starting data processing for {args.file_path}...")
    processed_records_count = 0
    for chunk in data_source.read_data():
        if not chunk.empty:
            processed_chunk = data_processor.process(chunk)
            data_sink.insert_data(processed_chunk, table_name)
            processed_records_count += len(processed_chunk)
            print(f"Processed and inserted {len(processed_chunk)} records in this chunk. Total processed: {processed_records_count}")

    print(f"Data processing complete. Total unique valid records inserted: {processed_records_count}")

if __name__ == "__main__":
    main()
