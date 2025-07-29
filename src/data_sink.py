import psycopg2
import pandas as pd

class DataSink:
    def __init__(self, db_config: dict):
        self.db_config = db_config

    def _connect(self):
        """Establishes a connection to the PostgreSQL database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return None

    def create_table(self, table_name: str):
        """Creates the specified table if it doesn't exist."""
        conn = self._connect()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        customer_id BIGINT,
                        customer_name VARCHAR(255),
                        email VARCHAR(255),
                        phone_number VARCHAR(20),
                        is_valid_phone BOOLEAN
                    );
                """)
                conn.commit()
                print(f"Table {table_name} ensured to exist.")
            except Exception as e:
                print(f"Error creating table {table_name}: {e}")
            finally:
                cur.close()
                conn.close()

    def insert_data(self, df: pd.DataFrame, table_name: str):
        """Inserts DataFrame records into the specified table."""
        conn = self._connect()
        if conn:
            try:
                cur = conn.cursor()
                # Convert DataFrame to list of tuples for executemany
                # Ensure the order of columns matches the table schema
                data_to_insert = [
                    (row['customer_id'], row['customer_name'], row['email'], row['phone_number'], row['is_valid_phone'])
                    for index, row in df.iterrows()
                ]
                # Use psycopg2.extras.execute_values for efficient bulk insertion
                from psycopg2.extras import execute_values
                execute_values(
                    cur, 
                    f"INSERT INTO {table_name} (customer_id, customer_name, email, phone_number, is_valid_phone) VALUES %s",
                    data_to_insert
                )
                conn.commit()
                print(f"Inserted {len(df)} records into {table_name}.")
            except Exception as e:
                print(f"Error inserting data into {table_name}: {e}")
                conn.rollback() # Rollback in case of error
            finally:
                cur.close()
                conn.close()
