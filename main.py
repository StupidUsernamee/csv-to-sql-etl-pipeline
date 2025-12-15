# FIXME


import os
import sys
import psycopg2
import csv
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Number of records to insert per batch
BATCH_SIZE = 1000

# Database connection object
conn = None

# Establish database connection
try:
    conn = psycopg2.connect(
        database="postgres",
        user=os.getenv("POSTGRES_USER"),
        host=os.getenv("POSTGRES_HOST"),
        password=os.getenv("POSTGRES_PASS"),
        port=os.getenv("POSTGRES_PORT")
    )
except:
    # Fail fast if database connection cannot be established
    print("connection to database failed.\nshuting down")
    sys.exit(1)

print("connection to database stablished.")


def extract(file_path: str):
    """
    Reads a CSV file and yields each row as a dictionary.

    Args:
        file_path (str): Path to the CSV file

    Yields:
        dict: A single CSV row mapped by column names
    """
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row


def transform(record: dict):
    """
    Transforms raw CSV record into typed values.

    Assumes data is clean and does not perform validation.

    Args:
        record (dict): Raw CSV record

    Returns:
        tuple: (transformed_record, is_valid)
    """

    # Convert identifier fields to integers
    record["Unique ID"] = int(record["Unique ID"])
    record["Indicator ID"] = int(record["Indicator ID"])
    record["Geo Join ID"] = int(record["Geo Join ID"])

    # Ensure text fields are strings
    record["Name"] = str(record["Name"])
    record["Measure"] = str(record["Measure"])
    record["Measure Info"] = str(record["Measure Info"])
    record["Geo Type Name"] = str(record["Geo Type Name"])
    record["Geo Place Name"] = str(record["Geo Place Name"])
    record["Time Period"] = str(record["Time Period"])

    # Parse date and convert to PostgreSQL-compatible format
    record["Start_Date"] = (
        datetime.strptime(record["Start_Date"], '%m/%d/%Y')
        .date()
        .strftime("%Y-%m-%d")
    )

    # Convert numeric measurement
    record["Data Value"] = float(record["Data Value"])

    # Normalize empty message field to NULL
    record["Message"] = str(record["Message"])
    if record["Message"] == "":
        record["Message"] = None

    
    record = list(record.values())

    return (record, True)


def load(batch: list):
    """
    Inserts a batch of records into the air_quality table.

    Args:
        batch (list): List of transformed records
    """
    insert_query = """
        INSERT INTO air_quality (
            unique_id,
            indicator_id,
            name,
            measure,
            measure_info,
            geo_type_name,
            geo_join_id,
            geo_place_name,
            time_period,
            start_date,
            data_value,
            message
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        );
    """

    # Execute batch insert
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, batch)


def init_db():
    """
    Initializes database schema using SQL file.
    """
    with conn.cursor() as cursor:
        cursor.execute(open("init.sql", 'r').read())


if __name__ == "__main__":
    try:
        # Initialize database schema
        init_db()

        buffer = []

        # Extract, transform, and batch-load records
        for record in extract('data/Air_Quality.csv'):
            record, _ = transform(record)
            buffer.append(record)

            if len(buffer) >= BATCH_SIZE:
                load(buffer)
                buffer.clear()

        # Load remaining records
        if len(buffer) > 0:
            load(buffer)

        buffer.clear()

    finally:
        # Ensure database connection is closed
        if conn:
            conn.close()
