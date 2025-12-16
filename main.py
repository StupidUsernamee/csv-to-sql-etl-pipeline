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
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        host=os.getenv("POSTGRES_HOST"),
        password=os.getenv("POSTGRES_PASS"),
        port=os.getenv("POSTGRES_PORT")
    )
except:
    print("connection to database failed. shutting down")
    sys.exit(1)

print("connection to database established")


def extract(file_path: str):
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row


def transform(record: dict):
    try:
        record["Unique ID"] = int(record["Unique ID"])
        record["Indicator ID"] = int(record["Indicator ID"])
        record["Geo Join ID"] = int(record["Geo Join ID"])

        record["Name"] = str(record["Name"])
        record["Measure"] = str(record["Measure"])
        record["Measure Info"] = str(record["Measure Info"])
        record["Geo Type Name"] = str(record["Geo Type Name"])
        record["Geo Place Name"] = str(record["Geo Place Name"])
        record["Time Period"] = str(record["Time Period"])

        record["Start_Date"] = (
            datetime.strptime(record["Start_Date"], '%m/%d/%Y')
            .date()
            .strftime("%Y-%m-%d")
        )

        record["Data Value"] = float(record["Data Value"])

        record["Message"] = record["Message"] or None

        record = list(record.values())

        return record, True
    except Exception as e:
        print(f"transform failed: {e}")
        return None, False


def load(batch: list):
    print(f"inserting batch of size {len(batch)}")

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
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        );
    """

    with conn.cursor() as cursor:
        cursor.executemany(insert_query, batch)

    conn.commit()
    print("batch committed")


def init_db():
    print("initializing database")
    with conn.cursor() as cursor:
        cursor.execute(open("init.sql", 'r').read())


if __name__ == "__main__":
    try:
        init_db()

        buffer = []
        total = 0

        for record in extract('data/Air_Quality.csv'):
            record, ok = transform(record)
            if not ok:
                continue

            buffer.append(record)

            if len(buffer) >= BATCH_SIZE:
                load(buffer)
                total += len(buffer)
                print(f"total rows inserted: {total}")
                buffer.clear()

        if buffer:
            load(buffer)
            total += len(buffer)
            print(f"total rows inserted: {total}")

    finally:
        if conn:
            conn.close()
            print("connection closed")
