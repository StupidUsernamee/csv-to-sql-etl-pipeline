import psycopg2
import csv
from datetime import datetime

BATCH_SIZE = 1000

def extract(file_path: str):
    """yields data from csv file

    Args:
        file_path (str): _description_

    Yields: records from csv file
    """
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row


def transform(record: dict):
    """transforms the incoming records. data types

    Args:
        record (_type_): dict
    
    Returns: (type_fixed_record, is_valid) 
    """

    record["Unique ID"] = int(record["Unique ID"])
    record["Indicator ID"] = int(record["Indicator ID"])
    record["Name"] = str(record["Name"])
    record["Measure"] = str(record["Measure"])
    record["Measure Info"] = str(record["Measure Info"])
    record["Geo Type Name"] = str(record["Geo Type Name"])
    record["Geo Join ID"] = int(record["Geo Join ID"])
    record["Geo Place Name"] = str(record["Geo Place Name"])
    record["Time Period"] = str(record["Time Period"])
    record["Start_Date"] = datetime.strptime(record["Start_Date"], '%m/%d/%Y').date()
    record["Data Value"] = float(record["Data Value"])
    record["Message"] = str(record["Message"])
    if record["Message"] == "":
        record["Message"] = None

    return (record, True)



def load(batch: list):
    pass


if __name__ == "__main__":
    buffer = []
    for record in extract('data/Air_Quality.csv'):
        record = transform(record)
        buffer.append(record)
        if len(buffer) >= BATCH_SIZE:
            load(buffer)
            buffer.clear()

    if len(buffer) > 0:
        load(buffer)

    buffer.clear() 