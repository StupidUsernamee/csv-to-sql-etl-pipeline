import psycopg2
import csv


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


def transform(record):
    pass


def load(batch: list):
    pass


if __name__ == "__main__":
    for record in extract('data/Air_Quality.csv'):
        print(record)