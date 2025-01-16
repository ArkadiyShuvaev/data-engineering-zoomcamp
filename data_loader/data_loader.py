import argparse
import subprocess
import sys
from time import time
from urllib.parse import quote_plus

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main(params) -> int:
    password = params.db_password
    host_name = params.db_host_name
    db_name = params.db_name
    db_port = params.db_port
    table_name = params.db_table_name
    file_url = params.file_url
    user_name = params.db_user_name
    file_taxi_zones_url = params.file_taxi_zones_url
    taxi_zones_table_name = params.db_taxi_zones_table_name

    chunk_size = 100000
    downloaded_file_path = "/var/tmp/output.parquet"
    downloaded_taxi_zones_file_path = "/var/tmp/output.csv"

    subprocess.run(["wget", "-O", downloaded_file_path, file_url], check=True)
    subprocess.run(["wget", "-O", downloaded_taxi_zones_file_path, file_taxi_zones_url], check=True)

    encoded_password = quote_plus(password)
    engine = create_engine(
        f"postgresql://{user_name}:{encoded_password}@{host_name}:{db_port}/{db_name}")

    print("Loading taxi zones data into PostgreSQL")
    dtypes = {
        "Zone": "string",
        "LocationID": "Int64",
        "Borough": "string"
    }

    ny_taxi_zones_df = pd.read_csv(downloaded_taxi_zones_file_path, usecols=["Zone", "LocationID", "Borough"], dtype=dtypes)
    ny_taxi_zones_df.to_sql(taxi_zones_table_name, engine, if_exists="replace")

    print("Loading data into PostgreSQL")
    parquet_file = pq.ParquetFile(downloaded_file_path)

    is_first_batch = True
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        start = time()
        chunk_df = pa.Table.from_batches([batch]).to_pandas()

        if is_first_batch:
            chunk_df.to_sql(table_name, engine, index=False, if_exists="replace")
            is_first_batch = False
        else:
            chunk_df.to_sql(table_name, engine, index=False, if_exists="append")

        end = time()
        print(f"Chunk loaded in {end - start} seconds")

    row_count = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", engine)
    print(f"Number of rows in the table: {row_count.iloc[0, 0]}")

    return 0


if __name__ == '__main__':
    print("Starting Data Ingestion")

    parser = argparse.ArgumentParser("Ingest Data File into PostgreSQL")
    parser.add_argument("--file_url", help="URL of the data to be downloaded")
    parser.add_argument("--file_taxi_zones_url", help="The URL of the taxi zones data to be downloaded")
    parser.add_argument("--db_table_name", help="The name of the table to be created")
    parser.add_argument("--db_taxi_zones_table_name", help="The name of the table with taxi zones to be created")
    parser.add_argument("--db_name", help="The name of the database to be created")
    parser.add_argument("--db_host_name", help="The name of the host")
    parser.add_argument("--db_port", help="The port number of the database")
    parser.add_argument("--db_password", help="The password of the user")
    parser.add_argument("--db_user_name", help="The username to connect to the database")

    args = parser.parse_args()

    try:
        sys.exit(main(args))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
