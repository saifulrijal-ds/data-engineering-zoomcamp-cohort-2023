import os
import argparse

import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    df_iter  = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    while True:

        try:
            t_start = time()

            chunk_df = next(df_iter)

            chunk_df["tpep_pickup_datetime"] = pd.to_datetime(chunk_df["tpep_pickup_datetime"])
            chunk_df["tpep_dropoff_datetime"] = pd.to_datetime(chunk_df["tpep_dropoff_datetime"])

            chunk_df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()

            print(f"inserted another chunk, took {(t_end-t_start):.3f} seconds")

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


# user = "root"
# password = "root"
# server = "localhost"
# port = 5432
# database = "ny_taxi"
# url_csv = "yellow_tripdata_2021-01.csv.gz"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres connection')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)