#!/usr/bin/env python
# coding: utf-8
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
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = "yellow_taxi_data_2020_05.csv.gz"

    os.system(f"wget {url} -O {csv_name}")

    # create engine for connecting
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    # create a iterator for data chunk reading
    df_iter = pd.read_csv(csv_name, compression="gzip", iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # create a table with column names
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    # ingest data with iterators
    while True:
        t_start = time()
        
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists="append")
        
        t_end = time()
        
        print("inserted another chunl..., took %.3f second" % (t_end - t_start))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest csv.gz data to Postgres")

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="name of the table where we will write the results to")
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()
   
    main(args)

