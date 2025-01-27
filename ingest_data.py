import os
import argparse
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    data_table_name = params.data_table_name
    lookup_table_name = params.lookup_table_name
    data_url = params.data_url
    lookup_url = params.lookup_url

    parquet_name = 'output.parquet'
    csv_name = 'lookup.csv'

    # download the Parquet file
    os.system(f"wget {data_url} -O {parquet_name}")
    os.system(f"wget {lookup_url} -O {csv_name}")

    # Create database connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Open the Parquet file
    parquet_file = pq.ParquetFile(parquet_name)

    # Read the lookup file
    lookup_file = pd.read_csv(csv_name)

    # Create lookup table with correct schema
    lookup_file.head(n=0).to_sql(name=lookup_table_name, con=engine, if_exists='replace', index=False)

    # Insert lookup data
    lookup_file.to_sql(name=lookup_table_name, con=engine, if_exists='append', index=False)

    # Create a batch iterator
    batch_iter = parquet_file.iter_batches(batch_size=100000)

    # Get first batch
    df = next(batch_iter).to_pandas()

    # Create data table with correct schema
    df.head(n=0).to_sql(name=data_table_name, con=engine, if_exists='replace', index=False)

    # Insert first chunk
    df.to_sql(name=data_table_name, con=engine, if_exists='append', index=False)

    # Process remaining chunks
    while True:
        try:
            t_start = time()

            df = next(batch_iter).to_pandas()

            df.to_sql(name = data_table_name, con=engine, if_exists='append', index=False)

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print('completed')
            break


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--data_table_name', help='name of the table where we will write the results to')
    parser.add_argument('--lookup_table_name', help='name of the lookup table')
    parser.add_argument('--data_url', help='url of the data file')
    parser.add_argument('--lookup_url', help='url of the lookup file')

    args = parser.parse_args()

    main(args)