import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):
    user = os.environ.get('USER')
    password = os.environ.get('PASSWORD')
    host = os.environ.get('HOST')
    port = os.environ.get('PORT')
    url = params.url
    db = os.environ.get('DB')
    table_name = params.table_name
    input_filename = params.input_filename
    output_csv = 'output.csv'

    # creating a connection to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # download the parquet file
    os.system(f'curl {url} -O {input_filename}')
    df = pd.read_parquet(f'{input_filename}')


    # convert parquet to csv
    df.to_csv(f'{output_csv}')

    # read the csv file
    df_csv = pd.read_csv(f'{output_csv}')

    # break the file into chunks for easier ingestion
    df_csv_iter = pd.read_csv(f'{output_csv}', iterator=True, chunksize=100000)

    # ingest the data
    for chunk in df_csv_iter:
        t_start = time()
        chunk.to_sql(name=f'{table_name}', con=engine, schema='public', if_exists='append')
        t_end = time()
        print('Inserted another chunk, took %3f second' % (t_end - t_start))



if __name__ == '__main__':
    # get the parameters from user in realtime
    parser = argparse.ArgumentParser(description = 'Ingest CSV data to Postgresql')

    parser.add_argument('--url', help='url of the csv file')
    parser.add_argument('--table_name', help='name of table where results will be saved to')
    parser.add_argument('--input_filename', help='name of the parquet file')

    args = parser.parse_args()

    main(args)

# command to run this python script
# python3 ingest_ny_taxi_data.py --url https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet --table_name yellow_taxi --input_filename yellow_tripdata_2022-01.parquet