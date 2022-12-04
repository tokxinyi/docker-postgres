from python:3.9.1

WORKDIR /app
COPY requirements.txt requirements.txt
COPY ingest_ny_taxi_data.py ingest_ny_taxi_data.py
COPY .env .env

RUN apt-get install curl
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3", "ingest_ny_taxi_data.py" ]

# docker run -it --network=docker-postgres-network --name taxi_ingest taxi_ingestion:v1 --url https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet --table_name yellow_taxi --input_filename yellow_tripdata_2022-01.parquet