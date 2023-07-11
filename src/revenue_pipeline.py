import click
import pandas as pd
from time import time
import os
import sys
from google.cloud import storage
import pyspark as ps
import psycopg2
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from dotenv import load_dotenv
from io import BytesIO
from sqlalchemy import create_engine

@click.command()
@click.option('--sa_path', help='Path to the service account json file')
@click.option('--year', default=2021, help='Year to download')
@click.option('--bucket', help='Name of the bucket to upload the data')
@click.option('--color', help='Str of the taxi-color for which data should be extracted')
@click.option('--month', help='Int of the month to summarize the data for')

def extract_data(sa_path, bucket, color, year, month):
    month = int(month)
    spark = SparkSession.builder \
        .master("local") \
        .appName(f"Pipe-{color}_taxi_{year}-{month:02d}") \
        .getOrCreate()
    file_name = f"ny_taxi/{color}_tripdata_{year}-0{month}.parquet"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(file_name)
    pq_taxi = blob.download_as_bytes()    
    pq_taxi = BytesIO(pq_taxi)
    df_taxi = pd.read_parquet(pq_taxi)
    df_taxi.drop("ehail_fee", inplace=True, axis=1)
    df_taxi = spark.createDataFrame(df_taxi)
    for col in df_taxi.columns:
        if df_taxi.schema[col].dataType == types.DoubleType():
            df_taxi = df_taxi.withColumn(col, F.col(col).cast('float'))
    return df_taxi, spark

def repartition_data():
    pass

def transform_data(df_taxi, spark):
    df_taxi = df_taxi \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')\
        .withColumnRenamed('PULocationID', 'pickup_location_id')\
        .withColumnRenamed('DOLocationID', 'dropoff_location_id')
    df_taxi.registerTempTable('df_taxi_temp')
    df_result = spark.sql("""
                    SELECT pickup_location_id AS revenue_zone,
                    date_trunc('day', pickup_datetime) AS revenue_day,
                    date_trunc('month', pickup_datetime) AS month,  
                    SUM(fare_amount) AS revenue_daily_fare,
                    SUM(extra) AS revenue_daily_extra,
                    SUM(mta_tax) AS revenue_daily_mta_tax,
                    SUM(tip_amount) AS revenue_daily_tip_amount,
                    SUM(tolls_amount) AS revenue_daily_tolls_amount,
                    SUM(improvement_surcharge) AS revenue_daily_improvement_surcharge,
                    SUM(total_amount) AS revenue_daily_total_amount,
                    SUM(congestion_surcharge) AS revenue_daily_congestion_surcharge,
                    AVG(passenger_count) AS avg_daily_passenger_count,
                    AVG(trip_distance) AS avg_daily_trip_distance
                    FROM df_taxi_temp
                    GROUP BY revenue_zone, revenue_day, month;
                      """)
    return df_result

def load_data(df_transformed, spark, color, year, month):
    load_dotenv()
    user = os.getenv('USER')
    pw = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    db = os.getenv('DB')
    schema = os.getenv('SCHEMA')
    engine = create_engine(f'postgresql://{user}:{pw}@{host}:{port}/{db}')
    table_name = f"{color}_revenue_{year}_{month}"
    df_transformed = df_transformed.withColumn("revenue_day", df_transformed.revenue_day.cast(types.StringType())) \
                                .withColumn("month", df_transformed.month.cast(types.StringType()))
    df_transformed = df_transformed.toPandas()
    df_transformed["revenue_day"] = pd.to_datetime(df_transformed["revenue_day"])
    df_transformed["month"] = pd.to_datetime(df_transformed["month"])
    df_transformed.to_sql(name=table_name, con=engine, schema=schema, 
                          if_exists="replace", index=False)

def main_pipeline(sa_path, bucket, color, year, month):    
    df_taxi, spark = extract_data(sa_path, bucket, color, year, month)
    df_transformed = transform_data(df_taxi, spark)
    load_data(df_transformed, spark, color, year, month)

if __name__ == '__main__':
    # extract_data()                                         # not nested works
    main_pipeline()                                          # main_pipeline() missing 5 required positional arguments
    # main_pipeline(sa_path, bucket, color, year, month)     # NameError: name 'sa_path' is not defined