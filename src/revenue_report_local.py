import click
import pandas as pd
from time import time
import os
from google.cloud import storage
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from dotenv import load_dotenv
from io import BytesIO

@click.command() # click commands instead of argparse.ArgumentParser()... or sys.argv[n]
@click.option('--sa_path', help='Path to the service account json file')
@click.option('--project_id', help='Project ID of you GCP project')
@click.option('--year', default=2021, help='Year to download')
@click.option('--bucket', help='Name of the bucket to upload the data')
@click.option('--color', help='Str of the taxi-color for which data should be extracted')
@click.option('--month', help='Int of the month to summarize the data for')

def extract_data(sa_path, bucket, color, year, month):
    # Create Spark Session
    spark = SparkSession.builder.config("spark.jars", "../postgresql-42.6.0.jar") \
    .master("local") \
    .appName(f"Pipe-{color}_taxi_{year}-{month:02d}") \
    .getOrCreate()
    # string of the file to be loaded
    file_name = f"ny_taxi/{color}_tripdata_{year}-{month:02d}.parquet"
    
    # Establish connection to GCS-Bucket
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
    # Create an instance of the GCS client to communicate with the Cloud
    client = storage.Client()
    
    # Retrieve address/path to the specified bucket and a blob representing the table
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(file_name)

    # Download parquet and write it to memory as binary to be accessible
    pq_taxi = blob.download_as_bytes()    
    pq_taxi = BytesIO(pq_taxi)
    
    # read the object in memory as df -> spark-df
    df_taxi = pd.read_parquet(pq_taxi)
    df_taxi.drop("ehail_fee", inplace=True, axis=1)
    df_taxi = spark.createDataFrame(df_taxi)

    for col in df_taxi.columns:
        if df_taxi.schema[col].dataType == types.DoubleType():
            df_taxi = df_taxi.withColumn(col, F.col(col).cast('float'))

    return df_taxi, spark

### 4. Repartition the data
def repartition_data():
    pass

### Transform Data to revenue report
def transform_data(df_taxi, spark):
    # some sql commands
    df_taxi = df_taxi \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')\
        .withColumnRenamed('PULocationID', 'pickup_location_id')\
        .withColumnRenamed('DOLocationID', 'dropoff_location_id')
    # Temporary SQL Table to be queried
    df_taxi.registerTempTable('df_taxi_temp')
    # Query the revenue
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

### 5. L: Load Data onto local Machine PostgreSQL
### 5. L: Load Data onto local Machine PostgreSQL
def load_data(df_transformed, spark, color, year, month):
    load_dotenv()
    # get the Database Credentials
    user = os.getenv('USER')
    pw = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    db = os.getenv('DB')
    schema = os.getenv('SCHEMA')
    # some commands to write it into storage in the db
    table_name = f"{color}_revenue_{year}_{month}"
    # Write DataFrame to PostgreSQL
    df_transformed.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/ny_taxi") \
        .option("schema", schema) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", pw) \
        .save()

