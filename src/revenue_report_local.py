### 1. Set up connection to google cloud
### 2. Import packages

import click
import pandas as pd
from time import time
import os
from google.cloud import storage
import pyspark

@click.command() # click commands instead of argparse.ArgumentParser()... or sys.argv[n]
@click.option('--sa_path', help='Path to the service account json file')
@click.option('--project_id', help='Project ID of you GCP project')
@click.option('--year', default=2021, help='Year to download')
@click.option('--bucket', help='Name of the bucket to upload the data')
@click.option('--color', help='Str of the taxi-color for which data should be extracted')
@click.option('--month', help='Int of the month to summarize the data for')

### 3. E: Extract the data as a function
def extract_data(sa_path, bucket, color, year, month):
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
    
    # read the object in memory
    df_taxi = pd.read_parquet(pq_taxi)
    return df_taxi

### 4. T: Transform with Spark-SQL as a function 
    ### First just get one row
def transform_data(df_taxi):
    # some sql commands
    pass
    # return df_transformed

### 5. L: Load Data onto local Machine
def load_data(df_transformed):
    # some commands to write it into storage as some file
    pass

### Main with print to test the script running