import click
import pandas as pd
from time import time
import os
from google.cloud import storage

@click.command() # click commands instead of argparse.ArgumentParser()... or sys.argv[n]
@click.option('--sa_path', help='Path to the service account json file')
@click.option('--project_id', help='Project ID of you GCP project')
@click.option('--year', default=2021, help='Year to download')
@click.option('--bucket', help='Name of the bucket to upload the data')
@click.option('--color', help='Str of the taxi-color for which data should be extracted')

def data_ingestion(sa_path, project_id, year, bucket, color):
    # sa_path is the local path to the .json with the key-credentials of the Storage-Account 
    # (who is loggin in with which role)
    # project_id relates to the identifier of the google-cloud project
    # year which year to download
    # bucket: Where to store - the name of the GCS-bucket
    
    for month in range(1, 4):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"
        
        # string of the file to be stored
        file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"

        print("Loading data from url...")
        df_taxi = pd.read_parquet(url)
        print('Uploading data to GCS...')
        # GC storage account key
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        # Create an instance of the GCS client to communicate with the Cloud
        client = storage.Client()
        # Retrieve address/path to the specified bucket, where data should be stored
        bucket = client.get_bucket(bucket)
        
        # Create empty blob file, before upload_... is used to transfer the parquet as string to the cloud into the blob
        bucket.blob(f'ny_taxi/{file_name}').\
            upload_from_string(df_taxi.to_parquet(), 'text/parquet')
        print("Successfully uploaded the data!")

### All function parameters are passed via the command line
if __name__ == '__main__':
    data_ingestion()