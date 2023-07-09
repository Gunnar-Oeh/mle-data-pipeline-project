import click
import pandas as pd
from time import time
import os
from google.cloud import storage

@click.command()
@click.option('--sa_path', help='Path to the service account json file')
@click.option('--project_id', help='Project ID of your GCP project')
@click.option('--year', default=2021, help='Year to download')
@click.option('--bucket', help='Name of the bucket to download the data from')
@click.option('--color', help='Str of the taxi-color for which data should be extracted')
@click.option('--output_dir', help='Directory to save the downloaded data')

def data_retrieval(sa_path, project_id, year, bucket, color, output_dir):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
    client = storage.Client()
    bucket = client.get_bucket(bucket)

    for month in range(1, 4):
        file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"
        blob = bucket.blob(f'ny_taxi/{file_name}')
        blob.download_to_filename(os.path.join(output_dir, file_name))
        print(f"Downloaded {file_name} from GCS")

if __name__ == '__main__':
    data_retrieval()