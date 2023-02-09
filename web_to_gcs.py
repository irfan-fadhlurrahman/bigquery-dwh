import os
import urllib
import pandas as pd
import numpy as np

from datetime import timedelta
from pathlib import Path

from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash

# Currently this dict only suitable for FHV tripdata
DATETIME_COLUMNS = [
    'pickup_datetime', 'dropOff_datetime',
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 
    'lpep_pickup_datetime', 'lpep_dropoff_datetime',
]
STRING_COLUMNS = [
    'dispatching_base_num', 'Affiliated_base_number',
    'PUlocationID', 'DOlocationID',
    'SR_Flag',
]

@task(log_prints=True)
def extract(dataset_url: str) -> pd.DataFrame:
    """Read .csv file data from GitHub Repository into Pandas DataFrame"""
    print(f"Read a CSV file from {dataset_url}")
    
    return pd.read_csv(dataset_url)

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues and check data quality of each column"""
    print("Cleaning the pandas DataFrame")
    
    for col in DATETIME_COLUMNS:
        if col in df.columns.tolist():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    for col in STRING_COLUMNS:
        if col in df.columns.tolist():
            df[col] = df[col].apply(numeric_to_string)
                
    print("Summary of cleaned pandas DataFrame")
    print(f"Size: {df.shape}")
    print(df.info())
    
    return df
    
def numeric_to_string(row):
    """Check each row dtype then convert to string if row is not null value"""
    if isinstance(row, float) and row != row:
        return ''
    else:
        return str(int(row))
    
@task(log_prints=True)
def transform(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write the pandas DataFrame into parquet file"""
    print("Converting pandas DataFrame to parquet file")
    
    # Define the destination path
    path = Path(f"data/{color}/{dataset_file}.parquet")
    if not os.path.exists(path):
        os.system(f'mkdir -p data/{color}')
    
    # Write it to local storage   
    df.to_parquet(path, compression="gzip")
    
    return path

@task(log_prints=True)
def load(path: Path) -> None:
    """Upload a parquet file to Google Cloud Storage"""
    print("Loading a parquet file to GCS")
    
    gcs_block = GcsBucket.load('gcp-bucket')
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    print("Finished")
    
@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function to extract data from GitHub to GCS"""
    # Dataset
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_repo = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    dataset_url = f"{dataset_repo}/{color}/{dataset_file}.csv.gz"
    
    # Task starts here
    df = extract(dataset_url)
    df = clean(df)
    
    # Task to load dataset into GCS bucket
    path = transform(df, color, dataset_file)
    load(path)
    
@flow()
def etl_parent_flow(color: str, year: int, month_list: list) -> None:
    """Run each etl_web_to_gcs flow for specific dataset"""
    for month in month_list:
        try:
            etl_web_to_gcs(year, month, color)
        except urllib.error.HTTPError:
            continue
        
if __name__ == "__main__":
    color = "fhv"
    month_list = [*range(1, 12+1)]
    year = 2019
    etl_parent_flow(color, year, month_list)
    
    