-- Execute this query on BigQuery in the GCP console

-- Create External Tabel for FHV Tripdata Dataset
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_dtc-de-course-375301/data/fhv/fhv_tripdata_2019-*.parquet']
);

-- Create FHV tripdata from external table
CREATE OR REPLACE TABLE `trips_data_all.fhv_tripdata` (
  dispatching_base_num STRING,
  pickup_datetime	DATETIME,				
  dropOff_datetime	DATETIME,				
  PUlocationID STRING,				
  DOlocationID STRING,				
  SR_Flag	STRING,				
  Affiliated_base_number STRING
) AS (
    -- NOTES: Missing values in STRING fields denoted by empty string ''.
    -- NOTES: Check on web_to_gcs.py for how to handle missing values in STRING fields
    SELECT
        dispatching_base_num,
        CAST(pickup_datetime AS DATETIME),
        CAST(dropOff_datetime AS DATETIME),
        NULLIF(PUlocationID, ''),
        NULLIF(DOlocationID, ''),
        NULLIF(SR_Flag, ''),
        NULLIF(Affiliated_base_number, ''),
    FROM
        `trips_data_all.external_fhv_tripdata`
);
