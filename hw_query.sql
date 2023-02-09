-- Question 2
SELECT
  COUNT(DISTINCT Affiliated_base_number)
FROM
  `trips_data_all.fhv_tripdata`;

-- Question 3
WITH null_location AS (
  SELECT
  -- COUNT(PUlocationID) + COUNT(DOlocationID) AS total_both_missing
  PUlocationID AS PUnull,
  DOlocationID AS DOnull
  FROM
    `trips_data_all.fhv_tripdata`
  WHERE
    PUlocationID IS NULL 
    AND DOlocationID IS NULL
)
SELECT COUNT(*) FROM null_location

-- Question 4
CREATE OR REPLACE TABLE `trips_data_all.fhv_tripdata_partitioned_clustered`
PARTITION BY 
  DATE(pickup_datetime)
CLUSTER BY 
  Affiliated_base_number AS
  SELECT 
    *
  FROM 
    `trips_data_all.fhv_tripdata`;

-- Question 5

-- Non-partitioned
SELECT 
  DISTINCT Affiliated_base_number 
FROM
  `trips_data_all.fhv_tripdata`
WHERE
  pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

-- Partitioned and Clustered 
SELECT 
  DISTINCT Affiliated_base_number 
FROM
  `trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE
  pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

