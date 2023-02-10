-- Create external and bq tables

CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375814.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dtc-de-375814/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE dtc-de-375814.trips_data_all.fhv_tripdata_non_partitoned AS
SELECT * FROM dtc-de-375814.trips_data_all.external_fhv_tripdata;

-- Q1

SELECT COUNT(*)
FROM dtc-de-375814.trips_data_all.fhv_tripdata_non_partitoned;

-- Q2

SELECT COUNT(DISTINCT Affiliated_base_number)
FROM dtc-de-375814.trips_data_all.external_fhv_tripdata;

SELECT COUNT(DISTINCT Affiliated_base_number)
FROM dtc-de-375814.trips_data_all.fhv_tripdata_non_partitoned;

-- Q3

SELECT COUNT(*)
FROM dtc-de-375814.trips_data_all.fhv_tripdata_non_partitoned
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

-- Q5

CREATE OR REPLACE TABLE dtc-de-375814.trips_data_all.fhv_tripdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM dtc-de-375814.trips_data_all.external_fhv_tripdata;


SELECT DISTINCT Affiliated_base_number
FROM dtc-de-375814.trips_data_all.fhv_tripdata_partitoned_clustered
WHERE pickup_datetime >= '2019-03-01 00:00:00' AND pickup_datetime <= '2019-03-31 23:59:59'

SELECT DISTINCT Affiliated_base_number
FROM dtc-de-375814.trips_data_all.fhv_tripdata_non_partitoned
WHERE pickup_datetime >= '2019-03-01 00:00:00' AND pickup_datetime <= '2019-03-31 23:59:59'



