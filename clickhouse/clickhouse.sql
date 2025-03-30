-- ClickHouse ETL
INSERT INTO
    FUNCTION s3 (
        'https://tp-internal2.s3.us-west-2.amazonaws.com/jove/s3etl/clickhouse/fhvhv_tripdata_{_partition_id}.parquet'
    )
PARTITION BY
    formatDateTime (pickup_datetime, '%Y-%m')
SELECT
    CASE hvfhs_license_num
        WHEN 'HV0002' THEN 'Juno'
        WHEN 'HV0003' THEN 'Uber'
        WHEN 'HV0004' THEN 'Via'
        WHEN 'HV0005' THEN 'Lyft'
        ELSE 'Unknown'
    END AS hvfhs_license_num,
    *
EXCEPT
(hvfhs_license_num)
FROM
    s3 (
        's3://timeplus-nyc-tlc/fhvhv_tripdata_20{19..25}-{01..12}.parquet',
        'Parquet',
        'hvfhs_license_num Nullable(String), dispatching_base_num Nullable(String), originating_base_num Nullable(String), request_datetime Nullable(DateTime64), on_scene_datetime Nullable(DateTime64), pickup_datetime Nullable(DateTime64), dropoff_datetime Nullable(DateTime64), PULocationID Nullable(Int32), DOLocationID Nullable(Int32), trip_miles Nullable(Float), trip_time Nullable(Int64), base_passenger_fare Nullable(Float), tolls Nullable(Float), bcf Nullable(Float), sales_tax Nullable(Float), congestion_surcharge Nullable(Float), airport_fee Nullable(Float), tips Nullable(Float), driver_pay Nullable(Float), shared_request_flag Nullable(String), shared_match_flag Nullable(String), access_a_ride_flag Nullable(String), wav_request_flag Nullable(String), wav_match_flag Nullable(String)'
    );
