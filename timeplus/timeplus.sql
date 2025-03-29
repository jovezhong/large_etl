-- An external table in Timeplus to read all parquet files from a S3 bucket
CREATE EXTERNAL TABLE nyc_fhvhv (
    `hvfhs_license_num` nullable (string),
    `dispatching_base_num` nullable (string),
    `originating_base_num` nullable (string),
    `request_datetime` nullable (datetime64),
    `on_scene_datetime` nullable (datetime64),
    `pickup_datetime` nullable (datetime64),
    `dropoff_datetime` nullable (datetime64),
    `PULocationID` nullable (int32),
    `DOLocationID` nullable (int32),
    `trip_miles` nullable (float),
    `trip_time` nullable (int64),
    `base_passenger_fare` nullable (float),
    `tolls` nullable (float),
    `bcf` nullable (float),
    `sales_tax` nullable (float),
    `congestion_surcharge` nullable (float),
    `airport_fee` nullable (float),
    `tips` nullable (float),
    `driver_pay` nullable (float),
    `shared_request_flag` nullable (string),
    `shared_match_flag` nullable (string),
    `access_a_ride_flag` nullable (string),
    `wav_request_flag` nullable (string),
    `wav_match_flag` nullable (string)
) SETTINGS type = 's3',
region = 'us-west-2',
bucket = 'timeplus-nyc-tlc',
use_environment_credentials = true,
read_from = 'fhvhv_tripdata_2019-02.parquet';

-- An external table in Timeplus to write data to a S3 bucket
CREATE EXTERNAL TABLE target_nyc_fhvhv (
    `hvfhs_license_num` nullable (string),
    `dispatching_base_num` nullable (string),
    `originating_base_num` nullable (string),
    `request_datetime` nullable (datetime64),
    `on_scene_datetime` nullable (datetime64),
    `pickup_datetime` nullable (datetime64),
    `dropoff_datetime` nullable (datetime64),
    `PULocationID` nullable (int32),
    `DOLocationID` nullable (int32),
    `trip_miles` nullable (float),
    `trip_time` nullable (int64),
    `base_passenger_fare` nullable (float),
    `tolls` nullable (float),
    `bcf` nullable (float),
    `sales_tax` nullable (float),
    `congestion_surcharge` nullable (float),
    `airport_fee` nullable (float),
    `tips` nullable (float),
    `driver_pay` nullable (float),
    `shared_request_flag` nullable (string),
    `shared_match_flag` nullable (string),
    `access_a_ride_flag` nullable (string),
    `wav_request_flag` nullable (string),
    `wav_match_flag` nullable (string)
)
PARTITION BY
    coalesce(
        format_datetime (pickup_datetime, '%Y-%m'),
        '0000-00'
    ) SETTINGS type = 's3',
    region = 'us-west-2',
    bucket = 'tp-internal2',
    use_environment_credentials = true,
    write_to = 'jove/s3etl/timeplus/fhvhv_tripdata_{_partition_id}.parquet';

INSERT INTO
    target_nyc_fhvhv
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
    nyc_fhvhv;
