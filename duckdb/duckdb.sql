INSTALL httpfs;

LOAD httpfs;

CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain,
    REGION 'us-west-2'
);

--  wav_match_flag in 2019Feb is Integer but VARCHAR in 2019Aug
DESCRIBE
SELECT
    *
FROM
    's3://timeplus-nyc-tlc/fhvhv_tripdata_2019-08.parquet';

-- Get row count for 2019-02: 20,159,102
SELECT
    COUNT(1)
FROM
    read_parquet (
        's3://timeplus-nyc-tlc/fhvhv_tripdata_2019-02.parquet'
    );

-- Get row count for all files: 1,257,319,004
SELECT
    COUNT(1)
FROM
    read_parquet ('s3://timeplus-nyc-tlc/fhvhv_tripdata_*.parquet');

-- Write Parquet via DuckDB, 1 file
COPY (
    SELECT
        CASE hvfhs_license_num
            WHEN 'HV0002' THEN 'Juno'
            WHEN 'HV0003' THEN 'Uber'
            WHEN 'HV0004' THEN 'Via'
            WHEN 'HV0005' THEN 'Lyft'
            ELSE 'Unknown'
        END AS hvfhs_license_num,
        * EXCLUDE (hvfhs_license_num)
    FROM
        read_parquet (
            's3://timeplus-nyc-tlc/fhvhv_tripdata_2019-02.parquet'
        )
) TO 's3://tp-internal2/jove/s3etl/duckdb/fhvhv_tripdata_2019-02.parquet';

-- DuckDB ETL, all files
COPY (
    SELECT
        CASE hvfhs_license_num
            WHEN 'HV0002' THEN 'Juno'
            WHEN 'HV0003' THEN 'Uber'
            WHEN 'HV0004' THEN 'Via'
            WHEN 'HV0005' THEN 'Lyft'
            ELSE 'Unknown'
        END AS hvfhs_license_num,
        * EXCLUDE (hvfhs_license_num)
    FROM
        read_parquet (
            's3://timeplus-nyc-tlc/fhvhv_tripdata_*.parquet',
            union_by_name = true
        )
) TO 's3://tp-internal2/jove/s3etl/duckdb' (FORMAT parquet);

-- Debug/tuning
SET
    memory_limit = '20GB';

SET
    s3_uploader_max_parts_per_file = 10;
