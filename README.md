# Large Scale ETL(Extract, Transform, Load) Test

I am about to load large amount of data to S3 using [Timeplus](https://timeplus.com/), as well as other tools, such as Apache Spark, to compare the performance and cost.

## Data Preparation
I started from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page. This is used to be available in AWS S3:  https://registry.opendata.aws/nyc-tlc-trip-records-pds/ But this is no longer working (AccessDenied for `aws s3 ls s3://nyc-tlc`).

So I have to write a script(with AI) to download the FHVHV data and upload it to our own S3 bucket.

Check [cmd.sh](cmd.sh) for details. Just set up an EC2 instance with proper IAM role and you can run this script.
```bash
bash cmd.sh
Processing fhvhv_tripdata files from 2019-02 to 2025-01...
-------------------------------------------------------
Downloading fhvhv_tripdata_2019-02.parquet (489.28 MB)...
Successfully downloaded fhvhv_tripdata_2019-02.parquet
Uploading fhvhv_tripdata_2019-02.parquet to S3...
upload: ./fhvhv_tripdata_2019-02.parquet to s3://timeplus-nyc-tlc/fhvhv_tripdata_2019-02.parquet
Successfully uploaded fhvhv_tripdata_2019-02.parquet to S3
-------------------------------------------------------
Downloading fhvhv_tripdata_2019-03.parquet (582.55 MB)...
Successfully downloaded fhvhv_tripdata_2019-03.parquet
Uploading fhvhv_tripdata_2019-03.parquet to S3...
Completed 582.6 MiB/582.6 MiB (103.4 MiB/s) with 1 file(s) remaining
```

Since Feb 2019 til now, the total size is 29.62 GB.
