import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_parquet_rows.py <gcs_input_path>", file=sys.stderr)
        sys.exit(-1)

    gcs_input_path = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("ParquetRowCount") \
        .getOrCreate()

    # Read the Parquet files from the specified GCS path
    df = spark.read.parquet(gcs_input_path)

    # Count the number of rows
    row_count = df.count()

    print(f"Total rows in Parquet files at {gcs_input_path}: {row_count}")

    spark.stop()
