import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: group_parquet_rows.py <gcs_input_path>", file=sys.stderr)
        sys.exit(-1)

    gcs_input_path = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("ParquetGroupCount") \
        .getOrCreate()

    # Read the Parquet files from the specified GCS path
    df = spark.read.parquet(gcs_input_path)

    # Register the DataFrame as a temporary view
    df.createOrReplaceTempView("parquetFile")

    # Execute the SQL query
    sql_query = """
    SELECT
        hvfhs_license_num,
        COUNT(1) AS rows
    FROM
        parquetFile
    GROUP BY
        hvfhs_license_num
    ORDER BY rows DESC
    """
    result_df = spark.sql(sql_query)

    # Show the results
    result_df.show()

    spark.stop()
