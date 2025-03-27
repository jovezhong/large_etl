import java.time.YearMonth
import java.time.format.DateTimeFormatter

val startMs = System.currentTimeMillis;

val numFilesToProcess = 2 // Set to 72 to process all files

// Source and destination paths
val sourceBucket = "s3a://timeplus-nyc-tlc/"
val destinationBucket = "s3a://tp-internal2/jove/s3etl/spark/"

// Generate a sequence of month strings starting from Feb 2019
def generateMonthStrings(
    startYear: Int,
    startMonth: Int,
    count: Int
): Seq[String] = {
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM")
  (0 until count).map { i =>
    val yearMonth = YearMonth.of(startYear, startMonth).plusMonths(i)
    yearMonth.format(formatter)
  }
}

// Generate file names
val monthStrings = generateMonthStrings(2019, 2, numFilesToProcess)
val fileNames = monthStrings.map(month => s"fhvhv_tripdata_$month")
// Process each file
fileNames.foreach { fileName =>
  val file = sourceBucket + fileName + ".parquet"

  try {
    println(s"Reading file: $fileName")
    var startReadMs = System.currentTimeMillis;
    // Read file as DataFrame
    val df = spark.read.parquet(file)
    var startTransformMs = System.currentTimeMillis;
    println(s"Done reading file(${(startTransformMs - startReadMs) / 1000}s)")
    println("Starting transformations...")
    // Create a temp view to run SQL
    df.createOrReplaceTempView("parquetFile")

    // Apply transformations using SQL
    val transformedDF = spark.sql("""
      SELECT
        CASE
          WHEN hvfhs_license_num='HV0002' THEN 'Juno'
          WHEN hvfhs_license_num='HV0003' THEN 'Uber'
          WHEN hvfhs_license_num='HV0004' THEN 'Via'
          WHEN hvfhs_license_num='HV0005' THEN 'Lyft'
          ELSE 'Unknown'
        END AS hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        request_datetime,
        on_scene_datetime,
        pickup_datetime,
        dropoff_datetime,
        PULocationID,
        DOLocationID,
        trip_miles,
        trip_time,
        base_passenger_fare,
        tolls,
        bcf,
        sales_tax,
        congestion_surcharge,
        airport_fee,
        tips,
        driver_pay,
        shared_request_flag,
        shared_match_flag,
        access_a_ride_flag,
        wav_request_flag,
        wav_match_flag
      FROM parquetFile
    """)
    var startLoadMs = System.currentTimeMillis;
    println(
      s"Done transformations(${(startLoadMs - startTransformMs) / 1000}s)"
    )
    println("Writing data to S3...")

    // Write the transformed data back to S3
    transformedDF.write
      .mode("overwrite")
      .parquet(destinationBucket + fileName)

    println(
      s"Successfully saved file to: $destinationBucket$fileName (${(System.currentTimeMillis - startLoadMs) / 1000}s)\n"
    )
  } catch {
    case e: Exception =>
      println(s"Error processing file $fileName: ${e.getMessage}\n")
  }
}
val endMs = System.currentTimeMillis;
val totalSecond = (endMs - startMs) / 1000
println(s"ETL job completed in $totalSecond seconds")
System.exit(0)
