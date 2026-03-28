import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AIDataPipeline {

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder
      .appName("AIDataPipeline")
      .master("local[*]") // Use "local[*]" for local testing, or a Spark cluster URL
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // For older date formats
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") // Reduce verbosity

    println("
--- Starting AI Data Pipeline ---
")

    // 1. Data Ingestion (Simulated from a CSV file)
    val rawDataPath = "./data/raw_sensor_data.csv"
    // Create dummy raw data file for demonstration
    createDummyRawData(rawDataPath)

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rawDataPath)

    println("Raw Data Schema:")
    rawDf.printSchema()
    println("Raw Data Sample:")
    rawDf.show(5)

    // 2. Data Cleaning and Transformation
    val cleanedDf = rawDf
      .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
      .filter(col("sensor_value").isNotNull && col("sensor_value") > 0) // Remove nulls and non-positive values
      .withColumn("sensor_value_scaled", col("sensor_value") / 100.0) // Scale sensor value
      .withColumn("feature_a", col("sensor_value_scaled") * 2)
      .withColumn("feature_b", log(col("sensor_value_scaled") + 1))

    println("Cleaned and Transformed Data Schema:")
    cleanedDf.printSchema()
    println("Cleaned and Transformed Data Sample:")
    cleanedDf.show(5)

    // 3. Feature Engineering (Example: rolling average)
    import org.apache.spark.sql.expressions.Window
    val windowSpec = Window.partitionBy("device_id").orderBy("timestamp")
    val featuredDf = cleanedDf
      .withColumn("rolling_avg_sensor_value", avg(col("sensor_value_scaled")).over(windowSpec.rowsBetween(-2, 0))) // 3-point rolling average
      .na.fill(0.0, Seq("rolling_avg_sensor_value")) // Fill initial nulls from rolling average

    println("Featured Data Schema:")
    featuredDf.printSchema()
    println("Featured Data Sample:")
    featuredDf.show(5)

    // 4. Data Validation (Example: check for outliers)
    val outlierThreshold = 3.0 // Z-score threshold
    val validatedDf = featuredDf.withColumn("is_outlier", 
      when(abs(col("sensor_value_scaled") - mean("sensor_value_scaled").over(windowSpec)) / stddev("sensor_value_scaled").over(windowSpec) > outlierThreshold, true)
      .otherwise(false)
    )
    println("Validated Data Sample (with outliers flagged):")
    validatedDf.filter(col("is_outlier")).show(5)

    // 5. Data Export (Simulated to Parquet)
    val processedDataPath = "./data/processed_sensor_data.parquet"
    validatedDf.write
      .mode("overwrite")
      .parquet(processedDataPath)

    println(s"Processed data saved to: $processedDataPath")
    println("
--- AI Data Pipeline Completed Successfully ---
")

    spark.stop()
  }

  // Helper function to create dummy raw data
  def createDummyRawData(path: String): Unit = {
    import java.io._
    val pw = new PrintWriter(new File(path))
    pw.write("timestamp,device_id,sensor_value,location
")
    val start = Instant.parse("2023-01-01T00:00:00Z")
    for (i <- 0 until 100) {
      val ts = start.plusSeconds(i * 3600).toString.replace("T", " ").dropRight(1) // YYYY-MM-DD HH:MM:SS
      val deviceId = s"device_${i % 5}"
      val sensorValue = 100 + (math.sin(i / 10.0) * 50 + random.nextInt(20)).toInt
      val location = s"zone_${i % 3}"
      pw.write(s"$ts,$deviceId,$sensorValue,$location
")
    }
    pw.close()
    println(s"Dummy raw data created at: $path")
  }
}
