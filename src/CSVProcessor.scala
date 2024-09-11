import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
*/

object CSVProcessor extends App {
  //def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession
      .builder()
      .appName("CSV Processor")
      .master("local[*]") // Use all available cores
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

  println("Spark session has been created")

    // Read the CSV file into a DataFrame
    val filePath = "D:\\D\\My Projects\\Scala\\ImportExport\\RG3SID3.csv"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)


  // Compare columns AA, BB, and CC row by row and create column DD
  val resultDf = df.withColumn("DD", when(col("RULENAME") === col("POLICYNAME") && col("POLICYNAME") === col("FLOWFILTERNAME"), "True").otherwise("False"))

  //val resultDf = df.withColumn("DD", when($"AA" === $"BB" && $"BB" === $"CC", lit(true)).otherwise(lit(false)))

  // Show the DataFrame (for debugging purposes)
  resultDf.show()

  // Save the result DataFrame to a new CSV file
  val outputFilePath = "D:\\D\\My Projects\\Scala\\ImportExport\\RG3SID3_result.csv"
  //resultDf.write.option("header", "true").csv(outputFilePath)


  // Stop the Spark session
    spark.stop()
/*
  def compareColumns(col1: Column, col2: Column, col3: Column): Column = {
    when(col1 === col2 && col2 === col3, lit(true)).otherwise(lit(false))
 */
}

