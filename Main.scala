import org.apache.spark.sql.SparkSession
import preprocessing.{Cleaning, Reduction, Transformation}
import java.io.{PrintWriter, File}
import scala.io.Codec

object Main {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", new java.io.File(".").getAbsolutePath)
    
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("UK Road Safety - Big Data Pipeline")
      .master("local[*]")
      .config("spark.local.dir", "./tmp")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
 
    spark.sparkContext.setLogLevel("WARN")

    // Create output directory
    new File("./output").mkdirs()
    new File("./tmp").mkdirs()

    // ─────────────────────────────────────────────
    // STEP 1 – READ RAW DATA
    // ─────────────────────────────────────────────
    println("\n========== STEP 1: READING RAW DATA ==========")
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/Accident_Information.csv")

    println(s"Rows loaded: ${df.count()}")

    // ─────────────────────────────────────────────
    // RUN ALL TRANSFORMATIONS
    // ─────────────────────────────────────────────
    println("\n========== RUNNING PIPELINE ==========")
    val cleanedDf = Cleaning.run(df)
    val sampledDf = Reduction.run(cleanedDf)
    val reducedDf = Reduction.runReduction(sampledDf)
    val transformedDf = Transformation.run(reducedDf)

    // ─────────────────────────────────────────────
    // SAVE DATA - USING PLAIN SCALA (NO HADOOP)
    // ─────────────────────────────────────────────
    println("\n========== SAVING TRANSFORMED DATA ==========")
    
    val columns = transformedDf.columns
    println(s"Total columns: ${columns.length}")
    println(s"Column names: ${columns.mkString(", ")}")
    
    // Get the data as RDD of strings (bypasses Dataset encoders)
    println("\nPreparing data for saving...")
    
    // Convert to RDD of strings
    val dataRDD = transformedDf.rdd.map { row =>
      columns.map { col =>
        val value = row.getAs[Any](col)
        if (value == null) "" 
        else value.toString.replace(",", ";").replace("\n", " ").replace("\r", " ")
      }.mkString(",")
    }
    
    // Collect header and data
    val header = columns.mkString(",")
    val data = dataRDD.collect()
    
    println(s"Writing ${data.length} rows to file...")
    
    // Write everything at once using PrintWriter
    val writer = new PrintWriter(new File("output/transformed_data.csv"), "UTF-8")
    writer.println(header)
    data.foreach(line => writer.println(line))
    writer.close()
    
    println(s" Data saved to: output/transformed_data.csv")
    println(s" Total rows: ${data.length}")
    println(s" Total columns: ${columns.length}")

    // Show sample
    println("\n========== SAMPLE OF TRANSFORMED DATA ==========")
    transformedDf.show(10, truncate = false)

    println("\n" + "="*80)
    println("PIPELINE COMPLETED SUCCESSFULLY! ")
    println("="*80)
    println("\nOutput file: output/transformed_data.csv")
    println("Your transformed dataset with all 41 columns is ready!")

    spark.stop()
  }
}