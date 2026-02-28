file:///C:/Users/tarfh/Nothor/Main.scala
empty definition using pc, found symbol in pc: 
semanticdb not found
empty definition using fallback
non-local guesses:
	 -org/apache.
	 -scala/Predef.org.apache.
offset: 17
uri: file:///C:/Users/tarfh/Nothor/Main.scala
text:
```scala
import org.apache@@.spark.sql.SparkSession
import preprocessing.{Cleaning, Reduction, Transformation}
import java.io.PrintWriter

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
    new java.io.File("./output").mkdirs()

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
    // SAVE AS SINGLE CSV (WITHOUT HADOOP)
    // ─────────────────────────────────────────────
    println("\n========== SAVING TRANSFORMED DATA ==========")
    
    // Collect data to driver (for small-medium datasets)
    val data = transformedDf.collect()
    val columns = transformedDf.columns
    
    // Write to CSV file
    val writer = new PrintWriter("output/transformed_data.csv")
    
    // Write header
    writer.println(columns.mkString(","))
    
    // Write data
    data.foreach { row =>
      val values = columns.map { col =>
        val value = row.getAs[Any](col)
        if (value == null) "" 
        else value.toString.replace(",", ";") // Handle commas in data
      }
      writer.println(values.mkString(","))
    }
    
    writer.close()
    
    println(s"✓ Saved ${data.length} rows to output/transformed_data.csv")
    println(s"✓ Columns: ${columns.mkString(", ")}")
    
    // Show sample
    println("\nSample of saved data:")
    transformedDf.show(10, truncate = false)

    spark.stop()
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 